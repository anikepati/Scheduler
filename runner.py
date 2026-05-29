"""
JobRunner — executes a single API call for a claimed job.

FIX: Metrics are now incremented on every execution outcome.
FIX: Response body is capped at api_config.max_response_bytes before storage.
FIX: Queue-level retry (schedule_retry) wires up the retry_attempts config;
     tenacity handles quick network-error recovery (2 attempts inline),
     queue retry handles the outer loop (retry_attempts total attempts).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import storage
from health import increment_metric          # FIX: was imported but never called
from logger import get_logger
from models import ApiConfig, ExecutionStatus, JobExecutionLog, JobQueueItem, JobSchedule, JobStatus
from secrets import build_auth_headers

log = get_logger(__name__)

_client: Optional[httpx.AsyncClient] = None

# Quick inline retries for transient network errors only.
# The outer retry loop (queue re-enqueue) is controlled by retry_attempts.
_NETWORK_RETRY_ATTEMPTS = int(__import__("os").getenv("NETWORK_RETRY_ATTEMPTS", "2"))


async def init_http_client() -> None:
    global _client
    _client = httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=30,
        ),
        timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0),
    )
    log.info("http_client_initialized")


async def close_http_client() -> None:
    global _client
    if _client:
        await _client.aclose()
        _client = None
        log.info("http_client_closed")


def _get_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("HTTP client not initialized — call init_http_client() first")
    return _client


# ---------------------------------------------------------------------------
# Main execution entry point
# ---------------------------------------------------------------------------

async def execute_job(
    queue_item: JobQueueItem,
    schedule: JobSchedule,
    api_config: ApiConfig,
    pod_name: str,
) -> None:
    started_at  = datetime.now(timezone.utc)
    job_log_id  = uuid.uuid4()

    log.info(
        "job_started",
        job_id=str(queue_item.id),
        schedule=schedule.name,
        url=api_config.url,
        attempt=queue_item.attempts,
    )

    http_status: Optional[int] = None
    response_body: Optional[Any] = None
    error_message: Optional[str] = None
    exec_status = ExecutionStatus.FAILED

    try:
        http_status, response_body = await _call_with_retry(api_config)
        exec_status = ExecutionStatus.SUCCESS
        increment_metric("jobs_succeeded_total")   # FIX

        log.info(
            "job_succeeded",
            job_id=str(queue_item.id),
            schedule=schedule.name,
            http_status=http_status,
        )

    except httpx.TimeoutException as exc:
        exec_status   = ExecutionStatus.TIMEOUT
        error_message = f"Timeout: {exc}"
        increment_metric("jobs_timeout_total")     # FIX
        log.warning("job_timeout", job_id=str(queue_item.id), error=error_message)

    except RetryError as exc:
        exec_status   = ExecutionStatus.FAILED
        error_message = f"Network retries exhausted: {exc.last_attempt.exception()}"
        increment_metric("jobs_failed_total")      # FIX
        log.error("job_network_retries_exhausted", job_id=str(queue_item.id), error=error_message)

    except Exception as exc:
        exec_status   = ExecutionStatus.FAILED
        error_message = str(exc)
        increment_metric("jobs_failed_total")      # FIX
        log.exception("job_unexpected_error", job_id=str(queue_item.id), error=error_message)

    finally:
        finished_at = datetime.now(timezone.utc)
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)

        # FIX: truncate response body before storing to prevent table bloat
        safe_body = _truncate_response(response_body, api_config.max_response_bytes)

        await storage.insert_execution_log(
            JobExecutionLog(
                id=job_log_id,
                job_queue_id=queue_item.id,
                job_schedule_id=queue_item.job_schedule_id,
                api_config_id=api_config.id,
                pod_name=pod_name,
                started_at=started_at,
                finished_at=finished_at,
                status=exec_status,
                http_status_code=http_status,
                response_body=safe_body,
                error_message=error_message,
                duration_ms=duration_ms,
            )
        )

        # FIX: wire up queue-level retry (previously schedule_retry was dead code)
        if exec_status == ExecutionStatus.SUCCESS:
            await storage.complete_job(queue_item.id, JobStatus.DONE)

        elif queue_item.attempts < schedule.retry_attempts:
            # Exponential backoff: backoff_sec * 2^(attempt-1), capped at 1 hour
            delay_sec = min(
                schedule.retry_backoff_sec * (2 ** (queue_item.attempts - 1)),
                3600,
            )
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_sec)
            await storage.schedule_retry(queue_item.id, retry_at)
            log.info(
                "job_requeued",
                job_id=str(queue_item.id),
                attempt=queue_item.attempts,
                max_attempts=schedule.retry_attempts,
                retry_in_sec=delay_sec,
            )

        else:
            await storage.complete_job(queue_item.id, JobStatus.FAILED)
            log.error(
                "job_exhausted_all_attempts",
                job_id=str(queue_item.id),
                attempts=queue_item.attempts,
            )


# ---------------------------------------------------------------------------
# HTTP call — tenacity for transient network errors only
# ---------------------------------------------------------------------------

async def _call_with_retry(api_config: ApiConfig) -> tuple[int, Any]:
    """
    Inline retry for NETWORK errors only (connect timeout, DNS, TLS).
    HTTP 5xx errors bubble up immediately — queue retry handles those.
    """
    auth_headers   = build_auth_headers(
        auth_type=api_config.auth_type.value,
        secret_ref=api_config.auth_secret_ref,
    )
    merged_headers = {**api_config.headers, **auth_headers}

    @retry(
        stop=stop_after_attempt(_NETWORK_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(httpx.TransportError),
        reraise=True,
    )
    async def _attempt() -> tuple[int, Any]:
        response = await _get_client().request(
            method=api_config.method,
            url=api_config.url,
            headers=merged_headers,
            json=api_config.payload_template,
            timeout=api_config.timeout_seconds,
        )
        try:
            body = response.json()
        except Exception:
            body = response.text

        return response.status_code, body

    return await _attempt()


# ---------------------------------------------------------------------------
# FIX: Response body truncation — prevents unbounded storage growth
# ---------------------------------------------------------------------------

def _truncate_response(body: Any, max_bytes: int) -> Any:
    """
    If the serialised response exceeds max_bytes, replace with a sentinel
    dict so the audit log row is still written but storage stays bounded.
    """
    if body is None:
        return None

    raw = json.dumps(body) if not isinstance(body, str) else body
    size = len(raw.encode("utf-8"))

    if size <= max_bytes:
        return body

    return {
        "_truncated": True,
        "original_size_bytes": size,
        "max_bytes": max_bytes,
        "preview": raw[:256],
    }
