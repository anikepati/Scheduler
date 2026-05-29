"""
JobRunner — executes a single API call for a claimed job.
Handles auth header injection, retry with backoff, timeout,
response parsing, and execution log writing.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
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
from logger import get_logger
from models import ApiConfig, ExecutionStatus, JobExecutionLog, JobQueueItem, JobSchedule, JobStatus
from secrets import build_auth_headers

log = get_logger(__name__)

# Shared async client — one per process, reused across all jobs.
# Configured with reasonable enterprise defaults.
_client: Optional[httpx.AsyncClient] = None


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
    """
    Execute one job end-to-end:
      1. Build request (headers, auth, payload)
      2. Call API with retry
      3. Write execution log
      4. Update job_queue status
    """
    started_at = datetime.now(timezone.utc)
    job_log_id = uuid.uuid4()

    log.info(
        "job_started",
        job_id=str(queue_item.id),
        schedule=schedule.name,
        url=api_config.url,
        attempt=queue_item.attempts + 1,
    )

    http_status: Optional[int] = None
    response_body: Optional[Any] = None
    error_message: Optional[str] = None
    exec_status = ExecutionStatus.FAILED

    try:
        http_status, response_body = await _call_with_retry(
            api_config=api_config,
            max_attempts=schedule.retry_attempts,
            backoff_sec=schedule.retry_backoff_sec,
        )
        exec_status = ExecutionStatus.SUCCESS

        log.info(
            "job_succeeded",
            job_id=str(queue_item.id),
            schedule=schedule.name,
            http_status=http_status,
        )

    except httpx.TimeoutException as exc:
        exec_status = ExecutionStatus.TIMEOUT
        error_message = f"Timeout: {exc}"
        log.warning("job_timeout", job_id=str(queue_item.id), error=error_message)

    except RetryError as exc:
        exec_status = ExecutionStatus.FAILED
        error_message = f"All retries exhausted: {exc.last_attempt.exception()}"
        log.error("job_retries_exhausted", job_id=str(queue_item.id), error=error_message)

    except Exception as exc:
        exec_status = ExecutionStatus.FAILED
        error_message = str(exc)
        log.exception("job_unexpected_error", job_id=str(queue_item.id), error=error_message)

    finally:
        finished_at = datetime.now(timezone.utc)
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)

        # Write immutable audit log regardless of outcome
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
                response_body=response_body,
                error_message=error_message,
                duration_ms=duration_ms,
            )
        )

        # Update queue item status
        if exec_status == ExecutionStatus.SUCCESS:
            await storage.complete_job(queue_item.id, JobStatus.DONE)
        else:
            await storage.complete_job(queue_item.id, JobStatus.FAILED)


# ---------------------------------------------------------------------------
# HTTP call with tenacity retry
# ---------------------------------------------------------------------------

async def _call_with_retry(
    api_config: ApiConfig,
    max_attempts: int,
    backoff_sec: int,
) -> tuple[int, Any]:
    """
    Returns (http_status_code, parsed_response_body).
    Retries on transient network errors and 5xx responses.
    """

    # Build auth headers at call time (not startup) so secret rotation works
    auth_headers = build_auth_headers(
        auth_type=api_config.auth_type.value,
        secret_ref=api_config.auth_secret_ref,
    )

    merged_headers = {**api_config.headers, **auth_headers}

    @retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=backoff_sec, min=backoff_sec, max=60),
        retry=retry_if_exception_type((httpx.TransportError, _RetryableHTTPError)),
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

        if response.status_code >= 500:
            raise _RetryableHTTPError(
                f"HTTP {response.status_code} from {api_config.url}"
            )

        # Parse response — prefer JSON, fall back to text
        try:
            body = response.json()
        except Exception:
            body = response.text

        return response.status_code, body

    return await _attempt()


class _RetryableHTTPError(Exception):
    """Signals a 5xx response that should be retried."""
