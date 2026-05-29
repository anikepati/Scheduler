"""
Storage layer — asyncpg connection pool and all DB operations.
All SQL lives here; no raw queries elsewhere in the codebase.
"""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Optional

import asyncpg

from logger import get_logger
from models import (
    ApiConfig,
    AuthType,
    ExecutionStatus,
    JobExecutionLog,
    JobQueueItem,
    JobSchedule,
    JobStatus,
)

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# DDL — schema creation
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS api_configs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT NOT NULL UNIQUE,
    url              TEXT NOT NULL,
    method           TEXT NOT NULL DEFAULT 'GET',
    headers          JSONB NOT NULL DEFAULT '{}',
    auth_type        TEXT NOT NULL DEFAULT 'none',
    auth_secret_ref  TEXT,
    payload_template JSONB,
    timeout_seconds  INT NOT NULL DEFAULT 30,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_schedules (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    api_config_id     UUID NOT NULL REFERENCES api_configs(id) ON DELETE CASCADE,
    name              TEXT NOT NULL UNIQUE,
    cron_expr         TEXT NOT NULL,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    jitter_seconds    INT NOT NULL DEFAULT 0,
    retry_attempts    INT NOT NULL DEFAULT 3,
    retry_backoff_sec INT NOT NULL DEFAULT 5,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_queue (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_schedule_id  UUID NOT NULL REFERENCES job_schedules(id) ON DELETE CASCADE,
    scheduled_at     TIMESTAMPTZ NOT NULL,
    claimed_by       TEXT,
    claimed_at       TIMESTAMPTZ,
    status           TEXT NOT NULL DEFAULT 'pending',
    attempts         INT NOT NULL DEFAULT 0,
    next_retry_at    TIMESTAMPTZ,
    UNIQUE (job_schedule_id, scheduled_at)
);

CREATE INDEX IF NOT EXISTS idx_job_queue_status_scheduled
    ON job_queue (status, scheduled_at)
    WHERE status IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS job_execution_log (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_queue_id     UUID NOT NULL REFERENCES job_queue(id),
    job_schedule_id  UUID NOT NULL REFERENCES job_schedules(id),
    api_config_id    UUID NOT NULL REFERENCES api_configs(id),
    pod_name         TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           TEXT NOT NULL,
    http_status_code INT,
    response_body    JSONB,
    error_message    TEXT,
    duration_ms      INT
);

CREATE INDEX IF NOT EXISTS idx_execution_log_schedule_started
    ON job_execution_log (job_schedule_id, started_at DESC);
"""

# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------

_pool: Optional[asyncpg.Pool] = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    dsn = os.environ["POSTGRES_DSN"]
    min_size = int(os.getenv("DB_POOL_MIN", "2"))
    max_size = int(os.getenv("DB_POOL_MAX", "10"))

    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=60,
        # Register custom codec so JSONB ↔ dict works transparently
        init=_init_connection,
    )
    log.info("db_pool_created", min=min_size, max=max_size)
    return _pool


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        log.info("db_pool_closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized — call init_pool() first")
    return _pool


@asynccontextmanager
async def acquire() -> AsyncGenerator[asyncpg.Connection, None]:
    async with get_pool().acquire() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

async def create_schema() -> None:
    async with acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    log.info("schema_ready")


# ---------------------------------------------------------------------------
# api_configs
# ---------------------------------------------------------------------------

async def upsert_api_config(cfg: ApiConfig) -> ApiConfig:
    async with acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO api_configs
                (id, name, url, method, headers, auth_type,
                 auth_secret_ref, payload_template, timeout_seconds)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (name) DO UPDATE SET
                url              = EXCLUDED.url,
                method           = EXCLUDED.method,
                headers          = EXCLUDED.headers,
                auth_type        = EXCLUDED.auth_type,
                auth_secret_ref  = EXCLUDED.auth_secret_ref,
                payload_template = EXCLUDED.payload_template,
                timeout_seconds  = EXCLUDED.timeout_seconds,
                updated_at       = NOW()
            RETURNING *
            """,
            cfg.id,
            cfg.name,
            cfg.url,
            cfg.method,
            cfg.headers,
            cfg.auth_type.value,
            cfg.auth_secret_ref,
            cfg.payload_template,
            cfg.timeout_seconds,
        )
    return _row_to_api_config(row)


async def get_api_config(api_config_id: uuid.UUID) -> Optional[ApiConfig]:
    async with acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM api_configs WHERE id = $1", api_config_id
        )
    return _row_to_api_config(row) if row else None


def _row_to_api_config(row: asyncpg.Record) -> ApiConfig:
    return ApiConfig(
        id=row["id"],
        name=row["name"],
        url=row["url"],
        method=row["method"],
        headers=row["headers"] or {},
        auth_type=AuthType(row["auth_type"]),
        auth_secret_ref=row["auth_secret_ref"],
        payload_template=row["payload_template"],
        timeout_seconds=row["timeout_seconds"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ---------------------------------------------------------------------------
# job_schedules
# ---------------------------------------------------------------------------

async def upsert_job_schedule(sched: JobSchedule) -> JobSchedule:
    async with acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO job_schedules
                (id, api_config_id, name, cron_expr, enabled,
                 jitter_seconds, retry_attempts, retry_backoff_sec)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (name) DO UPDATE SET
                api_config_id     = EXCLUDED.api_config_id,
                cron_expr         = EXCLUDED.cron_expr,
                enabled           = EXCLUDED.enabled,
                jitter_seconds    = EXCLUDED.jitter_seconds,
                retry_attempts    = EXCLUDED.retry_attempts,
                retry_backoff_sec = EXCLUDED.retry_backoff_sec,
                updated_at        = NOW()
            RETURNING *
            """,
            sched.id,
            sched.api_config_id,
            sched.name,
            sched.cron_expr,
            sched.enabled,
            sched.jitter_seconds,
            sched.retry_attempts,
            sched.retry_backoff_sec,
        )
    return _row_to_job_schedule(row)


async def list_enabled_schedules() -> list[JobSchedule]:
    async with acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM job_schedules WHERE enabled = TRUE"
        )
    return [_row_to_job_schedule(r) for r in rows]


def _row_to_job_schedule(row: asyncpg.Record) -> JobSchedule:
    return JobSchedule(
        id=row["id"],
        api_config_id=row["api_config_id"],
        name=row["name"],
        cron_expr=row["cron_expr"],
        enabled=row["enabled"],
        jitter_seconds=row["jitter_seconds"],
        retry_attempts=row["retry_attempts"],
        retry_backoff_sec=row["retry_backoff_sec"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ---------------------------------------------------------------------------
# job_queue — leader operations
# ---------------------------------------------------------------------------

async def enqueue_job(
    job_schedule_id: uuid.UUID,
    scheduled_at: datetime,
) -> Optional[JobQueueItem]:
    """
    Insert a pending job into the queue.
    ON CONFLICT DO NOTHING prevents duplicate enqueues for the same
    (schedule, scheduled_at) slot — safe to call repeatedly.
    """
    async with acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO job_queue (job_schedule_id, scheduled_at, status)
            VALUES ($1, $2, 'pending')
            ON CONFLICT (job_schedule_id, scheduled_at) DO NOTHING
            RETURNING *
            """,
            job_schedule_id,
            scheduled_at,
        )
    return _row_to_queue_item(row) if row else None


async def reset_stale_running_jobs(stale_after_seconds: int = 300) -> int:
    """
    Jobs stuck in 'running' longer than stale_after_seconds are reset to
    'pending' so another worker can pick them up (handles pod crash mid-run).
    """
    async with acquire() as conn:
        result = await conn.execute(
            """
            UPDATE job_queue
            SET status     = 'pending',
                claimed_by = NULL,
                claimed_at = NULL
            WHERE status = 'running'
              AND claimed_at < NOW() - ($1 || ' seconds')::INTERVAL
            """,
            str(stale_after_seconds),
        )
    count = int(result.split()[-1])
    if count:
        log.warning("stale_jobs_reset", count=count)
    return count


# ---------------------------------------------------------------------------
# job_queue — worker operations
# ---------------------------------------------------------------------------

async def claim_next_job(pod_name: str, batch_size: int = 5) -> list[JobQueueItem]:
    """
    Atomically claim up to batch_size pending jobs using
    SELECT FOR UPDATE SKIP LOCKED — the Postgres pattern for
    exactly-once distributed work dispatch.
    """
    async with acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT * FROM job_queue
                WHERE status = 'pending'
                  AND scheduled_at <= NOW()
                ORDER BY scheduled_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                batch_size,
            )
            if not rows:
                return []

            ids = [r["id"] for r in rows]
            now = datetime.now(timezone.utc)
            await conn.execute(
                """
                UPDATE job_queue
                SET status     = 'running',
                    claimed_by = $1,
                    claimed_at = $2
                WHERE id = ANY($3::uuid[])
                """,
                pod_name,
                now,
                ids,
            )
    return [_row_to_queue_item(r) for r in rows]


async def complete_job(job_id: uuid.UUID, status: JobStatus) -> None:
    async with acquire() as conn:
        await conn.execute(
            "UPDATE job_queue SET status = $1 WHERE id = $2",
            status.value,
            job_id,
        )


async def schedule_retry(
    job_id: uuid.UUID,
    retry_at: datetime,
    attempts: int,
) -> None:
    async with acquire() as conn:
        await conn.execute(
            """
            UPDATE job_queue
            SET status        = 'pending',
                claimed_by    = NULL,
                claimed_at    = NULL,
                next_retry_at = $1,
                attempts      = $2,
                scheduled_at  = $1
            WHERE id = $3
            """,
            retry_at,
            attempts,
            job_id,
        )


def _row_to_queue_item(row: asyncpg.Record) -> JobQueueItem:
    return JobQueueItem(
        id=row["id"],
        job_schedule_id=row["job_schedule_id"],
        scheduled_at=row["scheduled_at"],
        claimed_by=row["claimed_by"],
        claimed_at=row["claimed_at"],
        status=JobStatus(row["status"]),
        attempts=row["attempts"],
        next_retry_at=row["next_retry_at"],
    )


# ---------------------------------------------------------------------------
# job_execution_log
# ---------------------------------------------------------------------------

async def insert_execution_log(log_entry: JobExecutionLog) -> None:
    async with acquire() as conn:
        await conn.execute(
            """
            INSERT INTO job_execution_log
                (id, job_queue_id, job_schedule_id, api_config_id,
                 pod_name, started_at, finished_at, status,
                 http_status_code, response_body, error_message, duration_ms)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            """,
            log_entry.id,
            log_entry.job_queue_id,
            log_entry.job_schedule_id,
            log_entry.api_config_id,
            log_entry.pod_name,
            log_entry.started_at,
            log_entry.finished_at,
            log_entry.status.value,
            log_entry.http_status_code,
            log_entry.response_body,
            log_entry.error_message,
            log_entry.duration_ms,
        )


# ---------------------------------------------------------------------------
# Leader election — Postgres advisory locks
# ---------------------------------------------------------------------------

async def try_acquire_leader_lock(lock_key: int = 123456789) -> bool:
    """
    Non-blocking advisory lock attempt.
    Returns True if this pod becomes leader.
    Lock is session-scoped — released automatically on connection close.
    """
    async with acquire() as conn:
        result = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", lock_key
        )
    return bool(result)


async def release_leader_lock(lock_key: int = 123456789) -> None:
    async with acquire() as conn:
        await conn.execute("SELECT pg_advisory_unlock($1)", lock_key)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

async def ping() -> bool:
    try:
        async with acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception:
        return False
