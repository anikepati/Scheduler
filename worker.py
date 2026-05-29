"""
Worker loop — runs on ALL pods.

Continuously polls job_queue for pending jobs, claims them using
SELECT FOR UPDATE SKIP LOCKED, and executes them with bounded
concurrency via asyncio.Semaphore.
"""

from __future__ import annotations

import asyncio
import os
from time import monotonic
from typing import Optional

import storage
from logger import get_logger
from models import JobSchedule
from runner import execute_job

log = get_logger(__name__)

WORKER_POLL_INTERVAL = float(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "5"))
WORKER_CONCURRENCY   = int(os.getenv("WORKER_CONCURRENCY",  "20"))
WORKER_BATCH_SIZE    = int(os.getenv("WORKER_BATCH_SIZE",   "10"))

# FIX: TTL-based schedule cache — prevents serving stale configs indefinitely
_CACHE_TTL_SECONDS = int(os.getenv("SCHEDULE_CACHE_TTL_SECONDS", "300"))


class Worker:
    def __init__(self, pod_name: str) -> None:
        self.pod_name = pod_name
        self._semaphore   = asyncio.Semaphore(WORKER_CONCURRENCY)
        self._stop_event  = asyncio.Event()
        self._worker_task: asyncio.Task | None = None
        self._active_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self._worker_task = asyncio.create_task(self._poll_loop())
        log.info(
            "worker_started",
            pod=self.pod_name,
            concurrency=WORKER_CONCURRENCY,
            batch_size=WORKER_BATCH_SIZE,
        )

    async def stop(self) -> None:
        """
        Graceful shutdown:
          1. Stop accepting new jobs
          2. Wait for all in-flight executions to complete
        """
        self._stop_event.set()

        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        if self._active_tasks:
            log.info(
                "worker_draining",
                pod=self.pod_name,
                in_flight=len(self._active_tasks),
            )
            await asyncio.gather(*self._active_tasks, return_exceptions=True)

        log.info("worker_stopped", pod=self.pod_name)

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                items = await storage.claim_next_job(
                    pod_name=self.pod_name,
                    batch_size=WORKER_BATCH_SIZE,
                )

                if not items:
                    await asyncio.sleep(WORKER_POLL_INTERVAL)
                    continue

                for item in items:
                    task = asyncio.create_task(self._run_job(item))
                    self._active_tasks.add(task)
                    task.add_done_callback(self._active_tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("worker_poll_error", error=str(exc))
                await asyncio.sleep(WORKER_POLL_INTERVAL)

    async def _run_job(self, queue_item) -> None:
        """
        Bounded execution: semaphore ensures at most WORKER_CONCURRENCY
        simultaneous API calls per pod.
        """
        async with self._semaphore:
            try:
                schedule = await _get_schedule(queue_item.job_schedule_id)
                if not schedule:
                    log.error(
                        "schedule_not_found",
                        job_id=str(queue_item.id),
                        schedule_id=str(queue_item.job_schedule_id),
                    )
                    await storage.complete_job(queue_item.id, storage.JobStatus.FAILED)
                    return

                api_config = await storage.get_api_config(schedule.api_config_id)
                if not api_config:
                    log.error(
                        "api_config_not_found",
                        job_id=str(queue_item.id),
                        api_config_id=str(schedule.api_config_id),
                    )
                    await storage.complete_job(queue_item.id, storage.JobStatus.FAILED)
                    return

                await execute_job(
                    queue_item=queue_item,
                    schedule=schedule,
                    api_config=api_config,
                    pod_name=self.pod_name,
                )

            except Exception as exc:
                log.exception(
                    "run_job_unhandled_error",
                    job_id=str(queue_item.id),
                    error=str(exc),
                )


# ---------------------------------------------------------------------------
# FIX: TTL-based in-process schedule cache
# Replaces infinite dict that never invalidated stale configs.
# _CACHE_TTL_SECONDS controls how quickly config changes propagate.
# ---------------------------------------------------------------------------

# id → (JobSchedule, cached_at_monotonic)
_schedule_cache: dict[object, tuple[JobSchedule, float]] = {}


async def _get_schedule(schedule_id) -> Optional[JobSchedule]:
    entry = _schedule_cache.get(schedule_id)
    if entry is not None:
        sched, cached_at = entry
        if (monotonic() - cached_at) < _CACHE_TTL_SECONDS:
            return sched
        # TTL expired — remove stale entry and re-fetch
        del _schedule_cache[schedule_id]

    async with storage.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM job_schedules WHERE id = $1", schedule_id
        )

    if row:
        fresh = storage._row_to_job_schedule(row)
        _schedule_cache[schedule_id] = (fresh, monotonic())
        return fresh

    return None
