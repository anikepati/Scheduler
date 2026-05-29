"""
Leader election and scheduling loop.

Only ONE pod across all OCP clusters runs this at a time,
controlled by a Postgres advisory lock.

Responsibilities:
  - Re-reads all enabled job_schedules from Postgres every tick
  - Computes next fire time using croniter
  - Enqueues pending jobs into job_queue (idempotent)
  - Resets stale 'running' jobs (handles pod crashes)
  - Releases lock on shutdown
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timedelta, timezone

from croniter import croniter

import storage
from logger import get_logger
from models import JobSchedule

log = get_logger(__name__)

LEADER_LOCK_KEY = int(os.getenv("LEADER_LOCK_KEY", "987654321"))
SCHEDULE_TICK_SECONDS = int(os.getenv("SCHEDULE_TICK_SECONDS", "60"))
LOOKAHEAD_SECONDS = int(os.getenv("LOOKAHEAD_SECONDS", "120"))
STALE_JOB_TIMEOUT_SECONDS = int(os.getenv("STALE_JOB_TIMEOUT_SECONDS", "300"))


class LeaderElector:
    def __init__(self, pod_name: str) -> None:
        self.pod_name = pod_name
        self._is_leader = False
        self._leader_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def start(self) -> None:
        self._leader_task = asyncio.create_task(self._election_loop())
        log.info("leader_elector_started", pod=self.pod_name)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._leader_task:
            self._leader_task.cancel()
            try:
                await self._leader_task
            except asyncio.CancelledError:
                pass
        if self._is_leader:
            await storage.release_leader_lock(LEADER_LOCK_KEY)
            log.info("leader_lock_released", pod=self.pod_name)
        self._is_leader = False

    async def _election_loop(self) -> None:
        """
        Continuously attempt to acquire/hold the leader lock.
        If acquired → run scheduling loop.
        If lost (connection drop) → another pod will win.
        """
        while not self._stop_event.is_set():
            try:
                acquired = await storage.try_acquire_leader_lock(LEADER_LOCK_KEY)

                if acquired and not self._is_leader:
                    self._is_leader = True
                    log.info("became_leader", pod=self.pod_name)

                if self._is_leader:
                    await self._scheduling_tick()

                elif not acquired:
                    if self._is_leader:
                        # Lost the lock (connection reset etc.)
                        self._is_leader = False
                        log.warning("lost_leader_lock", pod=self.pod_name)

                await asyncio.sleep(SCHEDULE_TICK_SECONDS)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("leader_loop_error", error=str(exc))
                await asyncio.sleep(10)  # back off on unexpected errors

    async def _scheduling_tick(self) -> None:
        """
        Core leader work: read schedules → compute next fires → enqueue.
        """
        now = datetime.now(timezone.utc)
        lookahead = now + timedelta(seconds=LOOKAHEAD_SECONDS)

        # Reset stale running jobs first (handles crashed workers)
        await storage.reset_stale_running_jobs(STALE_JOB_TIMEOUT_SECONDS)

        # Re-read ALL enabled schedules every tick — picks up new/modified jobs
        schedules = await storage.list_enabled_schedules()

        enqueued = 0
        for sched in schedules:
            try:
                fire_times = _compute_fire_times(sched, now, lookahead)
                for fire_time in fire_times:
                    jittered = _apply_jitter(fire_time, sched.jitter_seconds)
                    result = await storage.enqueue_job(sched.id, jittered)
                    if result:
                        enqueued += 1
            except Exception as exc:
                log.error(
                    "schedule_enqueue_error",
                    schedule_id=str(sched.id),
                    schedule_name=sched.name,
                    error=str(exc),
                )

        log.info(
            "scheduling_tick_complete",
            schedules_evaluated=len(schedules),
            jobs_enqueued=enqueued,
            pod=self.pod_name,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_fire_times(
    sched: JobSchedule,
    after: datetime,
    before: datetime,
) -> list[datetime]:
    """
    Return all cron fire times in (after, before] window.
    croniter is used for robust cron parsing.
    """
    try:
        cron = croniter(sched.cron_expr, after.replace(tzinfo=None))
    except ValueError as exc:
        log.error(
            "invalid_cron_expr",
            schedule=sched.name,
            expr=sched.cron_expr,
            error=str(exc),
        )
        return []

    fire_times: list[datetime] = []
    while True:
        next_dt = cron.get_next(datetime)
        aware_dt = next_dt.replace(tzinfo=timezone.utc)
        if aware_dt > before:
            break
        fire_times.append(aware_dt)

    return fire_times


def _apply_jitter(fire_time: datetime, jitter_seconds: int) -> datetime:
    """
    Spread jobs that share a schedule window to avoid thundering herd.
    jitter_seconds=0 → no spread (exact fire time).
    """
    if jitter_seconds <= 0:
        return fire_time
    offset = random.uniform(0, jitter_seconds)
    return fire_time + timedelta(seconds=offset)
