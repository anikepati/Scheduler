"""
Scheduler — runs on ALL pods. No leader election.

FIX: Replaces Postgres advisory lock (which is broken with connection
pooling) with per-schedule optimistic locking via UPDATE last_enqueued_at.
All pods race per schedule; only the pod that wins UPDATE 1 enqueues.

FIX: Adds LISTEN/NOTIFY so a new or updated schedule activates within
milliseconds instead of waiting up to SCHEDULE_TICK_SECONDS.

Responsibilities:
  - All pods: poll job_schedules, attempt optimistic enqueue per schedule
  - All pods: listen for schedule_changed NOTIFY, wake up immediately
  - All pods: reset stale 'running' jobs (idempotent — only one pod wins)
  - All pods: periodic job_queue cleanup
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timedelta, timezone

import asyncpg
from croniter import croniter

import storage
from logger import get_logger
from models import JobSchedule

log = get_logger(__name__)

SCHEDULE_TICK_SECONDS   = int(os.getenv("SCHEDULE_TICK_SECONDS",   "60"))
LOOKAHEAD_SECONDS       = int(os.getenv("LOOKAHEAD_SECONDS",       "120"))
STALE_JOB_TIMEOUT_SECS  = int(os.getenv("STALE_JOB_TIMEOUT_SECONDS", "300"))
QUEUE_RETAIN_DAYS       = int(os.getenv("QUEUE_RETAIN_DAYS",       "7"))
# Purge old queue rows every N ticks (default ~1 hour at 60s tick)
_PURGE_EVERY_N_TICKS    = int(os.getenv("PURGE_EVERY_N_TICKS",     "60"))


class Scheduler:
    """
    Runs on every pod. Schedules jobs via optimistic locking — no leader needed.
    """

    def __init__(self, pod_name: str) -> None:
        self.pod_name  = pod_name
        self._stop_event = asyncio.Event()
        # FIX: NOTIFY wakeup — set by _on_notify, cleared after each tick
        self._notify_event = asyncio.Event()
        self._scheduling_task: asyncio.Task | None = None
        self._listener_task:   asyncio.Task | None = None
        self._tick_count = 0

    @property
    def is_active(self) -> bool:
        return (
            self._scheduling_task is not None
            and not self._scheduling_task.done()
        )

    async def start(self) -> None:
        self._scheduling_task = asyncio.create_task(self._scheduling_loop())
        self._listener_task   = asyncio.create_task(self._change_listener())
        log.info("scheduler_started", pod=self.pod_name)

    async def stop(self) -> None:
        self._stop_event.set()
        self._notify_event.set()   # unblock any waiting asyncio.wait

        for task in (self._scheduling_task, self._listener_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        log.info("scheduler_stopped", pod=self.pod_name)

    # -----------------------------------------------------------------------
    # Scheduling loop — periodic tick + instant wakeup via NOTIFY
    # -----------------------------------------------------------------------

    async def _scheduling_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._scheduling_tick()

                # Wait for whichever comes first:
                #   (a) regular tick interval, or
                #   (b) immediate NOTIFY wakeup from Postgres
                sleep_task  = asyncio.create_task(asyncio.sleep(SCHEDULE_TICK_SECONDS))
                notify_task = asyncio.create_task(self._notify_event.wait())

                done, pending = await asyncio.wait(
                    {sleep_task, notify_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

                self._notify_event.clear()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("scheduling_loop_error", error=str(exc), pod=self.pod_name)
                await asyncio.sleep(10)

    async def _scheduling_tick(self) -> None:
        self._tick_count += 1
        now      = datetime.now(timezone.utc)
        lookahead = now + timedelta(seconds=LOOKAHEAD_SECONDS)

        # All pods call this — only one will win each UPDATE (idempotent)
        await storage.reset_stale_running_jobs(STALE_JOB_TIMEOUT_SECS)

        schedules = await storage.list_enabled_schedules()
        enqueued  = 0

        for sched in schedules:
            try:
                fire_times = _compute_fire_times(sched, now, lookahead)
                for fire_time in fire_times:
                    jittered = _apply_jitter(fire_time, sched.jitter_seconds)
                    # FIX: optimistic locking — only one pod wins per fire slot
                    won = await storage.try_enqueue_schedule(sched.id, jittered)
                    if won:
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
            tick=self._tick_count,
            schedules_evaluated=len(schedules),
            jobs_enqueued=enqueued,
            pod=self.pod_name,
        )

        # FIX: periodic purge of old done/failed queue rows
        if self._tick_count % _PURGE_EVERY_N_TICKS == 0:
            await storage.purge_old_queue_entries(QUEUE_RETAIN_DAYS)

    # -----------------------------------------------------------------------
    # LISTEN/NOTIFY — dedicated persistent connection (not from pool)
    # FIX: instant schedule activation instead of polling delay
    # -----------------------------------------------------------------------

    async def _change_listener(self) -> None:
        """
        Maintains a dedicated long-lived Postgres connection to receive
        NOTIFY signals. On any schedule INSERT/UPDATE, all pods wake up
        immediately and run a scheduling tick.

        Auto-reconnects on connection loss.
        """
        while not self._stop_event.is_set():
            conn: asyncpg.Connection | None = None
            try:
                conn = await asyncpg.connect(
                    os.environ["POSTGRES_DSN"],
                    command_timeout=60,
                )
                await conn.add_listener("schedule_changed", self._on_notify)
                log.info("schedule_listener_connected", pod=self.pod_name)

                # Suspend here — asyncpg pumps NOTIFY signals via the event loop
                await self._stop_event.wait()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning(
                    "schedule_listener_reconnecting",
                    error=str(exc),
                    pod=self.pod_name,
                )
                await asyncio.sleep(5)
            finally:
                if conn and not conn.is_closed():
                    try:
                        await conn.remove_listener("schedule_changed", self._on_notify)
                        await conn.close()
                    except Exception:
                        pass

    def _on_notify(
        self,
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """Called synchronously by asyncpg from the event loop on NOTIFY."""
        log.info("schedule_change_notify", payload=payload, pod=self.pod_name)
        self._notify_event.set()   # wakes _scheduling_loop immediately


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_fire_times(
    sched: JobSchedule,
    after: datetime,
    before: datetime,
) -> list[datetime]:
    """
    Return all cron fire times in the half-open window (after, before].
    """
    try:
        # FIX: strip tz before feeding croniter, re-attach after
        cron = croniter(sched.cron_expr, after.astimezone(timezone.utc).replace(tzinfo=None))
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
        next_naive = cron.get_next(datetime)
        next_aware = next_naive.replace(tzinfo=timezone.utc)
        if next_aware > before:
            break
        fire_times.append(next_aware)

    return fire_times


def _apply_jitter(fire_time: datetime, jitter_seconds: int) -> datetime:
    """Random spread to avoid thundering herd. Zero jitter = exact fire time."""
    if jitter_seconds <= 0:
        return fire_time
    return fire_time + timedelta(seconds=random.uniform(0, jitter_seconds))
