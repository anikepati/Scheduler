"""
Entrypoint — runs on every OCP pod.

Startup sequence:
  1. Configure logging
  2. Connect DB pool + ensure schema exists
  3. Init HTTP client
  4. Start health server (liveness/readiness probes)
  5. Start scheduler (all pods — optimistic locking, no leader election)
  6. Start worker loop (all pods execute jobs)
  7. Wait for SIGTERM / SIGINT
  8. Graceful shutdown in reverse order
"""

from __future__ import annotations

import asyncio
import os
import signal

from logger import configure_logging, get_logger
from storage import close_pool, create_schema, init_pool
from runner import close_http_client, init_http_client
from leader import Scheduler          # FIX: all pods schedule via optimistic locking
from worker import Worker
from health import HealthServer

log = get_logger(__name__)


async def main() -> None:
    configure_logging()

    pod_name = os.environ.get("POD_NAME", "local-dev-pod")
    log.info("scheduler_starting", pod=pod_name)

    # -----------------------------------------------------------------------
    # Startup
    # -----------------------------------------------------------------------
    await init_pool()
    await create_schema()
    await init_http_client()

    scheduler = Scheduler(pod_name=pod_name)   # FIX: all pods run scheduler, no single leader
    worker    = Worker(pod_name=pod_name)
    health    = HealthServer(pod_name=pod_name, scheduler=scheduler, worker=worker)

    await health.start()
    await scheduler.start()
    await worker.start()

    log.info("scheduler_ready", pod=pod_name)

    # -----------------------------------------------------------------------
    # Wait for shutdown signal
    # -----------------------------------------------------------------------
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig) -> None:
        log.info("shutdown_signal_received", signal=sig.name, pod=pod_name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await stop_event.wait()

    # -----------------------------------------------------------------------
    # Graceful shutdown — reverse startup order
    # -----------------------------------------------------------------------
    log.info("scheduler_shutting_down", pod=pod_name)

    await worker.stop()       # drain in-flight jobs first
    await scheduler.stop()    # stop scheduling + listener
    await health.stop()       # stop accepting probe requests
    await close_http_client()
    await close_pool()

    log.info("scheduler_stopped", pod=pod_name)


if __name__ == "__main__":
    asyncio.run(main())
