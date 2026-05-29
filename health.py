"""
Lightweight HTTP server for OCP liveness/readiness probes
and Prometheus-compatible /metrics endpoint.

FIX: Updated for Scheduler (no leader concept — all pods are active).
FIX: increment_metric() is now called from runner.py so counters are live.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

from aiohttp import web

import storage
from logger import get_logger

if TYPE_CHECKING:
    from leader import Scheduler
    from worker import Worker

log = get_logger(__name__)

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))

# ---------------------------------------------------------------------------
# Per-pod in-process counters
# Incremented by runner.py via increment_metric().
# ---------------------------------------------------------------------------
_metrics: dict[str, float] = {
    "jobs_succeeded_total": 0,
    "jobs_failed_total":    0,
    "jobs_timeout_total":   0,
    "start_time":           time.time(),
}


def increment_metric(name: str, value: float = 1.0) -> None:
    """Called from runner.py on each job completion. Thread-safe for asyncio."""
    _metrics[name] = _metrics.get(name, 0) + value


class HealthServer:
    def __init__(
        self,
        pod_name: str,
        scheduler: "Scheduler",    # FIX: all pods are active; no single leader
        worker: "Worker",
    ) -> None:
        self.pod_name  = pod_name
        self.scheduler = scheduler
        self.worker    = worker
        self._app      = web.Application()
        self._runner: web.AppRunner | None = None
        self._setup_routes()

    def _setup_routes(self) -> None:
        self._app.router.add_get("/health/live",  self._liveness)
        self._app.router.add_get("/health/ready", self._readiness)
        self._app.router.add_get("/metrics",      self._metrics_handler)
        self._app.router.add_get("/status",       self._status_handler)

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", HEALTH_PORT)
        await site.start()
        log.info("health_server_started", port=HEALTH_PORT)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
        log.info("health_server_stopped")

    # -----------------------------------------------------------------------
    # OCP liveness probe — is the process alive?
    # -----------------------------------------------------------------------
    async def _liveness(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "alive", "pod": self.pod_name})

    # -----------------------------------------------------------------------
    # OCP readiness probe — is the pod ready?
    # Checks DB connectivity. OCP stops routing traffic if this returns 503.
    # -----------------------------------------------------------------------
    async def _readiness(self, request: web.Request) -> web.Response:
        db_ok = await storage.ping()
        if db_ok:
            return web.json_response(
                {"status": "ready", "pod": self.pod_name, "db": "ok"}
            )
        return web.json_response(
            {"status": "not_ready", "pod": self.pod_name, "db": "unreachable"},
            status=503,
        )

    # -----------------------------------------------------------------------
    # Prometheus-compatible /metrics
    # FIX: renamed metric
    # -----------------------------------------------------------------------
    async def _metrics_handler(self, request: web.Request) -> web.Response:
        uptime = time.time() - _metrics["start_time"]
        pod    = self.pod_name
        lines = [
            f'# HELP scheduler_jobs_succeeded_total Total successful executions',
            f'# TYPE scheduler_jobs_succeeded_total counter',
            f'scheduler_jobs_succeeded_total{{pod="{pod}"}} {_metrics.get("jobs_succeeded_total", 0)}',

            f'# HELP scheduler_jobs_failed_total Total failed executions',
            f'# TYPE scheduler_jobs_failed_total counter',
            f'scheduler_jobs_failed_total{{pod="{pod}"}} {_metrics.get("jobs_failed_total", 0)}',

            f'# HELP scheduler_jobs_timeout_total Total timed-out executions',
            f'# TYPE scheduler_jobs_timeout_total counter',
            f'scheduler_jobs_timeout_total{{pod="{pod}"}} {_metrics.get("jobs_timeout_total", 0)}',

            f'# HELP scheduler_uptime_seconds Pod uptime in seconds',
            f'# TYPE scheduler_uptime_seconds gauge',
            f'scheduler_uptime_seconds{{pod="{pod}"}} {uptime:.2f}',

            f'# HELP scheduler_is_active Whether this pod scheduler loop is running',
            f'# TYPE scheduler_is_active gauge',
            f'scheduler_is_active{{pod="{pod}"}} {1 if self.scheduler.is_active else 0}',

            f'# HELP scheduler_active_jobs Current in-flight job count',
            f'# TYPE scheduler_active_jobs gauge',
            f'scheduler_active_jobs{{pod="{pod}"}} {len(self.worker._active_tasks)}',
        ]
        return web.Response(
            text="\n".join(lines) + "\n",
            content_type="text/plain",
        )

    # -----------------------------------------------------------------------
    # Human-readable status
    # -----------------------------------------------------------------------
    async def _status_handler(self, request: web.Request) -> web.Response:
        return web.json_response({
            "pod":            self.pod_name,
            "is_active":      self.scheduler.is_active,   # FIX: renamed field
            "active_jobs":    len(self.worker._active_tasks),
            "metrics":        {k: v for k, v in _metrics.items() if k != "start_time"},
            "uptime_seconds": time.time() - _metrics["start_time"],
        })
