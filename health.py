"""
Lightweight HTTP server for OCP liveness/readiness probes
and Prometheus-compatible /metrics endpoint.

Uses aiohttp — no FastAPI overhead for these simple endpoints.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from aiohttp import web

import storage
from logger import get_logger

if TYPE_CHECKING:
    from leader import LeaderElector
    from worker import Worker

log = get_logger(__name__)

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))

# Simple in-process counters (per-pod metrics)
_metrics: dict[str, float] = {
    "jobs_succeeded_total": 0,
    "jobs_failed_total": 0,
    "jobs_timeout_total": 0,
    "start_time": time.time(),
}


def increment_metric(name: str, value: float = 1.0) -> None:
    _metrics[name] = _metrics.get(name, 0) + value


class HealthServer:
    def __init__(self, pod_name: str, leader: "LeaderElector", worker: "Worker") -> None:
        self.pod_name = pod_name
        self.leader = leader
        self.worker = worker
        self._app = web.Application()
        self._runner: web.AppRunner | None = None
        self._setup_routes()

    def _setup_routes(self) -> None:
        self._app.router.add_get("/health/live", self._liveness)
        self._app.router.add_get("/health/ready", self._readiness)
        self._app.router.add_get("/metrics", self._metrics_handler)
        self._app.router.add_get("/status", self._status_handler)

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
    # OCP readiness probe — is the pod ready to serve?
    # Checks DB connectivity. OCP stops routing traffic if this fails.
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
    # -----------------------------------------------------------------------
    async def _metrics_handler(self, request: web.Request) -> web.Response:
        uptime = time.time() - _metrics["start_time"]
        lines = [
            f'# HELP scheduler_jobs_succeeded_total Total successful job executions',
            f'# TYPE scheduler_jobs_succeeded_total counter',
            f'scheduler_jobs_succeeded_total{{pod="{self.pod_name}"}} {_metrics.get("jobs_succeeded_total", 0)}',
            f'# HELP scheduler_jobs_failed_total Total failed job executions',
            f'# TYPE scheduler_jobs_failed_total counter',
            f'scheduler_jobs_failed_total{{pod="{self.pod_name}"}} {_metrics.get("jobs_failed_total", 0)}',
            f'# HELP scheduler_jobs_timeout_total Total timed-out job executions',
            f'# TYPE scheduler_jobs_timeout_total counter',
            f'scheduler_jobs_timeout_total{{pod="{self.pod_name}"}} {_metrics.get("jobs_timeout_total", 0)}',
            f'# HELP scheduler_uptime_seconds Scheduler pod uptime in seconds',
            f'# TYPE scheduler_uptime_seconds gauge',
            f'scheduler_uptime_seconds{{pod="{self.pod_name}"}} {uptime:.2f}',
            f'# HELP scheduler_is_leader Whether this pod is the current leader',
            f'# TYPE scheduler_is_leader gauge',
            f'scheduler_is_leader{{pod="{self.pod_name}"}} {1 if self.leader.is_leader else 0}',
        ]
        return web.Response(
            text="\n".join(lines) + "\n",
            content_type="text/plain",
        )

    # -----------------------------------------------------------------------
    # Human-readable status page
    # -----------------------------------------------------------------------
    async def _status_handler(self, request: web.Request) -> web.Response:
        return web.json_response({
            "pod": self.pod_name,
            "is_leader": self.leader.is_leader,
            "active_jobs": len(self.worker._active_tasks),
            "metrics": {k: v for k, v in _metrics.items() if k != "start_time"},
            "uptime_seconds": time.time() - _metrics["start_time"],
        })
