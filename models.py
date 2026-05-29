"""
Pydantic models representing database rows and domain objects.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class AuthType(str, Enum):
    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class ExecutionStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

class ApiConfig(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str
    url: str
    method: str = "GET"
    headers: dict[str, Any] = Field(default_factory=dict)
    auth_type: AuthType = AuthType.NONE
    # Name of OCP/K8s env var that holds the credential — never the cred itself
    auth_secret_ref: Optional[str] = None
    payload_template: Optional[dict[str, Any]] = None
    timeout_seconds: int = 30
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class JobSchedule(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    api_config_id: uuid.UUID
    name: str
    cron_expr: str                     # standard 5-field cron
    enabled: bool = True
    jitter_seconds: int = 0            # anti-thundering-herd spread
    retry_attempts: int = 3
    retry_backoff_sec: int = 5
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class JobQueueItem(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    job_schedule_id: uuid.UUID
    scheduled_at: datetime
    claimed_by: Optional[str] = None
    claimed_at: Optional[datetime] = None
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    next_retry_at: Optional[datetime] = None


class JobExecutionLog(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    job_queue_id: uuid.UUID
    job_schedule_id: uuid.UUID
    api_config_id: uuid.UUID
    pod_name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: ExecutionStatus
    http_status_code: Optional[int] = None
    response_body: Optional[Any] = None
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None
