# Distributed API Scheduler

A lightweight, enterprise-grade Python scheduler for executing 300+ API schedules
across 4 OCP clusters with Postgres as the sole source of truth.

---

## Architecture Overview

```
Postgres (single source of truth)
    ├── api_configs        ← What to call (URL, method, auth, payload)
    ├── job_schedules      ← When to call (cron, retries, jitter)
    ├── job_queue          ← Execution coordination between pods
    └── job_execution_log  ← Immutable audit trail

           ↑ read/write by

OCP Pods (4 replicas across clusters)
    ├── Leader pod   → polls job_schedules, enqueues due jobs
    └── All pods     → claim jobs via SKIP LOCKED, execute concurrently
```

### Leader Election

Only **one pod** enqueues jobs at a time, using a Postgres advisory lock
(`pg_try_advisory_lock`). If the leader pod dies, another pod wins the lock
within one scheduling tick. No Zookeeper, Redis, or etcd required.

### Worker Dispatch

All pods (including the leader) execute jobs by polling `job_queue` using
`SELECT FOR UPDATE SKIP LOCKED` — Postgres's built-in mechanism for exactly-once
distributed work dispatch. No job is executed twice.

---

## File Structure

```
scheduler/
├── main.py          # Entrypoint — startup, signal handling, shutdown
├── leader.py        # Advisory lock election + scheduling loop
├── worker.py        # Job claim + execution loop
├── runner.py        # httpx API call + tenacity retry
├── storage.py       # asyncpg pool + all SQL queries
├── models.py        # Pydantic domain models
├── secrets.py       # OCP secret resolution (env var / mounted file)
├── health.py        # /health/live, /health/ready, /metrics endpoints
├── logger.py        # structlog JSON logging
├── requirements.txt
├── Dockerfile
├── deployment.yaml  # OCP Deployment, Secret, ConfigMap, ServiceMonitor
└── seed_jobs.sql    # Example job definitions + operational queries
```

---

## Database Schema

### `api_configs` — What to call

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `name` | TEXT | Human label (unique) |
| `url` | TEXT | Full endpoint URL |
| `method` | TEXT | GET / POST / PUT / PATCH / DELETE |
| `headers` | JSONB | Static request headers |
| `auth_type` | TEXT | `none` / `bearer` / `api_key` / `basic` |
| `auth_secret_ref` | TEXT | OCP env var name holding the credential |
| `payload_template` | JSONB | Request body (for POST/PUT) |
| `timeout_seconds` | INT | Per-request timeout |

### `job_schedules` — When to call

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `api_config_id` | UUID | FK → api_configs |
| `name` | TEXT | Human label (unique) |
| `cron_expr` | TEXT | Standard 5-field cron (`*/5 * * * *`) |
| `enabled` | BOOL | Toggle without deleting |
| `jitter_seconds` | INT | Random spread to prevent thundering herd |
| `retry_attempts` | INT | Max retries on failure |
| `retry_backoff_sec` | INT | Exponential backoff base |

### `job_queue` — Execution coordination

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `job_schedule_id` | UUID | FK → job_schedules |
| `scheduled_at` | TIMESTAMPTZ | When this execution should fire |
| `claimed_by` | TEXT | Pod name that claimed this job |
| `status` | TEXT | `pending` / `running` / `done` / `failed` |
| `attempts` | INT | Execution attempt count |

### `job_execution_log` — Immutable audit trail

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `pod_name` | TEXT | Which pod ran this |
| `started_at` | TIMESTAMPTZ | Execution start |
| `finished_at` | TIMESTAMPTZ | Execution end |
| `status` | TEXT | `success` / `failed` / `timeout` |
| `http_status_code` | INT | API response code |
| `response_body` | JSONB | Full API response |
| `duration_ms` | INT | Execution time in milliseconds |
| `error_message` | TEXT | Error detail on failure |

---

## Configuration

All configuration is via environment variables, injected by OCP via
`Secret` and `ConfigMap` in `deployment.yaml`.

### Required

| Variable | Description |
|----------|-------------|
| `POSTGRES_DSN` | Full asyncpg DSN: `postgresql://user:pass@host:5432/db` |
| `POD_NAME` | Injected by OCP downward API — used as worker identity |

### Optional (with defaults)

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULE_TICK_SECONDS` | `60` | How often leader checks for due jobs |
| `LOOKAHEAD_SECONDS` | `120` | How far ahead to enqueue jobs |
| `STALE_JOB_TIMEOUT_SECONDS` | `300` | Reset running jobs older than this |
| `WORKER_CONCURRENCY` | `20` | Max concurrent API calls per pod |
| `WORKER_BATCH_SIZE` | `10` | Jobs claimed per poll cycle |
| `WORKER_POLL_INTERVAL_SECONDS` | `5` | Worker polling interval |
| `HEALTH_PORT` | `8080` | Port for health/metrics endpoints |
| `DB_POOL_MIN` | `2` | Min DB connections per pod |
| `DB_POOL_MAX` | `10` | Max DB connections per pod |
| `LOG_LEVEL` | `INFO` | Log verbosity |

---

## Auth Configuration

Credentials are **never stored in Postgres**. `auth_secret_ref` holds the name
of an environment variable (or mounted secret file) that contains the credential.

### Bearer Token
```sql
UPDATE api_configs SET
    auth_type = 'bearer',
    auth_secret_ref = 'MY_API_TOKEN'   -- OCP Secret key name
WHERE name = 'my-api';
```
The scheduler reads `os.environ["MY_API_TOKEN"]` at execution time.

### API Key
```sql
UPDATE api_configs SET
    auth_type = 'api_key',
    auth_secret_ref = 'MY_API_KEY_SECRET'
WHERE name = 'my-api';
```
Secret value format: `Header-Name:value` (e.g. `X-API-Key:abc123`)

### Basic Auth
Secret value format: `username:password`

### Secret rotation
Update the OCP Secret. The scheduler reads it fresh on every job execution —
no restart required.

---

## Adding / Modifying Jobs

No code changes. No redeployment. Just SQL.

### Add a new job
```sql
-- 1. Define what to call
INSERT INTO api_configs (name, url, method, auth_type, auth_secret_ref, timeout_seconds)
VALUES ('my-new-api', 'https://api.example.com/endpoint', 'GET', 'bearer', 'MY_TOKEN', 30);

-- 2. Define when to call it
INSERT INTO job_schedules (api_config_id, name, cron_expr, enabled, jitter_seconds)
SELECT id, 'my-new-job-hourly', '0 * * * *', TRUE, 30
FROM api_configs WHERE name = 'my-new-api';
```
The leader picks it up within `SCHEDULE_TICK_SECONDS` (default 60s).

### Pause a job
```sql
UPDATE job_schedules SET enabled = FALSE WHERE name = 'my-new-job-hourly';
```

### Change the schedule
```sql
UPDATE job_schedules SET cron_expr = '*/15 * * * *' WHERE name = 'my-new-job-hourly';
```

### Delete a job
```sql
DELETE FROM job_schedules WHERE name = 'my-new-job-hourly';
-- Cascades: removes pending job_queue entries for this schedule
```

---

## OCP Deployment

### Prerequisites
- OCP 4.x cluster
- Postgres instance accessible from the cluster
- Container registry access

### Build and push
```bash
docker build -t your-registry/scheduler:latest .
docker push your-registry/scheduler:latest
```

### Deploy
```bash
# Update POSTGRES_DSN and any API secrets in deployment.yaml first
kubectl apply -f deployment.yaml

# Verify pods are running
kubectl get pods -n scheduler

# Check logs
kubectl logs -n scheduler -l app=scheduler -f

# Check which pod is leader
kubectl exec -n scheduler <pod-name> -- curl -s localhost:8080/status | python3 -m json.tool
```

### Health endpoints

| Endpoint | OCP Probe | Description |
|----------|-----------|-------------|
| `GET /health/live` | Liveness | Process alive check |
| `GET /health/ready` | Readiness | DB connectivity check |
| `GET /metrics` | Prometheus | Per-pod counters |
| `GET /status` | Manual | Leader status, active jobs |

---

## Operational Queries

```sql
-- Recent failures (last hour)
SELECT js.name, jel.started_at, jel.http_status_code, jel.error_message
FROM job_execution_log jel
JOIN job_schedules js ON jel.job_schedule_id = js.id
WHERE jel.status = 'failed'
  AND jel.started_at > NOW() - INTERVAL '1 hour'
ORDER BY jel.started_at DESC;

-- Pending queue depth by job
SELECT js.name, COUNT(*) AS pending
FROM job_queue jq
JOIN job_schedules js ON jq.job_schedule_id = js.id
WHERE jq.status = 'pending'
GROUP BY js.name ORDER BY pending DESC;

-- Slowest jobs (avg duration)
SELECT js.name, COUNT(*) AS runs, ROUND(AVG(duration_ms)) AS avg_ms
FROM job_execution_log jel
JOIN job_schedules js ON jel.job_schedule_id = js.id
WHERE jel.started_at > NOW() - INTERVAL '24 hours'
GROUP BY js.name ORDER BY avg_ms DESC LIMIT 20;

-- Pod workload distribution
SELECT pod_name, COUNT(*) AS executions, ROUND(AVG(duration_ms)) AS avg_ms
FROM job_execution_log
WHERE started_at > NOW() - INTERVAL '1 hour'
GROUP BY pod_name;

-- Success rate by job
SELECT js.name,
    COUNT(*) FILTER (WHERE jel.status = 'success') AS successes,
    COUNT(*) FILTER (WHERE jel.status = 'failed')  AS failures,
    ROUND(100.0 * COUNT(*) FILTER (WHERE jel.status = 'success') / COUNT(*), 1) AS success_pct
FROM job_execution_log jel
JOIN job_schedules js ON jel.job_schedule_id = js.id
WHERE jel.started_at > NOW() - INTERVAL '24 hours'
GROUP BY js.name ORDER BY success_pct ASC;
```

---

## Scaling Notes

| Scenario | Action |
|----------|--------|
| Add more execution capacity | Increase `replicas` in deployment.yaml |
| High DB connection pressure | Reduce `DB_POOL_MAX` or add a PgBouncer sidecar |
| Thundering herd (many jobs same minute) | Increase `jitter_seconds` per schedule |
| Slow API response times | Increase `timeout_seconds` on `api_configs` row |
| Backed-up queue | Increase `WORKER_CONCURRENCY` per pod |
| Leader pod thrashing | Increase `SCHEDULE_TICK_SECONDS` |

---

## Design Decisions

**Why Postgres advisory locks for leader election?**
Zero additional infrastructure. The lock is session-scoped — if the pod crashes,
the connection drops, the lock releases, and another pod wins within one tick.

**Why `SELECT FOR UPDATE SKIP LOCKED` for job dispatch?**
It's Postgres's native pattern for queue tables. Atomic claim with no application-level
locking. Works correctly across all 4 pods simultaneously with no coordination overhead.

**Why re-read all schedules every tick?**
This is intentional — it means adding/modifying/disabling a job in Postgres takes
effect within one tick with no cache invalidation logic needed.

**Why not APScheduler?**
APScheduler is designed for single-process scheduling. At 300+ jobs across 4 pods,
a DB-backed queue with `SKIP LOCKED` is simpler, more observable, and more resilient.
