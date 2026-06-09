# Workflow Execution Platform — System Design

A monolithic React + FastAPI application that executes long-running, agent-based
workflows as in-process background jobs, backed by a single relational database.
Modeled on the `adk web` / `adk api_server` pattern: one codebase, one image,
two run modes.

---

## 1. Goals and constraints

| Goal | Decision |
| --- | --- |
| Single deployable artifact | One image, one Deployment (monolith) |
| Two consumption modes | Bundled UI **or** headless API, same image |
| Long-running executions | In-process background job pool, not request-scoped |
| Progress streaming | Server-Sent Events (SSE) |
| Minimal infrastructure | **RDBMS only** — no Kafka, no Redis, no broker |
| Durable + resumable | Checkpointed state in the database; survives restarts |
| Human-in-the-loop pauses | Runs can park for days at zero compute cost |
| Target platform | OpenShift Container Platform (OCP) |

The defining choice: the database is the only stateful dependency. It serves as
the **work queue**, the **run state store**, and the **durable event log**.
Everything else is one stateless process replicated behind a Route.

---

## 2. Architecture at a glance

```
            ┌──────────────┐        ┌──────────────┐
            │  Built-in UI │        │  Custom UI   │
            │ (bundled SPA)│        │ (your app)   │
            └──────┬───────┘        └──────┬───────┘
                   └────────────┬──────────┘
                          OCP Route (TLS edge)
                                │
              ┌─────────────────▼──────────────────┐
              │        Pod  ×N  (one image)         │
              │  ┌────────────────────────────────┐ │
              │  │ FastAPI process                 │ │
              │  │  • /api/v1  routers             │ │
              │  │  • SPA static (when SERVE_UI)   │ │
              │  │  • SSE endpoints (LISTEN)       │ │
              │  │  • background job pool (claim   │ │
              │  │    → execute → checkpoint)      │ │
              │  └────────────────────────────────┘ │
              └─────────────────┬──────────────────┘
                                │
                        ┌───────▼────────┐
                        │   PostgreSQL   │
                        │  queue · state │
                        │  · event log   │
                        └────────────────┘
```

A single process per pod does everything: serves the API and (optionally) the
SPA, holds SSE connections, and runs the background workers that execute
workflows. Horizontal scale is just more replicas — `SKIP LOCKED` lets each
replica claim disjoint work with no coordinator.

---

## 3. Two run modes (one image)

The whole app is built from a single factory. The API router is always mounted;
the SPA is mounted only when `SERVE_UI` is true.

```python
def create_app(serve_ui: bool = False) -> FastAPI:
    app = FastAPI(title="myapp", version="1.0.0", lifespan=lifespan)
    app.include_router(api_router, prefix="/api/v1")   # always
    add_cors(app, settings.cors_origins)               # no-op if list empty
    add_health(app)                                    # /healthz, /readyz
    if serve_ui:
        mount_spa(app, settings.static_dir)            # StaticFiles + catch-all
    return app
```

| Mode | Command | Serves | Used by |
| --- | --- | --- | --- |
| Web (bundled) | `myapp web` | API + SPA | built-in UI, same origin, no CORS |
| API (headless) | `myapp api` | API only | a customer's own UI, other origin, CORS + token |

Mount order matters: `/api/v1` must be registered before the SPA catch-all, or
the catch-all swallows API calls. The auto-generated `/openapi.json` and `/docs`
are the integration contract for headless consumers — version the prefix
(`/api/v1`) from day one so the contract can evolve without breaking clients.

---

## 4. Project layout

```
myapp/
├── backend/
│   ├── app/
│   │   ├── main.py            # create_app factory + lifespan
│   │   ├── cli.py             # "myapp web" / "myapp api"
│   │   ├── api/
│   │   │   ├── router.py      # APIRouter(prefix="/api/v1")
│   │   │   ├── runs.py        # submit / status / resume / cancel / list
│   │   │   └── events.py      # SSE stream endpoint
│   │   ├── jobs/
│   │   │   ├── pool.py        # background executor pool (lifespan-managed)
│   │   │   ├── claim.py       # SKIP LOCKED claim + lease + sweep
│   │   │   └── runner.py      # executes a workflow, checkpoints, emits events
│   │   ├── workflows/         # ADK agents / DAG definitions (config-first)
│   │   ├── core/
│   │   │   ├── config.py      # pydantic-settings (env / ConfigMap / Secret)
│   │   │   ├── db.py          # connection pool, LISTEN/NOTIFY helpers
│   │   │   └── audit.py       # hash-chained tamper-evident audit log
│   │   ├── schemas/           # pydantic request/response models
│   │   └── spa.py             # StaticFiles mount + catch-all
│   ├── migrations/            # alembic
│   └── pyproject.toml
├── frontend/
│   ├── src/
│   │   ├── pages/
│   │   │   ├── RunsList.tsx   # page 1 — dashboard of runs
│   │   │   ├── RunDetail.tsx  # page 2 — live SSE timeline of one run
│   │   │   └── NewRun.tsx     # page 3 — submit a workflow
│   │   ├── App.tsx            # React Router, 3 routes
│   │   └── api/client.ts      # fetch wrapper -> /api/v1
│   └── vite.config.ts
├── Dockerfile                 # multi-stage: build SPA -> copy into python image
└── deploy/                    # ConfigMap, Secret, Deployment, Service, Route
```

The three pages map directly onto the run lifecycle: list runs, watch one run
live, and submit a new run.

---

## 5. Request lifecycle

1. Client `POST`s a workflow run. The API validates, inserts a `runs` row with
   status `queued` (honoring an `Idempotency-Key` so retries don't double-submit),
   `NOTIFY`s the queue channel, and returns **202** with a `run_id`.
2. A background executor in some pod is woken by the notification, claims the row
   with `SKIP LOCKED`, flips it to `running`, and begins executing the workflow.
3. As the workflow progresses it appends rows to `run_events` and `NOTIFY`s a
   lightweight pointer (run id + event id) so any pod streaming that run can
   fetch and forward the new event.
4. The client subscribes to `GET /runs/{id}/events` (SSE) and receives the
   timeline live; on reconnect it replays missed events via `Last-Event-ID`.
5. On completion the run is marked `succeeded`/`failed` with a result; a terminal
   event closes the stream.

The HTTP request that submits a run never waits for execution. The request that
streams events is long-lived but does no work — it only forwards.

---

## 6. The data model

```sql
CREATE TABLE runs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_type   text        NOT NULL,
    status          text        NOT NULL DEFAULT 'queued',  -- queued|running|
                                                            -- waiting_approval|
                                                            -- succeeded|failed|cancelled
    input           jsonb       NOT NULL,
    state           jsonb,                  -- checkpoint: resume from here
    result          jsonb,
    idempotency_key text UNIQUE,
    correlation_id  text        NOT NULL,   -- W3C trace context
    attempt         int         NOT NULL DEFAULT 0,
    claimed_at      timestamptz,            -- lease start
    lease_expires_at timestamptz,           -- crash recovery boundary
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON runs (status, created_at) WHERE status = 'queued';
CREATE INDEX ON runs (lease_expires_at)   WHERE status = 'running';

-- Append-only durable event log; the id IS the SSE Last-Event-ID
CREATE TABLE run_events (
    id          bigserial PRIMARY KEY,
    run_id      uuid     NOT NULL REFERENCES runs(id),
    type        text     NOT NULL,          -- step_started|step_finished|log|
                                            -- approval_requested|completed|error
    payload     jsonb    NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON run_events (run_id, id);

-- Durable barrier for within-run fan-out/fan-in (parallel branches)
CREATE TABLE run_steps (
    run_id      uuid    NOT NULL REFERENCES runs(id),
    step_id     text    NOT NULL,           -- the fan-out node
    branch_key  text    NOT NULL,           -- one row per parallel branch
    status      text    NOT NULL DEFAULT 'pending',  -- pending|done|failed
    output      jsonb,
    PRIMARY KEY (run_id, step_id, branch_key)
);
```

The `runs` table *is* the queue — there is no separate queue object. The
`run_events` table *is* the event stream — durable, replayable, and its
`bigserial` id doubles as the SSE event id.

---

## 7. Background job model (in-process)

The executor pool is started in FastAPI's `lifespan` and stopped gracefully on
shutdown. It runs in the same process as the API.

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    pool = ExecutorPool(
        concurrency=settings.max_concurrent_jobs,   # bounded slots per pod
        claim=claim_next_run,                        # SKIP LOCKED
        run=run_workflow,                            # execute + checkpoint
    )
    await pool.start()                               # also LISTENs run_queued
    try:
        yield
    finally:
        await pool.drain(timeout=settings.shutdown_grace)  # stop claiming,
        await db.disconnect()                              # finish/checkpoint
```

**Claiming work** — atomic, leaderless, no broker:

```sql
UPDATE runs SET status='running',
                claimed_at = now(),
                lease_expires_at = now() + interval '60 seconds',
                attempt = attempt + 1
WHERE id = (
    SELECT id FROM runs
    WHERE status = 'queued'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

Key properties:

- **Bounded concurrency.** A semaphore caps how many runs a pod executes at once,
  so a burst can't overwhelm the pod or starve the API.
- **Don't block the event loop.** Workflow steps are `await`ed; CPU-heavy work is
  offloaded with `run_in_executor` so SSE and API stay responsive.
- **Wake, don't poll.** Workers `LISTEN` on `run_queued`; the API `NOTIFY`s on
  insert, so workers sleep until there is work and a low-frequency timer is only
  a safety net.
- **Lease-based recovery.** Each claim sets `lease_expires_at`. A sweep re-queues
  `running` rows whose lease expired (crashed or killed pod), so no run is lost.
- **Idempotent steps.** Because a run may be re-claimed after a crash, every step
  must resume from the checkpoint rather than re-do completed side effects.

---

## 8. SSE streaming on the database alone

The fan-out problem (a client's SSE connection lives on one pod; events are
produced by a worker that may be on another pod) is solved with PostgreSQL
`LISTEN/NOTIFY` — no Redis required.

- The executor appends an event to `run_events`, then
  `NOTIFY run_events, '{"run_id": "...", "event_id": 123}'`.
- Every pod holding SSE connections `LISTEN`s on `run_events`. On a notification
  it checks whether it holds a connection for that `run_id`; if so it `SELECT`s
  the event row and pushes it down the stream.
- The notification carries only a pointer, never the payload, so the 8 KB NOTIFY
  limit is never an issue and the database remains the single source of truth.

**Reconnect and replay.** Every SSE frame sets `id:` to the `run_events.id`. On
reconnect the browser sends `Last-Event-ID`; the endpoint first replays
`SELECT … FROM run_events WHERE run_id = $1 AND id > $2 ORDER BY id`, then
switches to live. No event is ever lost across a dropped connection, and no
sticky sessions are needed.

> Connection budget: each SSE handler and each `LISTEN` holds a database
> connection. Size the pool accordingly, cap SSE connections per pod, and scale
> out rather than up. A connection pooler (e.g. pgbouncer) helps, but note that
> `LISTEN` requires session-level connections.

---

## 9. Parallel execution

**Across runs, one pod.** The executor pool runs up to `max_concurrent_jobs`
workflows at once, each in its own `asyncio` task.

**Across pods.** More replicas of the single Deployment. `SKIP LOCKED` guarantees
they claim disjoint rows with no coordination, no leader, no partition
assignment — which is exactly the leaderless model we want.

**Within a run (fan-out / fan-in).** A workflow can split into parallel branches
and join. This maps onto ADK's `ParallelAgent`:

- Lightweight, I/O-bound branches run in-process with `asyncio.gather`.
- Heavy or long branches are enqueued as their own `runs`/`run_steps` so other
  pods pick them up — true cross-pod parallelism.
- The **barrier is durable**: each branch writes its result to `run_steps`; when
  the count of `done` rows for the fan-out node equals K, the join fires and the
  parent continues. A crash mid-fan-out resumes from persisted branch results
  rather than restarting, because completion is tracked in the database, not in
  memory.

---

## 10. Human-in-the-loop and long pauses

When a workflow reaches an approval gate it does **not** hold a worker:

1. The executor checkpoints `runs.state`, sets status `waiting_approval`, emits an
   `approval_requested` event, and **releases its slot** back to the pool.
2. The run sits in the database indefinitely — a 40-day pause costs zero compute.
3. `POST /runs/{id}/resume` records the decision, sets status back to `queued`,
   and `NOTIFY`s. A worker re-claims and continues from the checkpoint.

This is the ADK `ResumabilityConfig` + database-session pattern: durable state
keyed by `output_key`-style completion tracking, so resumption is exact.

---

## 11. Scalability

In a single-deployment monolith you scale the whole pod, and you have three knobs:

| Knob | Effect |
| --- | --- |
| Replica count (HPA) | More pods → more API capacity **and** more executor slots |
| `max_concurrent_jobs` | Executor slots per pod (tune vs. CPU and DB pool) |
| DB connection pool | Ceiling on concurrent executors + SSE listeners |

- The HPA scales replicas on CPU/memory; because each replica also claims work,
  adding replicas scales execution throughput too. `SKIP LOCKED` keeps this safe.
- Keep `minReplicas ≥ 2`: the pod must always be up to answer requests and hold
  SSE connections, so scale-to-zero does not apply here.
- Watch the event loop: offload CPU-bound steps to a thread/process pool so long
  jobs never degrade API latency.
- Backpressure: cap queued depth and SSE connections per pod; surface queue depth
  and executor utilization as metrics so the HPA and operators have honest signals.

---

## 12. Deployment on OCP

**Single Deployment**, single image, multi-stage build.

```dockerfile
# Stage 1 — build the SPA
FROM node:20-alpine AS web
WORKDIR /web
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build            # -> /web/dist

# Stage 2 — runtime
FROM python:3.12-slim
WORKDIR /app
COPY backend/pyproject.toml ./
RUN pip install --no-cache-dir .
COPY backend/ ./
COPY --from=web /web/dist ./static
# OCP runs an arbitrary non-root UID: make files group-readable (GID 0)
RUN chgrp -R 0 /app && chmod -R g=u /app
EXPOSE 8080
CMD ["myapp", "web"]         # or "myapp api" for headless
```

OCP specifics that matter:

- **Arbitrary non-root UID** (`restricted-v2` SCC): never hardcode a UID; make
  the app group-writable for GID 0 and don't write to UID-specific paths.
- **Probes**: `/healthz` for liveness; `/readyz` returns ready only after the DB
  is reachable and migrations have run.
- **Graceful shutdown**: set `terminationGracePeriodSeconds` long enough for the
  pool to stop claiming and checkpoint in-flight runs after `SIGTERM`.
- **Config**: non-secret values in a ConfigMap, credentials in a Secret, injected
  as env and read by `pydantic-settings`.
- **Route**: TLS terminated at the edge.
- **Migrations**: run via an init container or on startup (alembic) before
  `/readyz` flips true.

```yaml
# deploy/deployment.yaml (essentials)
apiVersion: apps/v1
kind: Deployment
metadata: { name: myapp }
spec:
  replicas: 2
  selector: { matchLabels: { app: myapp } }
  template:
    metadata: { labels: { app: myapp } }
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: myapp
          image: image-registry/myapp:latest
          args: ["web"]                 # or "api"
          ports: [{ containerPort: 8080 }]
          envFrom:
            - configMapRef: { name: myapp-config }
            - secretRef:    { name: myapp-secrets }
          readinessProbe: { httpGet: { path: /readyz, port: 8080 } }
          livenessProbe:  { httpGet: { path: /healthz, port: 8080 } }
          resources:
            requests: { cpu: "500m", memory: "512Mi" }
            limits:   { cpu: "2",    memory: "2Gi" }
```

---

## 13. Configuration

`pydantic-settings`, sourced from env (ConfigMap/Secret). Workflow definitions
are YAML (config-first; no code change to add or tune a workflow).

| Key | Purpose |
| --- | --- |
| `DATABASE_URL` | Postgres connection |
| `SERVE_UI` | Mount the SPA (web) or not (api) |
| `CORS_ORIGINS` | Allowed origins for headless mode (empty in bundled) |
| `MAX_CONCURRENT_JOBS` | Executor slots per pod |
| `LEASE_SECONDS` | Claim lease / crash-recovery window |
| `JOB_POLL_INTERVAL` | Safety-net poll between notifications |
| `SHUTDOWN_GRACE` | Drain window on SIGTERM |

---

## 14. Observability and audit

- **Correlation**: a W3C trace context / `correlation_id` is created at submit and
  threaded through the queue row, the executor, every branch, and every event, so
  one run is traceable end to end.
- **Metrics**: runs by status, queue depth, executor utilization, claim latency,
  active SSE connections, run duration percentiles.
- **Audit**: each state transition appends to a hash-chained, tamper-evident audit
  log as a first-class output of execution — the compliance trail matches exactly
  what ran.

---

## 15. API surface

| Method & path | Purpose |
| --- | --- |
| `POST /api/v1/workflows/{type}/runs` | Submit a run (Idempotency-Key) → 202 + run_id |
| `GET /api/v1/runs` | List / filter runs |
| `GET /api/v1/runs/{id}` | Status snapshot |
| `GET /api/v1/runs/{id}/events` | SSE stream (Last-Event-ID replay) |
| `POST /api/v1/runs/{id}/resume` | HITL approval / resume from checkpoint |
| `POST /api/v1/runs/{id}/cancel` | Request cancellation |
| `GET /healthz`, `GET /readyz` | Probes |

---

## 16. Tech stack

| Layer | Choice |
| --- | --- |
| Frontend | React + Vite (TypeScript), React Router (3 pages) |
| Backend | FastAPI on uvicorn (async) |
| Agents / workflows | Google ADK (Sequential / Parallel / Loop agents) |
| Background jobs | In-process asyncio executor pool, lifespan-managed |
| Queue + state + events | PostgreSQL (`SKIP LOCKED`, `LISTEN/NOTIFY`) |
| Migrations | alembic |
| Packaging | Single multi-stage image |
| Platform | OpenShift Container Platform |

---

## 17. Future evolution (when, not now)

The design is deliberately lean. If load later demands it, each step is additive
and leaves the data model untouched:

- **Split the worker role** into its own Deployment from the *same image*
  (`myapp worker`) and autoscale it with KEDA's Postgres scaler (including
  scale-to-zero) when execution load starts contending with API responsiveness.
- **Add Redis pub/sub** for SSE fan-out only if event volume outgrows
  `LISTEN/NOTIFY`. The durable log and replay logic stay identical.
- **Adopt a durable-execution engine** (e.g. Temporal) only if timers, retries,
  and complex parallel joins justify another platform component.

Until then: one image, one Deployment, one database.
