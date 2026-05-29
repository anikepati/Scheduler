-- =============================================================================
-- seed_jobs.sql
-- Example: adding API configs and job schedules directly in Postgres.
-- No redeployment needed — leader picks up changes within one tick (~60s).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Add an API config (what to call)
-- ---------------------------------------------------------------------------
INSERT INTO api_configs (
    name,
    url,
    method,
    headers,
    auth_type,
    auth_secret_ref,    -- name of OCP env var holding the credential
    payload_template,
    timeout_seconds
) VALUES
(
    'market-data-feed',
    'https://api.example.com/v1/market/feed',
    'GET',
    '{"Accept": "application/json"}',
    'bearer',
    'MARKET_DATA_API_TOKEN',   -- OCP Secret key name
    NULL,
    30
),
(
    'risk-score-ingest',
    'https://risk.internal/api/score',
    'POST',
    '{"Content-Type": "application/json"}',
    'api_key',
    'RISK_API_KEY',
    '{"source": "scheduler", "version": "1"}',
    45
),
(
    'health-check-external',
    'https://status.external-partner.com/ping',
    'GET',
    '{}',
    'none',
    NULL,
    NULL,
    10
);

-- ---------------------------------------------------------------------------
-- 2. Add job schedules (when to call)
-- ---------------------------------------------------------------------------
INSERT INTO job_schedules (
    api_config_id,
    name,
    cron_expr,
    enabled,
    jitter_seconds,
    retry_attempts,
    retry_backoff_sec
)
SELECT
    id,
    'market-data-feed-every-5min',
    '*/5 * * * *',     -- every 5 minutes
    TRUE,
    30,                -- spread up to 30s to avoid thundering herd
    3,
    5
FROM api_configs WHERE name = 'market-data-feed';

INSERT INTO job_schedules (
    api_config_id,
    name,
    cron_expr,
    enabled,
    jitter_seconds,
    retry_attempts,
    retry_backoff_sec
)
SELECT
    id,
    'risk-score-hourly',
    '0 * * * *',       -- top of every hour
    TRUE,
    60,
    3,
    10
FROM api_configs WHERE name = 'risk-score-ingest';

INSERT INTO job_schedules (
    api_config_id,
    name,
    cron_expr,
    enabled,
    jitter_seconds,
    retry_attempts,
    retry_backoff_sec
)
SELECT
    id,
    'external-health-check-every-minute',
    '* * * * *',       -- every minute
    TRUE,
    0,                 -- no jitter for health checks
    1,
    5
FROM api_configs WHERE name = 'health-check-external';

-- ---------------------------------------------------------------------------
-- 3. Common operational queries
-- ---------------------------------------------------------------------------

-- Pause a job (takes effect within next leader tick)
-- UPDATE job_schedules SET enabled = FALSE WHERE name = 'market-data-feed-every-5min';

-- Change a schedule
-- UPDATE job_schedules SET cron_expr = '*/10 * * * *' WHERE name = 'market-data-feed-every-5min';

-- View last 20 executions for a job
-- SELECT jel.started_at, jel.status, jel.http_status_code, jel.duration_ms, jel.error_message
-- FROM job_execution_log jel
-- JOIN job_schedules js ON jel.job_schedule_id = js.id
-- WHERE js.name = 'market-data-feed-every-5min'
-- ORDER BY jel.started_at DESC
-- LIMIT 20;

-- View all failures in the last hour
-- SELECT js.name, jel.started_at, jel.http_status_code, jel.error_message
-- FROM job_execution_log jel
-- JOIN job_schedules js ON jel.job_schedule_id = js.id
-- WHERE jel.status = 'failed'
--   AND jel.started_at > NOW() - INTERVAL '1 hour'
-- ORDER BY jel.started_at DESC;

-- View pending queue depth
-- SELECT js.name, COUNT(*) as pending_count
-- FROM job_queue jq
-- JOIN job_schedules js ON jq.job_schedule_id = js.id
-- WHERE jq.status = 'pending'
-- GROUP BY js.name
-- ORDER BY pending_count DESC;

-- View which pod is doing the most work
-- SELECT pod_name, COUNT(*) as executions, AVG(duration_ms) as avg_ms
-- FROM job_execution_log
-- WHERE started_at > NOW() - INTERVAL '1 hour'
-- GROUP BY pod_name;
