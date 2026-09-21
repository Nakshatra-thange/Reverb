CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key  TEXT UNIQUE NOT NULL,
    queue            TEXT NOT NULL,
    payload          JSONB NOT NULL,
    priority         SMALLINT NOT NULL DEFAULT 1,   -- 0=high, 1=default, 2=low
    status           TEXT NOT NULL DEFAULT 'pending',
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 5,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON jobs (status, run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_priority ON jobs (queue, priority, created_at);

CREATE TABLE IF NOT EXISTS job_leases (
    job_id            UUID PRIMARY KEY REFERENCES jobs(id),
    worker_id         TEXT NOT NULL,
    leased_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    job_id           UUID PRIMARY KEY,
    payload          JSONB NOT NULL,
    failure_history  JSONB NOT NULL DEFAULT '[]',
    moved_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS recurring_jobs (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name             TEXT UNIQUE NOT NULL,
    queue            TEXT NOT NULL,
    payload          JSONB NOT NULL,
    priority         SMALLINT NOT NULL DEFAULT 1,
    cron_expression  TEXT NOT NULL,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_run_at      TIMESTAMPTZ,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recurring_due ON recurring_jobs (enabled, next_run_at);