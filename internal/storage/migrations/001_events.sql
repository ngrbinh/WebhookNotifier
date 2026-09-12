CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    account_id TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','published','delivered','retriable','dead_letter')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMPTZ,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS events_pending_account_idx ON events (account_id, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS events_retry_idx ON events (next_retry_at, account_id) WHERE status = 'retriable';
CREATE TABLE IF NOT EXISTS event_attempts (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(id),
    attempt_number INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    status_code INTEGER,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
