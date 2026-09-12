CREATE TABLE IF NOT EXISTS simulator_accounts (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS simulator_webhooks (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES simulator_accounts(id) ON DELETE CASCADE,
    event_types TEXT[] NOT NULL,
    response_status INTEGER NOT NULL DEFAULT 200 CHECK (response_status BETWEEN 100 AND 599),
    response_delay_ms INTEGER NOT NULL DEFAULT 0 CHECK (response_delay_ms BETWEEN 0 AND 30000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (cardinality(event_types) BETWEEN 1 AND 3),
    CHECK (event_types <@ ARRAY['subscriber.created', 'subscriber.added_to_segment', 'subscriber.unsubscribed']::TEXT[])
);

CREATE INDEX IF NOT EXISTS simulator_webhooks_account_idx ON simulator_webhooks (account_id);

CREATE TABLE IF NOT EXISTS simulator_deliveries (
    id BIGSERIAL PRIMARY KEY,
    webhook_id TEXT NOT NULL REFERENCES simulator_webhooks(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL,
    response_status INTEGER NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS simulator_deliveries_webhook_received_idx ON simulator_deliveries (webhook_id, received_at DESC);