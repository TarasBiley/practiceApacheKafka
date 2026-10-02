CREATE TABLE audit_log (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    action TEXT NOT NULL
        CHECK (action IN ('login', 'view', 'purchase')),
    resource_id TEXT NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE stats_cache (
    id SERIAL PRIMARY KEY,
    action TEXT NOT NULL,
    count INT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sent', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE TABLE analytics_events (
    event_id UUID PRIMARY KEY,
    action TEXT NOT NULL
        CHECK (action IN ('login', 'view', 'purchase')),
    timestamp TIMESTAMPTZ NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL
);