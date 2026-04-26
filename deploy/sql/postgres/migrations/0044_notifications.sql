CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.notification_requests (
    request_id text PRIMARY KEY,
    source_repo text NOT NULL,
    source_system text,
    client_request_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('message', 'trade_approval')),
    status text NOT NULL CHECK (status IN ('pending', 'delivered', 'delivery_failed', 'decided', 'expired')),
    title text NOT NULL,
    description text NOT NULL,
    target_url text,
    request_payload jsonb NOT NULL,
    trade_approval_payload jsonb,
    decision_status text NOT NULL CHECK (decision_status IN ('not_required', 'pending', 'approved', 'denied', 'expired')),
    decision text CHECK (decision IS NULL OR decision IN ('approve', 'deny')),
    decided_at timestamptz,
    decided_by text,
    execution_status text NOT NULL CHECK (
        execution_status IN ('not_applicable', 'pending_approval', 'submitted', 'blocked', 'release_failed')
    ),
    execution_order_id text,
    execution_message text,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_repo, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_notification_requests_status_updated
    ON core.notification_requests (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_notification_requests_source_client
    ON core.notification_requests (source_repo, client_request_id);

CREATE TABLE IF NOT EXISTS core.notification_recipients (
    request_id text NOT NULL REFERENCES core.notification_requests (request_id) ON DELETE CASCADE,
    recipient_id text NOT NULL,
    display_name text,
    email text,
    phone_number text,
    channels jsonb NOT NULL DEFAULT '[]'::jsonb,
    recipient_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (request_id, recipient_id)
);

CREATE TABLE IF NOT EXISTS core.notification_delivery_attempts (
    attempt_id text PRIMARY KEY,
    request_id text NOT NULL REFERENCES core.notification_requests (request_id) ON DELETE CASCADE,
    recipient_id text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('email', 'sms')),
    address text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'sent', 'failed', 'skipped')),
    provider text,
    provider_message_id text,
    attempt_number integer NOT NULL DEFAULT 1 CHECK (attempt_number >= 1),
    sanitized_error text,
    attempted_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_delivery_request
    ON core.notification_delivery_attempts (request_id, attempted_at DESC);

CREATE INDEX IF NOT EXISTS idx_notification_delivery_pending
    ON core.notification_delivery_attempts (created_at, attempt_id)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS core.notification_action_tokens (
    token_id text PRIMARY KEY,
    request_id text NOT NULL REFERENCES core.notification_requests (request_id) ON DELETE CASCADE,
    recipient_id text NOT NULL,
    token_hash text NOT NULL UNIQUE,
    expires_at timestamptz,
    viewed_at timestamptz,
    used_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_action_tokens_request
    ON core.notification_action_tokens (request_id);

CREATE INDEX IF NOT EXISTS idx_notification_action_tokens_expiry
    ON core.notification_action_tokens (expires_at)
    WHERE used_at IS NULL;

CREATE TABLE IF NOT EXISTS core.notification_audit_events (
    event_id text PRIMARY KEY,
    request_id text NOT NULL REFERENCES core.notification_requests (request_id) ON DELETE CASCADE,
    token_id text,
    event_type text NOT NULL CHECK (
        event_type IN (
            'created',
            'delivered',
            'delivery_failed',
            'viewed',
            'approved',
            'denied',
            'expired',
            'execution_submitted',
            'execution_blocked',
            'execution_failed',
            'execution_release_failed'
        )
    ),
    actor text,
    summary text NOT NULL DEFAULT '',
    event_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_audit_request_occurred
    ON core.notification_audit_events (request_id, occurred_at DESC);
