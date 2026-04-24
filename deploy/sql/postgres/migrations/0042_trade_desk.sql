CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.trade_accounts (
    account_id text PRIMARY KEY,
    name text NOT NULL,
    provider text NOT NULL CHECK (provider IN ('alpaca', 'etrade', 'schwab')),
    environment text NOT NULL CHECK (environment IN ('paper', 'sandbox', 'live')),
    provider_account_key text,
    account_number_masked text,
    base_currency text NOT NULL DEFAULT 'USD',
    enabled boolean NOT NULL DEFAULT true,
    live_trading_allowed boolean NOT NULL DEFAULT false,
    kill_switch_active boolean NOT NULL DEFAULT false,
    account_payload jsonb NOT NULL,
    detail_payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trade_accounts_provider_environment
    ON core.trade_accounts (provider, environment)
    WHERE enabled = true;

CREATE TABLE IF NOT EXISTS core.trade_positions (
    account_id text NOT NULL REFERENCES core.trade_accounts (account_id) ON DELETE CASCADE,
    symbol text NOT NULL,
    position_payload jsonb NOT NULL,
    as_of timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_trade_positions_account_as_of
    ON core.trade_positions (account_id, as_of DESC);

CREATE TABLE IF NOT EXISTS core.trade_orders (
    order_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES core.trade_accounts (account_id) ON DELETE RESTRICT,
    provider text NOT NULL CHECK (provider IN ('alpaca', 'etrade', 'schwab')),
    environment text NOT NULL CHECK (environment IN ('paper', 'sandbox', 'live')),
    status text NOT NULL CHECK (
        status IN (
            'draft',
            'previewed',
            'submitted',
            'accepted',
            'partially_filled',
            'filled',
            'cancel_pending',
            'cancelled',
            'rejected',
            'expired',
            'unknown_reconcile_required'
        )
    ),
    symbol text NOT NULL,
    side text NOT NULL CHECK (side IN ('buy', 'sell')),
    client_request_id text,
    idempotency_key text,
    provider_order_id text,
    request_hash text,
    request_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    response_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    order_payload jsonb NOT NULL,
    reconciliation_required boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trade_orders_account_updated
    ON core.trade_orders (account_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_orders_provider_order
    ON core.trade_orders (provider, provider_order_id)
    WHERE provider_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trade_orders_reconciliation
    ON core.trade_orders (updated_at DESC)
    WHERE reconciliation_required = true OR status = 'unknown_reconcile_required';

CREATE TABLE IF NOT EXISTS core.trade_order_idempotency (
    account_id text NOT NULL REFERENCES core.trade_accounts (account_id) ON DELETE RESTRICT,
    action text NOT NULL CHECK (action IN ('place', 'cancel')),
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    actor text,
    response_payload jsonb NOT NULL,
    provider_order_id text,
    replay_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_replayed_at timestamptz,
    PRIMARY KEY (account_id, action, idempotency_key)
);

CREATE TABLE IF NOT EXISTS core.trade_desk_audit_events (
    event_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES core.trade_accounts (account_id) ON DELETE RESTRICT,
    order_id text,
    provider text NOT NULL CHECK (provider IN ('alpaca', 'etrade', 'schwab')),
    environment text NOT NULL CHECK (environment IN ('paper', 'sandbox', 'live')),
    event_type text NOT NULL CHECK (
        event_type IN (
            'preview',
            'submit',
            'cancel',
            'status_update',
            'reject',
            'fill',
            'reconcile',
            'system_block',
            'authz_block'
        )
    ),
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    actor text,
    client_request_id text,
    idempotency_key text,
    status_before text,
    status_after text,
    event_payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trade_audit_account_occurred
    ON core.trade_desk_audit_events (account_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_audit_order
    ON core.trade_desk_audit_events (order_id)
    WHERE order_id IS NOT NULL;
