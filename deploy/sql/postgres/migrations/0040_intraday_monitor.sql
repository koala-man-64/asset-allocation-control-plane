BEGIN;

CREATE TABLE IF NOT EXISTS core.intraday_watchlists (
    watchlist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    poll_interval_minutes INTEGER NOT NULL DEFAULT 5,
    refresh_cooldown_minutes INTEGER NOT NULL DEFAULT 15,
    auto_refresh_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    market_session TEXT NOT NULL DEFAULT 'us_equities_regular',
    next_due_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (poll_interval_minutes BETWEEN 1 AND 1440),
    CHECK (refresh_cooldown_minutes BETWEEN 1 AND 1440),
    CHECK (market_session IN ('us_equities_regular'))
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_watchlists_enabled_due
    ON core.intraday_watchlists(enabled, next_due_at);

CREATE TABLE IF NOT EXISTS core.intraday_watchlist_symbols (
    watchlist_id TEXT NOT NULL REFERENCES core.intraday_watchlists(watchlist_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL REFERENCES core.symbols(symbol) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (watchlist_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_watchlist_symbols_symbol
    ON core.intraday_watchlist_symbols(symbol);

CREATE TABLE IF NOT EXISTS core.intraday_monitor_runs (
    run_id TEXT PRIMARY KEY,
    watchlist_id TEXT NOT NULL REFERENCES core.intraday_watchlists(watchlist_id) ON DELETE CASCADE,
    trigger_kind TEXT NOT NULL DEFAULT 'scheduled',
    status TEXT NOT NULL DEFAULT 'queued',
    force_refresh BOOLEAN NOT NULL DEFAULT FALSE,
    claim_token TEXT,
    execution_name TEXT,
    symbol_count INTEGER NOT NULL DEFAULT 0,
    observed_symbol_count INTEGER NOT NULL DEFAULT 0,
    eligible_refresh_count INTEGER NOT NULL DEFAULT 0,
    refresh_batch_count INTEGER NOT NULL DEFAULT 0,
    due_at TIMESTAMPTZ,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (trigger_kind IN ('scheduled', 'manual')),
    CHECK (status IN ('queued', 'claimed', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_monitor_runs_status_due
    ON core.intraday_monitor_runs(status, due_at, queued_at);

CREATE INDEX IF NOT EXISTS idx_core_intraday_monitor_runs_watchlist
    ON core.intraday_monitor_runs(watchlist_id, queued_at DESC);

CREATE TABLE IF NOT EXISTS core.intraday_monitor_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES core.intraday_monitor_runs(run_id) ON DELETE CASCADE,
    watchlist_id TEXT NOT NULL REFERENCES core.intraday_watchlists(watchlist_id) ON DELETE CASCADE,
    symbol TEXT REFERENCES core.symbols(symbol) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (severity IN ('info', 'warning', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_monitor_events_watchlist_created
    ON core.intraday_monitor_events(watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_intraday_monitor_events_run_created
    ON core.intraday_monitor_events(run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS core.intraday_symbol_status (
    watchlist_id TEXT NOT NULL REFERENCES core.intraday_watchlists(watchlist_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL REFERENCES core.symbols(symbol) ON DELETE CASCADE,
    monitor_status TEXT NOT NULL DEFAULT 'idle',
    last_snapshot_at TIMESTAMPTZ,
    last_observed_price DOUBLE PRECISION,
    last_successful_market_refresh_at TIMESTAMPTZ,
    last_run_id TEXT REFERENCES core.intraday_monitor_runs(run_id) ON DELETE SET NULL,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (watchlist_id, symbol),
    CHECK (monitor_status IN ('idle', 'observed', 'refresh_queued', 'refreshed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_symbol_status_watchlist_updated
    ON core.intraday_symbol_status(watchlist_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS core.intraday_refresh_batches (
    batch_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES core.intraday_monitor_runs(run_id) ON DELETE CASCADE,
    watchlist_id TEXT NOT NULL REFERENCES core.intraday_watchlists(watchlist_id) ON DELETE CASCADE,
    domain TEXT NOT NULL DEFAULT 'market',
    bucket_letter TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    claim_token TEXT,
    execution_name TEXT,
    symbols JSONB NOT NULL DEFAULT '[]'::jsonb,
    symbol_count INTEGER NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (status IN ('queued', 'claimed', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_refresh_batches_status_created
    ON core.intraday_refresh_batches(status, created_at);

CREATE INDEX IF NOT EXISTS idx_core_intraday_refresh_batches_watchlist_created
    ON core.intraday_refresh_batches(watchlist_id, created_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.intraday_watchlists TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.intraday_watchlist_symbols TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.intraday_monitor_runs TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.intraday_monitor_events TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.intraday_symbol_status TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.intraday_refresh_batches TO backtest_service;
  END IF;
END $$;

COMMIT;
