BEGIN;

CREATE TABLE IF NOT EXISTS core.intraday_watchlist_events (
    event_id TEXT PRIMARY KEY,
    watchlist_id TEXT NOT NULL REFERENCES core.intraday_watchlists(watchlist_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (event_type IN ('symbols_appended')),
    actor TEXT,
    request_id TEXT,
    reason TEXT,
    symbols_added JSONB NOT NULL DEFAULT '[]'::jsonb,
    symbols_already_present JSONB NOT NULL DEFAULT '[]'::jsonb,
    symbol_count_before INTEGER NOT NULL DEFAULT 0 CHECK (symbol_count_before >= 0),
    symbol_count_after INTEGER NOT NULL DEFAULT 0 CHECK (symbol_count_after >= 0),
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_watchlist_events_watchlist_created
    ON core.intraday_watchlist_events(watchlist_id, created_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT ON TABLE core.intraday_watchlist_events TO backtest_service;
  END IF;
END $$;

COMMIT;
