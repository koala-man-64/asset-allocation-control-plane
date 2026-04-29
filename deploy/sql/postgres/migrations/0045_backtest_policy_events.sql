BEGIN;

CREATE TABLE IF NOT EXISTS core.backtest_policy_events (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
  bar_ts TIMESTAMPTZ NOT NULL,
  scope TEXT NOT NULL,
  policy_type TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  symbol TEXT,
  position_id TEXT,
  policy_id TEXT,
  observed_value DOUBLE PRECISION,
  threshold_value DOUBLE PRECISION,
  action TEXT,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (run_id, event_seq)
);

CREATE INDEX IF NOT EXISTS idx_backtest_policy_events_run_bar_seq
  ON core.backtest_policy_events(run_id, bar_ts, event_seq);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_policy_events TO backtest_service;
  END IF;
END $$;

COMMIT;
