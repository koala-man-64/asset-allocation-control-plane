BEGIN;

CREATE TABLE IF NOT EXISTS core.ranking_refresh_state (
  strategy_name TEXT PRIMARY KEY REFERENCES core.strategies(name) ON DELETE CASCADE,
  dependency_fingerprint TEXT,
  dependency_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  dirty_start_date DATE,
  dirty_end_date DATE,
  status TEXT NOT NULL DEFAULT 'idle',
  claim_token TEXT,
  claimed_by TEXT,
  claimed_at TIMESTAMPTZ,
  claim_expires_at TIMESTAMPTZ,
  last_materialized_fingerprint TEXT,
  last_materialized_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_materialized_at TIMESTAMPTZ,
  last_run_id TEXT REFERENCES core.ranking_runs(run_id) ON DELETE SET NULL,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('idle', 'dirty', 'claimed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_core_ranking_refresh_state_status_updated_at
  ON core.ranking_refresh_state(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_ranking_refresh_state_dirty_window
  ON core.ranking_refresh_state(dirty_start_date, dirty_end_date)
  WHERE dirty_start_date IS NOT NULL AND dirty_end_date IS NOT NULL;

CREATE TABLE IF NOT EXISTS core.canonical_backtest_targets (
  target_id TEXT PRIMARY KEY,
  strategy_name TEXT NOT NULL REFERENCES core.strategies(name) ON DELETE CASCADE,
  start_ts TIMESTAMPTZ NOT NULL,
  end_ts TIMESTAMPTZ NOT NULL,
  bar_size TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_applied_fingerprint TEXT,
  last_dependency_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_enqueued_fingerprint TEXT,
  last_enqueued_at TIMESTAMPTZ,
  last_run_id TEXT REFERENCES core.runs(run_id) ON DELETE SET NULL,
  last_completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (start_ts <= end_ts)
);

CREATE INDEX IF NOT EXISTS idx_core_canonical_backtest_targets_enabled_strategy
  ON core.canonical_backtest_targets(enabled, strategy_name);

ALTER TABLE core.runs
  ADD COLUMN IF NOT EXISTS canonical_target_id TEXT,
  ADD COLUMN IF NOT EXISTS canonical_fingerprint TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_core_runs_canonical_target'
      AND conrelid = 'core.runs'::regclass
  ) THEN
    ALTER TABLE core.runs
      ADD CONSTRAINT fk_core_runs_canonical_target
      FOREIGN KEY (canonical_target_id)
      REFERENCES core.canonical_backtest_targets(target_id)
      ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_core_runs_canonical_target_fingerprint
  ON core.runs(canonical_target_id, canonical_fingerprint, submitted_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE ON TABLE core.ranking_refresh_state TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.canonical_backtest_targets TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.runs TO backtest_service;
  END IF;
END $$;

COMMIT;
