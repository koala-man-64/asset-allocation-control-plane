BEGIN;

ALTER TABLE core.runs
  ADD COLUMN IF NOT EXISTS config_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS request_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS idx_core_runs_request_fingerprint_submitted_at
  ON core.runs(request_fingerprint, submitted_at DESC)
  WHERE request_fingerprint IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_core_runs_config_fingerprint_submitted_at
  ON core.runs(config_fingerprint, submitted_at DESC)
  WHERE config_fingerprint IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE ON TABLE core.runs TO backtest_service;
  END IF;
END $$;

COMMIT;
