BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.purge_rules (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  layer TEXT NOT NULL,
  domain TEXT NOT NULL,
  column_name TEXT NOT NULL,
  operator TEXT NOT NULL,
  threshold DOUBLE PRECISION NOT NULL,
  run_interval_minutes INTEGER NOT NULL CHECK (run_interval_minutes > 0),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  next_run_at TIMESTAMPTZ,
  last_run_at TIMESTAMPTZ,
  last_status TEXT,
  last_error TEXT,
  last_match_count INTEGER,
  last_purge_count INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT,
  updated_by TEXT
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'core'
      AND table_name = 'purge_rules'
      AND column_name = 'enabled'
  ) THEN
    CREATE INDEX IF NOT EXISTS idx_purge_rules_enabled_next_run
      ON core.purge_rules (enabled, next_run_at);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_purge_rules_layer_domain
  ON core.purge_rules (layer, domain);
CREATE INDEX IF NOT EXISTS idx_purge_rules_next_run
  ON core.purge_rules (next_run_at);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.purge_rules TO backtest_service;
  END IF;
END $$;

COMMIT;
