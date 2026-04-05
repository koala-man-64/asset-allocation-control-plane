BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.runtime_config (
  scope TEXT NOT NULL DEFAULT 'global',
  key TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  value TEXT NOT NULL DEFAULT '',
  description TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by TEXT,
  PRIMARY KEY (scope, key)
);

CREATE INDEX IF NOT EXISTS idx_runtime_config_key ON core.runtime_config (key);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.runtime_config TO backtest_service;
  END IF;
END $$;

COMMIT;
