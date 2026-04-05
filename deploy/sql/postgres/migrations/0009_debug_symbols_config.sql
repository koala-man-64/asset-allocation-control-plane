BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.debug_symbols (
  id SMALLINT PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  symbols TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by TEXT
);

INSERT INTO core.debug_symbols(id) VALUES (1) ON CONFLICT DO NOTHING;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.debug_symbols TO backtest_service;
  END IF;
END $$;

COMMIT;
