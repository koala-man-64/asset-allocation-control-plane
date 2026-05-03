BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.rebalance_policy_configs (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  config JSONB NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL DEFAULT 'active',
  intended_use TEXT NOT NULL DEFAULT 'research',
  thesis TEXT NOT NULL DEFAULT '',
  what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.rebalance_policy_config_revisions (
  policy_name TEXT NOT NULL REFERENCES core.rebalance_policy_configs(name) ON DELETE CASCADE,
  version INTEGER NOT NULL CHECK (version >= 1),
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  intended_use TEXT NOT NULL DEFAULT 'research',
  thesis TEXT NOT NULL DEFAULT '',
  what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb,
  config_hash TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (policy_name, version)
);

ALTER TABLE core.regime_policy_configs
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS intended_use TEXT NOT NULL DEFAULT 'research',
  ADD COLUMN IF NOT EXISTS thesis TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE core.risk_policy_configs
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS intended_use TEXT NOT NULL DEFAULT 'research',
  ADD COLUMN IF NOT EXISTS thesis TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE core.exit_rule_sets
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS intended_use TEXT NOT NULL DEFAULT 'research',
  ADD COLUMN IF NOT EXISTS thesis TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE core.regime_policy_config_revisions
  ALTER COLUMN status SET DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS intended_use TEXT NOT NULL DEFAULT 'research',
  ADD COLUMN IF NOT EXISTS thesis TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE core.risk_policy_config_revisions
  ALTER COLUMN status SET DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS intended_use TEXT NOT NULL DEFAULT 'research',
  ADD COLUMN IF NOT EXISTS thesis TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE core.exit_rule_set_revisions
  ALTER COLUMN status SET DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS intended_use TEXT NOT NULL DEFAULT 'research',
  ADD COLUMN IF NOT EXISTS thesis TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS what_to_monitor JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE core.regime_policy_config_revisions SET status = 'active' WHERE status = 'published';
UPDATE core.risk_policy_config_revisions SET status = 'active' WHERE status = 'published';
UPDATE core.exit_rule_set_revisions SET status = 'active' WHERE status = 'published';

ALTER TABLE core.strategy_revisions
  ADD COLUMN IF NOT EXISTS rebalance_policy_name TEXT,
  ADD COLUMN IF NOT EXISTS rebalance_policy_version INTEGER;

CREATE INDEX IF NOT EXISTS idx_core_rebalance_policy_configs_updated_at
  ON core.rebalance_policy_configs(updated_at DESC);

UPDATE core.strategy_revisions AS r
SET
  rebalance_policy_name = NULLIF(BTRIM(r.config #>> '{componentRefs,rebalance,name}'), ''),
  rebalance_policy_version = NULLIF(BTRIM(r.config #>> '{componentRefs,rebalance,version}'), '')::INTEGER
WHERE r.config IS NOT NULL
  AND NULLIF(BTRIM(r.config #>> '{componentRefs,rebalance,name}'), '') IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.rebalance_policy_configs TO backtest_service;
    GRANT SELECT ON TABLE core.rebalance_policy_config_revisions TO backtest_service;
  END IF;
END $$;

COMMIT;
