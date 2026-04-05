BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.universe_configs (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.universe_config_revisions (
  universe_name TEXT NOT NULL REFERENCES core.universe_configs(name) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (universe_name, version)
);

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__universe', 128) AS universe_name,
    COALESCE(s.config -> 'universe', NULL) AS universe_config
  FROM core.strategies AS s
  WHERE s.config ? 'universe'
    AND jsonb_typeof(s.config -> 'universe') = 'object'
),
inserted AS (
  INSERT INTO core.universe_configs (name, description, version, config, created_at, updated_at)
  SELECT
    extracted.universe_name,
    'Backfilled from strategy ' || extracted.strategy_name,
    1,
    extracted.universe_config,
    NOW(),
    NOW()
  FROM extracted
  ON CONFLICT (name) DO NOTHING
  RETURNING name
)
INSERT INTO core.universe_config_revisions (universe_name, version, description, config, created_at)
SELECT
  extracted.universe_name,
  1,
  'Backfilled from strategy ' || extracted.strategy_name,
  extracted.universe_config,
  NOW()
FROM extracted
ON CONFLICT (universe_name, version) DO NOTHING;

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__universe', 128) AS universe_name
  FROM core.strategies AS s
  WHERE s.config ? 'universe'
    AND jsonb_typeof(s.config -> 'universe') = 'object'
)
UPDATE core.strategies AS s
SET config = jsonb_set(
  s.config - 'universe',
  '{universeConfigName}',
  to_jsonb(extracted.universe_name),
  true
)
FROM extracted
WHERE s.name = extracted.strategy_name
  AND COALESCE(NULLIF(BTRIM(s.config ->> 'universeConfigName'), ''), '') = '';

CREATE INDEX IF NOT EXISTS idx_core_universe_configs_updated_at
  ON core.universe_configs(updated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.universe_configs TO backtest_service;
    GRANT SELECT ON TABLE core.universe_config_revisions TO backtest_service;
  END IF;
END $$;

COMMIT;
