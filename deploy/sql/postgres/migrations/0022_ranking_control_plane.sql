BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS platinum;

ALTER TABLE core.strategies
  ADD COLUMN IF NOT EXISTS output_table_name TEXT;

WITH normalized AS (
  SELECT
    name,
    COALESCE(
      NULLIF(
        trim(
          BOTH '_' FROM regexp_replace(
            regexp_replace(lower(name), '[^a-z0-9]+', '_', 'g'),
            '_+',
            '_',
            'g'
          )
        ),
        ''
      ),
      'strategy_output'
    ) AS base_slug
  FROM core.strategies
)
UPDATE core.strategies AS s
SET output_table_name = LEFT(
  CASE
    WHEN normalized.base_slug ~ '^[0-9]' THEN 'strategy_' || normalized.base_slug
    ELSE normalized.base_slug
  END,
  63
)
FROM normalized
WHERE s.name = normalized.name
  AND (s.output_table_name IS NULL OR btrim(s.output_table_name) = '');

CREATE UNIQUE INDEX IF NOT EXISTS idx_core_strategies_output_table_name
  ON core.strategies(output_table_name)
  WHERE output_table_name IS NOT NULL;

CREATE TABLE IF NOT EXISTS core.ranking_schemas (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.ranking_schema_revisions (
  schema_name TEXT NOT NULL REFERENCES core.ranking_schemas(name) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (schema_name, version)
);

CREATE TABLE IF NOT EXISTS core.ranking_runs (
  run_id TEXT PRIMARY KEY,
  strategy_name TEXT NOT NULL REFERENCES core.strategies(name) ON DELETE CASCADE,
  ranking_schema_name TEXT NOT NULL REFERENCES core.ranking_schemas(name) ON DELETE RESTRICT,
  ranking_schema_version INTEGER NOT NULL,
  output_table_name TEXT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  status TEXT NOT NULL,
  row_count INTEGER NOT NULL DEFAULT 0,
  date_count INTEGER NOT NULL DEFAULT 0,
  triggered_by TEXT NOT NULL DEFAULT 'manual',
  error TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS core.ranking_watermarks (
  strategy_name TEXT PRIMARY KEY REFERENCES core.strategies(name) ON DELETE CASCADE,
  ranking_schema_name TEXT NOT NULL REFERENCES core.ranking_schemas(name) ON DELETE RESTRICT,
  ranking_schema_version INTEGER NOT NULL,
  output_table_name TEXT NOT NULL,
  last_ranked_date DATE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_ranking_runs_strategy_started_at
  ON core.ranking_runs(strategy_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_ranking_runs_status_started_at
  ON core.ranking_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_ranking_schemas_updated_at
  ON core.ranking_schemas(updated_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.ranking_schemas TO backtest_service;
    GRANT SELECT ON TABLE core.ranking_schema_revisions TO backtest_service;
    GRANT SELECT ON TABLE core.ranking_runs TO backtest_service;
    GRANT SELECT ON TABLE core.ranking_watermarks TO backtest_service;

    GRANT USAGE ON SCHEMA platinum TO backtest_service;
    ALTER DEFAULT PRIVILEGES IN SCHEMA platinum GRANT SELECT ON TABLES TO backtest_service;
  END IF;
END $$;

COMMIT;
