BEGIN;

ALTER TABLE core.universe_config_revisions
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'published',
  ADD COLUMN IF NOT EXISTS config_hash TEXT,
  ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

UPDATE core.universe_config_revisions
SET
  config_hash = COALESCE(config_hash, md5(config::text)),
  published_at = COALESCE(published_at, created_at)
WHERE config_hash IS NULL OR published_at IS NULL;

ALTER TABLE core.ranking_schema_revisions
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'published',
  ADD COLUMN IF NOT EXISTS config_hash TEXT,
  ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

UPDATE core.ranking_schema_revisions
SET
  config_hash = COALESCE(config_hash, md5(config::text)),
  published_at = COALESCE(published_at, created_at)
WHERE config_hash IS NULL OR published_at IS NULL;

CREATE TABLE IF NOT EXISTS core.strategy_revisions (
  strategy_name TEXT NOT NULL REFERENCES core.strategies(name) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  ranking_schema_name TEXT REFERENCES core.ranking_schemas(name) ON DELETE RESTRICT,
  ranking_schema_version INTEGER,
  universe_name TEXT REFERENCES core.universe_configs(name) ON DELETE RESTRICT,
  universe_version INTEGER,
  status TEXT NOT NULL DEFAULT 'published',
  config_hash TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (strategy_name, version)
);

CREATE INDEX IF NOT EXISTS idx_core_strategy_revisions_published_at
  ON core.strategy_revisions(published_at DESC);

WITH backfill AS (
  SELECT
    s.name AS strategy_name,
    1 AS version,
    COALESCE(s.description, '') AS description,
    COALESCE(s.config, '{}'::jsonb) AS config,
    NULLIF(BTRIM(COALESCE(s.config ->> 'rankingSchemaName', '')), '') AS ranking_schema_name,
    rs.version AS ranking_schema_version,
    COALESCE(
      NULLIF(BTRIM(COALESCE(s.config ->> 'universeConfigName', '')), ''),
      NULLIF(BTRIM(COALESCE(rs.config ->> 'universeConfigName', '')), '')
    ) AS universe_name,
    uc.version AS universe_version,
    md5(COALESCE(s.config, '{}'::jsonb)::text) AS config_hash,
    COALESCE(s.updated_at, NOW()) AS published_at,
    COALESCE(s.updated_at, NOW()) AS created_at
  FROM core.strategies AS s
  LEFT JOIN core.ranking_schemas AS rs
    ON rs.name = NULLIF(BTRIM(COALESCE(s.config ->> 'rankingSchemaName', '')), '')
  LEFT JOIN core.universe_configs AS uc
    ON uc.name = COALESCE(
      NULLIF(BTRIM(COALESCE(s.config ->> 'universeConfigName', '')), ''),
      NULLIF(BTRIM(COALESCE(rs.config ->> 'universeConfigName', '')), '')
    )
)
INSERT INTO core.strategy_revisions (
  strategy_name,
  version,
  description,
  config,
  ranking_schema_name,
  ranking_schema_version,
  universe_name,
  universe_version,
  status,
  config_hash,
  published_at,
  created_at
)
SELECT
  strategy_name,
  version,
  description,
  config,
  ranking_schema_name,
  ranking_schema_version,
  universe_name,
  universe_version,
  'published',
  config_hash,
  published_at,
  created_at
FROM backfill
ON CONFLICT (strategy_name, version) DO NOTHING;

ALTER TABLE core.runs
  ADD COLUMN IF NOT EXISTS strategy_name TEXT,
  ADD COLUMN IF NOT EXISTS strategy_version INTEGER,
  ADD COLUMN IF NOT EXISTS ranking_schema_name TEXT,
  ADD COLUMN IF NOT EXISTS ranking_schema_version INTEGER,
  ADD COLUMN IF NOT EXISTS universe_name TEXT,
  ADD COLUMN IF NOT EXISTS universe_version INTEGER,
  ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS bar_size TEXT,
  ADD COLUMN IF NOT EXISTS execution_name TEXT,
  ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS summary_json JSONB,
  ADD COLUMN IF NOT EXISTS artifact_manifest_path TEXT,
  ADD COLUMN IF NOT EXISTS submitted_by TEXT;

UPDATE core.runs
SET
  start_ts = COALESCE(
    start_ts,
    CASE
      WHEN start_date IS NULL OR BTRIM(start_date) = '' THEN NULL
      ELSE (start_date::date)::timestamptz
    END
  ),
  end_ts = COALESCE(
    end_ts,
    CASE
      WHEN end_date IS NULL OR BTRIM(end_date) = '' THEN NULL
      ELSE (end_date::date)::timestamptz
    END
  )
WHERE start_ts IS NULL OR end_ts IS NULL;

CREATE INDEX IF NOT EXISTS idx_core_runs_strategy_submitted_at
  ON core.runs(strategy_name, submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_runs_status_heartbeat_at
  ON core.runs(status, heartbeat_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.strategy_revisions TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.runs TO backtest_service;
  END IF;
END $$;

COMMIT;
