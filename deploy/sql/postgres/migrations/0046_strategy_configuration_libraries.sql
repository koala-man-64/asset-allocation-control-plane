BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.regime_policy_configs (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  config JSONB NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.regime_policy_config_revisions (
  policy_name TEXT NOT NULL REFERENCES core.regime_policy_configs(name) ON DELETE CASCADE,
  version INTEGER NOT NULL CHECK (version >= 1),
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'published',
  config_hash TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (policy_name, version)
);

CREATE TABLE IF NOT EXISTS core.risk_policy_configs (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  config JSONB NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.risk_policy_config_revisions (
  policy_name TEXT NOT NULL REFERENCES core.risk_policy_configs(name) ON DELETE CASCADE,
  version INTEGER NOT NULL CHECK (version >= 1),
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'published',
  config_hash TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (policy_name, version)
);

CREATE TABLE IF NOT EXISTS core.exit_rule_sets (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
  config JSONB NOT NULL,
  archived BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.exit_rule_set_revisions (
  rule_set_name TEXT NOT NULL REFERENCES core.exit_rule_sets(name) ON DELETE CASCADE,
  version INTEGER NOT NULL CHECK (version >= 1),
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'published',
  config_hash TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (rule_set_name, version)
);

ALTER TABLE core.strategy_revisions
  ADD COLUMN IF NOT EXISTS regime_policy_name TEXT,
  ADD COLUMN IF NOT EXISTS regime_policy_version INTEGER,
  ADD COLUMN IF NOT EXISTS risk_policy_name TEXT,
  ADD COLUMN IF NOT EXISTS risk_policy_version INTEGER,
  ADD COLUMN IF NOT EXISTS exit_rule_set_name TEXT,
  ADD COLUMN IF NOT EXISTS exit_rule_set_version INTEGER;

CREATE INDEX IF NOT EXISTS idx_core_regime_policy_configs_updated_at
  ON core.regime_policy_configs(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_risk_policy_configs_updated_at
  ON core.risk_policy_configs(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_exit_rule_sets_updated_at
  ON core.exit_rule_sets(updated_at DESC);

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__regime_policy', 128) AS policy_name,
    s.config -> 'regimePolicy' AS policy_config
  FROM core.strategies AS s
  WHERE s.config ? 'regimePolicy'
    AND jsonb_typeof(s.config -> 'regimePolicy') = 'object'
    AND COALESCE(NULLIF(BTRIM(s.config ->> 'regimePolicyConfigName'), ''), '') = ''
)
INSERT INTO core.regime_policy_configs (name, description, version, config, created_at, updated_at)
SELECT
  policy_name,
  'Backfilled from strategy ' || strategy_name,
  1,
  policy_config,
  NOW(),
  NOW()
FROM extracted
ON CONFLICT (name) DO NOTHING;

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__regime_policy', 128) AS policy_name,
    s.config -> 'regimePolicy' AS policy_config
  FROM core.strategies AS s
  WHERE s.config ? 'regimePolicy'
    AND jsonb_typeof(s.config -> 'regimePolicy') = 'object'
    AND COALESCE(NULLIF(BTRIM(s.config ->> 'regimePolicyConfigName'), ''), '') = ''
)
INSERT INTO core.regime_policy_config_revisions (
  policy_name,
  version,
  description,
  config,
  status,
  config_hash,
  published_at,
  created_at
)
SELECT
  policy_name,
  1,
  'Backfilled from strategy ' || strategy_name,
  policy_config,
  'published',
  md5(policy_config::text),
  NOW(),
  NOW()
FROM extracted
ON CONFLICT (policy_name, version) DO NOTHING;

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__risk_policy', 128) AS policy_name,
    jsonb_build_object('policy', COALESCE(s.config -> 'strategyRiskPolicy', s.config -> 'riskPolicy')) AS policy_config
  FROM core.strategies AS s
  WHERE (
    (
      s.config ? 'strategyRiskPolicy'
      AND jsonb_typeof(s.config -> 'strategyRiskPolicy') = 'object'
    )
    OR (
      s.config ? 'riskPolicy'
      AND jsonb_typeof(s.config -> 'riskPolicy') = 'object'
    )
  )
  AND COALESCE(NULLIF(BTRIM(s.config ->> 'riskPolicyName'), ''), '') = ''
)
INSERT INTO core.risk_policy_configs (name, description, version, config, created_at, updated_at)
SELECT
  policy_name,
  'Backfilled from strategy ' || strategy_name,
  1,
  policy_config,
  NOW(),
  NOW()
FROM extracted
ON CONFLICT (name) DO NOTHING;

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__risk_policy', 128) AS policy_name,
    jsonb_build_object('policy', COALESCE(s.config -> 'strategyRiskPolicy', s.config -> 'riskPolicy')) AS policy_config
  FROM core.strategies AS s
  WHERE (
    (
      s.config ? 'strategyRiskPolicy'
      AND jsonb_typeof(s.config -> 'strategyRiskPolicy') = 'object'
    )
    OR (
      s.config ? 'riskPolicy'
      AND jsonb_typeof(s.config -> 'riskPolicy') = 'object'
    )
  )
  AND COALESCE(NULLIF(BTRIM(s.config ->> 'riskPolicyName'), ''), '') = ''
)
INSERT INTO core.risk_policy_config_revisions (
  policy_name,
  version,
  description,
  config,
  status,
  config_hash,
  published_at,
  created_at
)
SELECT
  policy_name,
  1,
  'Backfilled from strategy ' || strategy_name,
  policy_config,
  'published',
  md5(policy_config::text),
  NOW(),
  NOW()
FROM extracted
ON CONFLICT (policy_name, version) DO NOTHING;

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__exit_rules', 128) AS rule_set_name,
    jsonb_build_object(
      'intrabarConflictPolicy',
      COALESCE(NULLIF(s.config ->> 'intrabarConflictPolicy', ''), 'stop_first'),
      'exits',
      COALESCE(s.config -> 'exits', '[]'::jsonb)
    ) AS rule_set_config
  FROM core.strategies AS s
  WHERE s.config ? 'exits'
    AND jsonb_typeof(s.config -> 'exits') = 'array'
    AND jsonb_array_length(s.config -> 'exits') > 0
    AND COALESCE(NULLIF(BTRIM(s.config ->> 'exitRuleSetName'), ''), '') = ''
)
INSERT INTO core.exit_rule_sets (name, description, version, config, created_at, updated_at)
SELECT
  rule_set_name,
  'Backfilled from strategy ' || strategy_name,
  1,
  rule_set_config,
  NOW(),
  NOW()
FROM extracted
ON CONFLICT (name) DO NOTHING;

WITH extracted AS (
  SELECT
    s.name AS strategy_name,
    LEFT(s.name || '__exit_rules', 128) AS rule_set_name,
    jsonb_build_object(
      'intrabarConflictPolicy',
      COALESCE(NULLIF(s.config ->> 'intrabarConflictPolicy', ''), 'stop_first'),
      'exits',
      COALESCE(s.config -> 'exits', '[]'::jsonb)
    ) AS rule_set_config
  FROM core.strategies AS s
  WHERE s.config ? 'exits'
    AND jsonb_typeof(s.config -> 'exits') = 'array'
    AND jsonb_array_length(s.config -> 'exits') > 0
    AND COALESCE(NULLIF(BTRIM(s.config ->> 'exitRuleSetName'), ''), '') = ''
)
INSERT INTO core.exit_rule_set_revisions (
  rule_set_name,
  version,
  description,
  config,
  status,
  config_hash,
  published_at,
  created_at
)
SELECT
  rule_set_name,
  1,
  'Backfilled from strategy ' || strategy_name,
  rule_set_config,
  'published',
  md5(rule_set_config::text),
  NOW(),
  NOW()
FROM extracted
ON CONFLICT (rule_set_name, version) DO NOTHING;

UPDATE core.strategies AS s
SET config = jsonb_set(
  jsonb_set(s.config, '{regimePolicyConfigName}', to_jsonb(LEFT(s.name || '__regime_policy', 128)), TRUE),
  '{regimePolicyConfigVersion}',
  '1'::jsonb,
  TRUE
)
WHERE s.config ? 'regimePolicy'
  AND jsonb_typeof(s.config -> 'regimePolicy') = 'object'
  AND COALESCE(NULLIF(BTRIM(s.config ->> 'regimePolicyConfigName'), ''), '') = '';

UPDATE core.strategies AS s
SET config = jsonb_set(
  jsonb_set(s.config, '{riskPolicyName}', to_jsonb(LEFT(s.name || '__risk_policy', 128)), TRUE),
  '{riskPolicyVersion}',
  '1'::jsonb,
  TRUE
)
WHERE (
    (
    s.config ? 'strategyRiskPolicy'
    AND jsonb_typeof(s.config -> 'strategyRiskPolicy') = 'object'
  )
  OR (
    s.config ? 'riskPolicy'
    AND jsonb_typeof(s.config -> 'riskPolicy') = 'object'
  )
)
AND COALESCE(NULLIF(BTRIM(s.config ->> 'riskPolicyName'), ''), '') = '';

UPDATE core.strategies AS s
SET config = jsonb_set(
  jsonb_set(s.config, '{exitRuleSetName}', to_jsonb(LEFT(s.name || '__exit_rules', 128)), TRUE),
  '{exitRuleSetVersion}',
  '1'::jsonb,
  TRUE
)
WHERE s.config ? 'exits'
  AND jsonb_typeof(s.config -> 'exits') = 'array'
  AND jsonb_array_length(s.config -> 'exits') > 0
  AND COALESCE(NULLIF(BTRIM(s.config ->> 'exitRuleSetName'), ''), '') = '';

UPDATE core.strategy_revisions AS r
SET
  regime_policy_name = NULLIF(BTRIM(r.config ->> 'regimePolicyConfigName'), ''),
  regime_policy_version = NULLIF(BTRIM(r.config ->> 'regimePolicyConfigVersion'), '')::INTEGER,
  risk_policy_name = NULLIF(BTRIM(r.config ->> 'riskPolicyName'), ''),
  risk_policy_version = NULLIF(BTRIM(r.config ->> 'riskPolicyVersion'), '')::INTEGER,
  exit_rule_set_name = NULLIF(BTRIM(r.config ->> 'exitRuleSetName'), ''),
  exit_rule_set_version = NULLIF(BTRIM(r.config ->> 'exitRuleSetVersion'), '')::INTEGER
WHERE r.config IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.regime_policy_configs TO backtest_service;
    GRANT SELECT ON TABLE core.regime_policy_config_revisions TO backtest_service;
    GRANT SELECT ON TABLE core.risk_policy_configs TO backtest_service;
    GRANT SELECT ON TABLE core.risk_policy_config_revisions TO backtest_service;
    GRANT SELECT ON TABLE core.exit_rule_sets TO backtest_service;
    GRANT SELECT ON TABLE core.exit_rule_set_revisions TO backtest_service;
  END IF;
END $$;

COMMIT;
