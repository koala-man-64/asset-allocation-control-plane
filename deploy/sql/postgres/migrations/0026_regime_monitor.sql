BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS core.regime_models (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.regime_model_revisions (
  model_name TEXT NOT NULL REFERENCES core.regime_models(name) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  config JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'published',
  config_hash TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (model_name, version)
);

CREATE TABLE IF NOT EXISTS core.regime_model_activations (
  activation_id BIGSERIAL PRIMARY KEY,
  model_name TEXT NOT NULL REFERENCES core.regime_models(name) ON DELETE CASCADE,
  model_version INTEGER NOT NULL,
  activated_by TEXT,
  activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_core_regime_models_updated_at
  ON core.regime_models(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_regime_model_activations_model_name_activated_at
  ON core.regime_model_activations(model_name, activated_at DESC);

CREATE TABLE IF NOT EXISTS gold.regime_inputs_daily (
  as_of_date DATE PRIMARY KEY,
  spy_close DOUBLE PRECISION,
  return_1d DOUBLE PRECISION,
  return_20d DOUBLE PRECISION,
  rvol_10d_ann DOUBLE PRECISION,
  vix_spot_close DOUBLE PRECISION,
  vix3m_close DOUBLE PRECISION,
  vix_slope DOUBLE PRECISION,
  trend_state TEXT,
  curve_state TEXT,
  vix_gt_32_streak INTEGER,
  inputs_complete_flag BOOLEAN NOT NULL DEFAULT FALSE,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.regime_history (
  as_of_date DATE NOT NULL,
  effective_from_date DATE NOT NULL,
  model_name TEXT NOT NULL,
  model_version INTEGER NOT NULL,
  regime_code TEXT NOT NULL,
  regime_status TEXT NOT NULL,
  matched_rule_id TEXT,
  halt_flag BOOLEAN NOT NULL DEFAULT FALSE,
  halt_reason TEXT,
  spy_return_20d DOUBLE PRECISION,
  rvol_10d_ann DOUBLE PRECISION,
  vix_spot_close DOUBLE PRECISION,
  vix3m_close DOUBLE PRECISION,
  vix_slope DOUBLE PRECISION,
  trend_state TEXT,
  curve_state TEXT,
  vix_gt_32_streak INTEGER,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (as_of_date, model_name, model_version)
);

CREATE TABLE IF NOT EXISTS gold.regime_latest (
  model_name TEXT NOT NULL,
  model_version INTEGER NOT NULL,
  as_of_date DATE NOT NULL,
  effective_from_date DATE NOT NULL,
  regime_code TEXT NOT NULL,
  regime_status TEXT NOT NULL,
  matched_rule_id TEXT,
  halt_flag BOOLEAN NOT NULL DEFAULT FALSE,
  halt_reason TEXT,
  spy_return_20d DOUBLE PRECISION,
  rvol_10d_ann DOUBLE PRECISION,
  vix_spot_close DOUBLE PRECISION,
  vix3m_close DOUBLE PRECISION,
  vix_slope DOUBLE PRECISION,
  trend_state TEXT,
  curve_state TEXT,
  vix_gt_32_streak INTEGER,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (model_name, model_version)
);

CREATE TABLE IF NOT EXISTS gold.regime_transitions (
  model_name TEXT NOT NULL,
  model_version INTEGER NOT NULL,
  effective_from_date DATE NOT NULL,
  prior_regime_code TEXT,
  new_regime_code TEXT NOT NULL,
  trigger_rule_id TEXT,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (model_name, model_version, effective_from_date)
);

CREATE INDEX IF NOT EXISTS idx_gold_regime_history_model_effective
  ON gold.regime_history(model_name, model_version, effective_from_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_regime_history_model_as_of
  ON gold.regime_history(model_name, model_version, as_of_date DESC);

ALTER TABLE core.runs
  ADD COLUMN IF NOT EXISTS regime_model_name TEXT,
  ADD COLUMN IF NOT EXISTS regime_model_version INTEGER;

INSERT INTO core.regime_models (name, description, version, config, created_at, updated_at)
VALUES (
  'default-regime',
  'Default gold-only regime monitor model',
  1,
  '{
    "trendPositiveThreshold": 0.02,
    "trendNegativeThreshold": -0.02,
    "curveContangoThreshold": 0.5,
    "curveInvertedThreshold": -0.5,
    "highVolEnterThreshold": 28.0,
    "highVolExitThreshold": 25.0,
    "bearVolMin": 15.0,
    "bearVolMaxExclusive": 25.0,
    "bullVolMaxExclusive": 15.0,
    "choppyVolMin": 10.0,
    "choppyVolMaxExclusive": 18.0,
    "haltVixThreshold": 32.0,
    "haltVixStreakDays": 2,
    "precedence": [
      "high_vol",
      "trending_bear",
      "trending_bull",
      "choppy_mean_reversion",
      "unclassified"
    ]
  }'::jsonb,
  NOW(),
  NOW()
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO core.regime_model_revisions (
  model_name,
  version,
  description,
  config,
  status,
  config_hash,
  published_at,
  created_at
)
SELECT
  'default-regime',
  1,
  'Default gold-only regime monitor model',
  '{
    "trendPositiveThreshold": 0.02,
    "trendNegativeThreshold": -0.02,
    "curveContangoThreshold": 0.5,
    "curveInvertedThreshold": -0.5,
    "highVolEnterThreshold": 28.0,
    "highVolExitThreshold": 25.0,
    "bearVolMin": 15.0,
    "bearVolMaxExclusive": 25.0,
    "bullVolMaxExclusive": 15.0,
    "choppyVolMin": 10.0,
    "choppyVolMaxExclusive": 18.0,
    "haltVixThreshold": 32.0,
    "haltVixStreakDays": 2,
    "precedence": [
      "high_vol",
      "trending_bear",
      "trending_bull",
      "choppy_mean_reversion",
      "unclassified"
    ]
  }'::jsonb,
  'published',
  md5('{
    "trendPositiveThreshold":0.02,
    "trendNegativeThreshold":-0.02,
    "curveContangoThreshold":0.5,
    "curveInvertedThreshold":-0.5,
    "highVolEnterThreshold":28.0,
    "highVolExitThreshold":25.0,
    "bearVolMin":15.0,
    "bearVolMaxExclusive":25.0,
    "bullVolMaxExclusive":15.0,
    "choppyVolMin":10.0,
    "choppyVolMaxExclusive":18.0,
    "haltVixThreshold":32.0,
    "haltVixStreakDays":2,
    "precedence":["high_vol","trending_bear","trending_bull","choppy_mean_reversion","unclassified"]
  }'),
  NOW(),
  NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM core.regime_model_revisions
  WHERE model_name = 'default-regime' AND version = 1
);

INSERT INTO core.regime_model_activations (model_name, model_version, activated_by, activated_at)
SELECT 'default-regime', 1, 'migration-0026', NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM core.regime_model_activations
  WHERE model_name = 'default-regime'
);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.regime_models TO backtest_service;
    GRANT SELECT ON TABLE core.regime_model_revisions TO backtest_service;
    GRANT SELECT ON TABLE core.regime_model_activations TO backtest_service;

    GRANT USAGE ON SCHEMA gold TO backtest_service;
    GRANT SELECT ON TABLE gold.regime_inputs_daily TO backtest_service;
    GRANT SELECT ON TABLE gold.regime_history TO backtest_service;
    GRANT SELECT ON TABLE gold.regime_latest TO backtest_service;
    GRANT SELECT ON TABLE gold.regime_transitions TO backtest_service;
  END IF;
END $$;

COMMIT;
