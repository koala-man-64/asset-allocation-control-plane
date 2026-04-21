BEGIN;

CREATE TABLE IF NOT EXISTS gold.regime_macro_inputs_daily (
  as_of_date DATE PRIMARY KEY,
  rate_2y DOUBLE PRECISION,
  rate_10y DOUBLE PRECISION,
  curve_2s10s DOUBLE PRECISION,
  hy_oas DOUBLE PRECISION,
  hy_oas_z_20d DOUBLE PRECISION,
  rates_event_flag BOOLEAN,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE gold.regime_inputs_daily
  ADD COLUMN IF NOT EXISTS qqq_close DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS iwm_close DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS acwi_close DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS spy_return_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS qqq_return_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS iwm_return_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS acwi_return_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS sma_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS sma_50d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS sma_200d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS rsi_14d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS bb_width_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_14d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_14d_pct_of_close DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gap_atr DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS volume_pct_rank_252d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS rate_2y DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS rate_10y DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS curve_2s10s DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS hy_oas DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS hy_oas_z_20d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS rates_event_flag BOOLEAN;

ALTER TABLE gold.regime_history
  ADD COLUMN IF NOT EXISTS display_name TEXT,
  ADD COLUMN IF NOT EXISTS signal_state TEXT,
  ADD COLUMN IF NOT EXISTS score DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS activation_threshold DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN,
  ADD COLUMN IF NOT EXISTS evidence_json JSONB;

UPDATE gold.regime_history
SET
  display_name = COALESCE(display_name, INITCAP(REPLACE(regime_code, '_', ' '))),
  signal_state = COALESCE(signal_state, 'active'),
  score = COALESCE(score, 1.0),
  activation_threshold = COALESCE(activation_threshold, 0.6),
  is_active = COALESCE(is_active, TRUE),
  evidence_json = COALESCE(evidence_json, '{}'::jsonb)
WHERE regime_code IS NOT NULL;

ALTER TABLE gold.regime_history
  DROP CONSTRAINT IF EXISTS regime_history_pkey;

ALTER TABLE gold.regime_history
  DROP COLUMN IF EXISTS regime_status,
  DROP COLUMN IF EXISTS spy_return_20d,
  DROP COLUMN IF EXISTS rvol_10d_ann,
  DROP COLUMN IF EXISTS vix_spot_close,
  DROP COLUMN IF EXISTS vix3m_close,
  DROP COLUMN IF EXISTS vix_slope,
  DROP COLUMN IF EXISTS trend_state,
  DROP COLUMN IF EXISTS curve_state,
  DROP COLUMN IF EXISTS vix_gt_32_streak;

ALTER TABLE gold.regime_history
  ADD PRIMARY KEY (as_of_date, model_name, model_version, regime_code);

ALTER TABLE gold.regime_latest
  ADD COLUMN IF NOT EXISTS display_name TEXT,
  ADD COLUMN IF NOT EXISTS signal_state TEXT,
  ADD COLUMN IF NOT EXISTS score DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS activation_threshold DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN,
  ADD COLUMN IF NOT EXISTS evidence_json JSONB;

UPDATE gold.regime_latest
SET
  display_name = COALESCE(display_name, INITCAP(REPLACE(regime_code, '_', ' '))),
  signal_state = COALESCE(signal_state, 'active'),
  score = COALESCE(score, 1.0),
  activation_threshold = COALESCE(activation_threshold, 0.6),
  is_active = COALESCE(is_active, TRUE),
  evidence_json = COALESCE(evidence_json, '{}'::jsonb)
WHERE regime_code IS NOT NULL;

ALTER TABLE gold.regime_latest
  DROP CONSTRAINT IF EXISTS regime_latest_pkey;

ALTER TABLE gold.regime_latest
  DROP COLUMN IF EXISTS regime_status,
  DROP COLUMN IF EXISTS spy_return_20d,
  DROP COLUMN IF EXISTS rvol_10d_ann,
  DROP COLUMN IF EXISTS vix_spot_close,
  DROP COLUMN IF EXISTS vix3m_close,
  DROP COLUMN IF EXISTS vix_slope,
  DROP COLUMN IF EXISTS trend_state,
  DROP COLUMN IF EXISTS curve_state,
  DROP COLUMN IF EXISTS vix_gt_32_streak;

ALTER TABLE gold.regime_latest
  ADD PRIMARY KEY (model_name, model_version, regime_code);

ALTER TABLE gold.regime_transitions
  ADD COLUMN IF NOT EXISTS regime_code TEXT,
  ADD COLUMN IF NOT EXISTS transition_type TEXT,
  ADD COLUMN IF NOT EXISTS prior_score DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS new_score DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS activation_threshold DOUBLE PRECISION;

UPDATE gold.regime_transitions
SET
  regime_code = COALESCE(regime_code, new_regime_code),
  transition_type = COALESCE(transition_type, 'entered'),
  new_score = COALESCE(new_score, 1.0),
  activation_threshold = COALESCE(activation_threshold, 0.6)
WHERE new_regime_code IS NOT NULL OR regime_code IS NOT NULL;

ALTER TABLE gold.regime_transitions
  DROP CONSTRAINT IF EXISTS regime_transitions_pkey;

ALTER TABLE gold.regime_transitions
  DROP COLUMN IF EXISTS prior_regime_code,
  DROP COLUMN IF EXISTS new_regime_code;

ALTER TABLE gold.regime_transitions
  ADD PRIMARY KEY (model_name, model_version, effective_from_date, regime_code, transition_type);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'core'
      AND table_name = 'backtest_regime_trace'
      AND column_name = 'regime_code'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'core'
      AND table_name = 'backtest_regime_trace'
      AND column_name = 'primary_regime_code'
  ) THEN
    EXECUTE 'ALTER TABLE core.backtest_regime_trace RENAME COLUMN regime_code TO primary_regime_code';
  END IF;
END $$;

ALTER TABLE core.backtest_regime_trace
  ADD COLUMN IF NOT EXISTS active_regimes_json JSONB,
  ADD COLUMN IF NOT EXISTS signals_json JSONB;

UPDATE core.backtest_regime_trace
SET
  active_regimes_json = COALESCE(
    active_regimes_json,
    CASE
      WHEN primary_regime_code IS NULL THEN '[]'::jsonb
      ELSE jsonb_build_array(primary_regime_code)
    END
  ),
  signals_json = COALESCE(signals_json, '[]'::jsonb);

ALTER TABLE core.backtest_regime_trace
  DROP COLUMN IF EXISTS regime_status,
  DROP COLUMN IF EXISTS matched_rule_id,
  DROP COLUMN IF EXISTS blocked,
  DROP COLUMN IF EXISTS blocked_reason,
  DROP COLUMN IF EXISTS blocked_action,
  DROP COLUMN IF EXISTS exposure_multiplier;

CREATE OR REPLACE FUNCTION core.normalize_default_regime_policy_v3(payload JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  normalized JSONB := COALESCE(payload, '{}'::jsonb);
  regime_policy JSONB;
BEGIN
  IF jsonb_typeof(normalized) <> 'object' THEN
    RETURN normalized;
  END IF;

  regime_policy := normalized -> 'regimePolicy';
  IF jsonb_typeof(regime_policy) <> 'object' THEN
    RETURN normalized;
  END IF;

  IF COALESCE(NULLIF(BTRIM(regime_policy ->> 'modelName'), ''), '') <> 'default-regime' THEN
    RETURN normalized;
  END IF;

  RETURN jsonb_set(
    normalized,
    '{regimePolicy}',
    jsonb_build_object(
      'modelName', 'default-regime',
      'mode', 'observe_only'
    ),
    TRUE
  );
END;
$$;

UPDATE core.strategies
SET config = core.normalize_default_regime_policy_v3(config)
WHERE config IS NOT NULL;

UPDATE core.strategy_revisions
SET config = core.normalize_default_regime_policy_v3(config)
WHERE config IS NOT NULL;

DROP FUNCTION core.normalize_default_regime_policy_v3(JSONB);

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
VALUES (
  'default-regime',
  3,
  'Default multi-label observational regime monitor model',
  '{
    "activationThreshold": 0.6,
    "signalConfigs": {
      "trending_up": {
        "displayName": "Trending (Up)",
        "requiredMetrics": ["spy_above_sma_200", "qqq_above_sma_200", "spy_return_20d"],
        "rules": [
          {"metric": "spy_above_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Broad U.S. equity index is above its 200-day average."},
          {"metric": "qqq_above_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Nasdaq-100 proxy is above its 200-day average."},
          {"metric": "spy_return_20d", "comparison": "gte", "lower": 0.02, "upper": null, "description": "Broad market 20-day return is at least +2%."}
        ]
      },
      "trending_down": {
        "displayName": "Trending (Down)",
        "requiredMetrics": ["spy_below_sma_200", "qqq_below_sma_200", "spy_return_20d"],
        "rules": [
          {"metric": "spy_below_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Broad U.S. equity index is below its 200-day average."},
          {"metric": "qqq_below_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Nasdaq-100 proxy is below its 200-day average."},
          {"metric": "spy_return_20d", "comparison": "lte", "lower": -0.02, "upper": null, "description": "Broad market 20-day return is at most -2%."}
        ]
      },
      "mean_reverting": {
        "displayName": "Mean-Reverting",
        "requiredMetrics": ["rsi_14d", "bb_width_20d", "abs_spy_return_20d"],
        "rules": [
          {"metric": "rsi_14d", "comparison": "between", "lower": 30.0, "upper": 70.0, "description": "RSI is in the neutral mean-reversion corridor."},
          {"metric": "bb_width_20d", "comparison": "lte", "lower": 0.18, "upper": null, "description": "Bollinger band width remains compressed."},
          {"metric": "abs_spy_return_20d", "comparison": "lte", "lower": 0.03, "upper": null, "description": "The broad market is not in a strong directional burst."}
        ]
      },
      "low_volatility": {
        "displayName": "Low Volatility",
        "requiredMetrics": ["vix_spot_close", "atr_14d_pct_of_close", "bb_width_20d"],
        "rules": [
          {"metric": "vix_spot_close", "comparison": "lte", "lower": 15.0, "upper": null, "description": "VIX is in the low-volatility zone."},
          {"metric": "atr_14d_pct_of_close", "comparison": "lte", "lower": 0.03, "upper": null, "description": "ATR is narrow relative to price."},
          {"metric": "bb_width_20d", "comparison": "lte", "lower": 0.12, "upper": null, "description": "Bollinger bands remain narrow."}
        ]
      },
      "high_volatility": {
        "displayName": "High Volatility",
        "requiredMetrics": ["vix_spot_close", "atr_14d_pct_of_close", "gap_atr"],
        "rules": [
          {"metric": "vix_spot_close", "comparison": "gte", "lower": 25.0, "upper": null, "description": "VIX is elevated relative to normal conditions."},
          {"metric": "atr_14d_pct_of_close", "comparison": "gte", "lower": 0.04, "upper": null, "description": "ATR has expanded materially relative to price."},
          {"metric": "gap_atr", "comparison": "gte", "lower": 0.5, "upper": null, "description": "Overnight or opening gaps are large versus ATR."}
        ]
      },
      "liquidity_stress": {
        "displayName": "Liquidity Regime",
        "requiredMetrics": ["volume_pct_rank_252d", "gap_atr", "hy_oas_z_20d"],
        "rules": [
          {"metric": "volume_pct_rank_252d", "comparison": "lte", "lower": 0.2, "upper": null, "description": "Trading volume sits in the bottom quintile of the last year."},
          {"metric": "gap_atr", "comparison": "gte", "lower": 0.75, "upper": null, "description": "Gap size indicates stressed execution conditions."},
          {"metric": "hy_oas_z_20d", "comparison": "gte", "lower": 1.0, "upper": null, "description": "High-yield spreads have widened versus the recent baseline."}
        ]
      },
      "macro_alignment": {
        "displayName": "Global/Macro Regime",
        "requiredMetrics": ["global_equity_alignment", "rates_event_flag", "cross_asset_stress_alignment"],
        "rules": [
          {"metric": "global_equity_alignment", "comparison": "bool_true", "lower": null, "upper": null, "description": "Global equity proxies are moving in the same direction."},
          {"metric": "rates_event_flag", "comparison": "bool_true", "lower": null, "upper": null, "description": "A rates or macro event flag is active."},
          {"metric": "cross_asset_stress_alignment", "comparison": "bool_true", "lower": null, "upper": null, "description": "Rates, credit, and volatility proxies are aligned."}
        ]
      },
      "unclassified": {
        "displayName": "Unclassified",
        "requiredMetrics": [],
        "rules": []
      }
    },
    "haltVixThreshold": 32.0,
    "haltVixStreakDays": 2
  }'::jsonb,
  'published',
  '5edb792d2703cf163163b133fa2ccbca',
  NOW(),
  NOW()
)
ON CONFLICT (model_name, version) DO NOTHING;

UPDATE core.regime_models
SET
  version = GREATEST(version, 3),
  description = 'Default multi-label observational regime monitor model',
  config = CASE
    WHEN version < 3 THEN
      '{
        "activationThreshold": 0.6,
        "signalConfigs": {
          "trending_up": {
            "displayName": "Trending (Up)",
            "requiredMetrics": ["spy_above_sma_200", "qqq_above_sma_200", "spy_return_20d"],
            "rules": [
              {"metric": "spy_above_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Broad U.S. equity index is above its 200-day average."},
              {"metric": "qqq_above_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Nasdaq-100 proxy is above its 200-day average."},
              {"metric": "spy_return_20d", "comparison": "gte", "lower": 0.02, "upper": null, "description": "Broad market 20-day return is at least +2%."}
            ]
          },
          "trending_down": {
            "displayName": "Trending (Down)",
            "requiredMetrics": ["spy_below_sma_200", "qqq_below_sma_200", "spy_return_20d"],
            "rules": [
              {"metric": "spy_below_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Broad U.S. equity index is below its 200-day average."},
              {"metric": "qqq_below_sma_200", "comparison": "bool_true", "lower": null, "upper": null, "description": "Nasdaq-100 proxy is below its 200-day average."},
              {"metric": "spy_return_20d", "comparison": "lte", "lower": -0.02, "upper": null, "description": "Broad market 20-day return is at most -2%."}
            ]
          },
          "mean_reverting": {
            "displayName": "Mean-Reverting",
            "requiredMetrics": ["rsi_14d", "bb_width_20d", "abs_spy_return_20d"],
            "rules": [
              {"metric": "rsi_14d", "comparison": "between", "lower": 30.0, "upper": 70.0, "description": "RSI is in the neutral mean-reversion corridor."},
              {"metric": "bb_width_20d", "comparison": "lte", "lower": 0.18, "upper": null, "description": "Bollinger band width remains compressed."},
              {"metric": "abs_spy_return_20d", "comparison": "lte", "lower": 0.03, "upper": null, "description": "The broad market is not in a strong directional burst."}
            ]
          },
          "low_volatility": {
            "displayName": "Low Volatility",
            "requiredMetrics": ["vix_spot_close", "atr_14d_pct_of_close", "bb_width_20d"],
            "rules": [
              {"metric": "vix_spot_close", "comparison": "lte", "lower": 15.0, "upper": null, "description": "VIX is in the low-volatility zone."},
              {"metric": "atr_14d_pct_of_close", "comparison": "lte", "lower": 0.03, "upper": null, "description": "ATR is narrow relative to price."},
              {"metric": "bb_width_20d", "comparison": "lte", "lower": 0.12, "upper": null, "description": "Bollinger bands remain narrow."}
            ]
          },
          "high_volatility": {
            "displayName": "High Volatility",
            "requiredMetrics": ["vix_spot_close", "atr_14d_pct_of_close", "gap_atr"],
            "rules": [
              {"metric": "vix_spot_close", "comparison": "gte", "lower": 25.0, "upper": null, "description": "VIX is elevated relative to normal conditions."},
              {"metric": "atr_14d_pct_of_close", "comparison": "gte", "lower": 0.04, "upper": null, "description": "ATR has expanded materially relative to price."},
              {"metric": "gap_atr", "comparison": "gte", "lower": 0.5, "upper": null, "description": "Overnight or opening gaps are large versus ATR."}
            ]
          },
          "liquidity_stress": {
            "displayName": "Liquidity Regime",
            "requiredMetrics": ["volume_pct_rank_252d", "gap_atr", "hy_oas_z_20d"],
            "rules": [
              {"metric": "volume_pct_rank_252d", "comparison": "lte", "lower": 0.2, "upper": null, "description": "Trading volume sits in the bottom quintile of the last year."},
              {"metric": "gap_atr", "comparison": "gte", "lower": 0.75, "upper": null, "description": "Gap size indicates stressed execution conditions."},
              {"metric": "hy_oas_z_20d", "comparison": "gte", "lower": 1.0, "upper": null, "description": "High-yield spreads have widened versus the recent baseline."}
            ]
          },
          "macro_alignment": {
            "displayName": "Global/Macro Regime",
            "requiredMetrics": ["global_equity_alignment", "rates_event_flag", "cross_asset_stress_alignment"],
            "rules": [
              {"metric": "global_equity_alignment", "comparison": "bool_true", "lower": null, "upper": null, "description": "Global equity proxies are moving in the same direction."},
              {"metric": "rates_event_flag", "comparison": "bool_true", "lower": null, "upper": null, "description": "A rates or macro event flag is active."},
              {"metric": "cross_asset_stress_alignment", "comparison": "bool_true", "lower": null, "upper": null, "description": "Rates, credit, and volatility proxies are aligned."}
            ]
          },
          "unclassified": {
            "displayName": "Unclassified",
            "requiredMetrics": [],
            "rules": []
          }
        },
        "haltVixThreshold": 32.0,
        "haltVixStreakDays": 2
      }'::jsonb
    ELSE config
  END,
  updated_at = NOW()
WHERE name = 'default-regime';

INSERT INTO core.regime_model_activations (model_name, model_version, activated_by, activated_at)
SELECT 'default-regime', 3, 'migration-0041', NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM core.regime_model_activations
  WHERE model_name = 'default-regime'
    AND model_version = 3
);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT ON TABLE gold.regime_macro_inputs_daily TO backtest_service;
  END IF;
END $$;

COMMIT;
