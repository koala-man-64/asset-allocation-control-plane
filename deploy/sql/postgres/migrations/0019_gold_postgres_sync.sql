BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS gold;

DO $$
BEGIN
  IF to_regclass('gold.market_data') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'gold' AND table_name = 'market_data' AND column_name = 'return_5d'
     ) THEN
    DROP TABLE gold.market_data;
  END IF;

  IF to_regclass('gold.finance_data') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'gold' AND table_name = 'finance_data' AND column_name = 'market_cap'
     ) THEN
    DROP TABLE gold.finance_data;
  END IF;

  IF to_regclass('gold.earnings_data') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'gold' AND table_name = 'earnings_data' AND column_name = 'surprise_std_8q'
     ) THEN
    DROP TABLE gold.earnings_data;
  END IF;

  IF to_regclass('gold.price_target_data') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'gold' AND table_name = 'price_target_data' AND column_name = 'obs_date'
     ) THEN
    DROP TABLE gold.price_target_data;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS core.gold_sync_state (
    domain TEXT NOT NULL,
    bucket TEXT NOT NULL,
    source_commit DOUBLE PRECISION,
    status TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    symbol_count INTEGER NOT NULL DEFAULT 0,
    min_observation_date DATE,
    max_observation_date DATE,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error TEXT,
    PRIMARY KEY (domain, bucket)
);

CREATE TABLE IF NOT EXISTS gold.market_data (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    return_1d DOUBLE PRECISION,
    return_5d DOUBLE PRECISION,
    return_20d DOUBLE PRECISION,
    return_60d DOUBLE PRECISION,
    vol_20d DOUBLE PRECISION,
    vol_60d DOUBLE PRECISION,
    rolling_max_252d DOUBLE PRECISION,
    drawdown_1y DOUBLE PRECISION,
    true_range DOUBLE PRECISION,
    atr_14d DOUBLE PRECISION,
    gap_atr DOUBLE PRECISION,
    sma_20d DOUBLE PRECISION,
    sma_50d DOUBLE PRECISION,
    sma_200d DOUBLE PRECISION,
    sma_20_gt_sma_50 INTEGER,
    sma_50_gt_sma_200 INTEGER,
    trend_50_200 DOUBLE PRECISION,
    above_sma_50 INTEGER,
    sma_20_crosses_above_sma_50 INTEGER,
    sma_20_crosses_below_sma_50 INTEGER,
    sma_50_crosses_above_sma_200 INTEGER,
    sma_50_crosses_below_sma_200 INTEGER,
    bb_width_20d DOUBLE PRECISION,
    range_close DOUBLE PRECISION,
    range_20 DOUBLE PRECISION,
    compression_score DOUBLE PRECISION,
    volume_z_20d DOUBLE PRECISION,
    volume_pct_rank_252d DOUBLE PRECISION,
    "range" DOUBLE PRECISION,
    body DOUBLE PRECISION,
    is_bull INTEGER,
    is_bear INTEGER,
    upper_shadow DOUBLE PRECISION,
    lower_shadow DOUBLE PRECISION,
    body_to_range DOUBLE PRECISION,
    upper_to_range DOUBLE PRECISION,
    lower_to_range DOUBLE PRECISION,
    pat_doji INTEGER,
    pat_spinning_top INTEGER,
    pat_bullish_marubozu INTEGER,
    pat_bearish_marubozu INTEGER,
    pat_star_gap_up INTEGER,
    pat_star_gap_down INTEGER,
    pat_star INTEGER,
    pat_hammer INTEGER,
    pat_hanging_man INTEGER,
    pat_inverted_hammer INTEGER,
    pat_shooting_star INTEGER,
    pat_dragonfly_doji INTEGER,
    pat_gravestone_doji INTEGER,
    pat_bullish_spinning_top INTEGER,
    pat_bearish_spinning_top INTEGER,
    pat_bullish_engulfing INTEGER,
    pat_bearish_engulfing INTEGER,
    pat_bullish_harami INTEGER,
    pat_bearish_harami INTEGER,
    pat_piercing_line INTEGER,
    pat_dark_cloud_line INTEGER,
    pat_tweezer_bottom INTEGER,
    pat_tweezer_top INTEGER,
    pat_bullish_kicker INTEGER,
    pat_bearish_kicker INTEGER,
    pat_morning_star INTEGER,
    pat_morning_doji_star INTEGER,
    pat_evening_star INTEGER,
    pat_evening_doji_star INTEGER,
    pat_bullish_abandoned_baby INTEGER,
    pat_bearish_abandoned_baby INTEGER,
    pat_three_white_soldiers INTEGER,
    pat_three_black_crows INTEGER,
    pat_bullish_three_line_strike INTEGER,
    pat_bearish_three_line_strike INTEGER,
    pat_three_inside_up INTEGER,
    pat_three_outside_up INTEGER,
    pat_three_inside_down INTEGER,
    pat_three_outside_down INTEGER,
    ha_open DOUBLE PRECISION,
    ha_high DOUBLE PRECISION,
    ha_low DOUBLE PRECISION,
    ha_close DOUBLE PRECISION,
    ichimoku_tenkan_sen_9 DOUBLE PRECISION,
    ichimoku_kijun_sen_26 DOUBLE PRECISION,
    ichimoku_senkou_span_a DOUBLE PRECISION,
    ichimoku_senkou_span_b DOUBLE PRECISION,
    ichimoku_senkou_span_a_26 DOUBLE PRECISION,
    ichimoku_senkou_span_b_26 DOUBLE PRECISION,
    ichimoku_chikou_span_26 DOUBLE PRECISION,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS gold.finance_data (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    pe_ratio DOUBLE PRECISION,
    forward_pe DOUBLE PRECISION,
    piotroski_roa_pos INTEGER,
    piotroski_cfo_pos INTEGER,
    piotroski_delta_roa_pos INTEGER,
    piotroski_accruals_pos INTEGER,
    piotroski_leverage_decrease INTEGER,
    piotroski_liquidity_increase INTEGER,
    piotroski_no_new_shares INTEGER,
    piotroski_gross_margin_increase INTEGER,
    piotroski_asset_turnover_increase INTEGER,
    piotroski_f_score INTEGER,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS gold.earnings_data (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    reported_eps DOUBLE PRECISION,
    eps_estimate DOUBLE PRECISION,
    surprise DOUBLE PRECISION,
    surprise_pct DOUBLE PRECISION,
    surprise_mean_4q DOUBLE PRECISION,
    surprise_std_8q DOUBLE PRECISION,
    beat_rate_8q DOUBLE PRECISION,
    is_earnings_day INTEGER,
    last_earnings_date DATE,
    days_since_earnings INTEGER,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS gold.price_target_data (
    symbol TEXT NOT NULL,
    obs_date DATE NOT NULL,
    tp_mean_est DOUBLE PRECISION,
    tp_std_dev_est DOUBLE PRECISION,
    tp_high_est DOUBLE PRECISION,
    tp_low_est DOUBLE PRECISION,
    tp_cnt_est INTEGER,
    tp_cnt_est_rev_up INTEGER,
    tp_cnt_est_rev_down INTEGER,
    disp_abs DOUBLE PRECISION,
    disp_norm DOUBLE PRECISION,
    disp_std_norm DOUBLE PRECISION,
    rev_net INTEGER,
    rev_ratio DOUBLE PRECISION,
    rev_intensity DOUBLE PRECISION,
    disp_norm_change_30d DOUBLE PRECISION,
    tp_mean_change_30d DOUBLE PRECISION,
    disp_z DOUBLE PRECISION,
    tp_mean_slope_90d DOUBLE PRECISION,
    PRIMARY KEY (symbol, obs_date)
);

CREATE INDEX IF NOT EXISTS idx_gold_market_data_symbol_date
    ON gold.market_data(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_market_data_date_symbol
    ON gold.market_data(date DESC, symbol);

CREATE INDEX IF NOT EXISTS idx_gold_finance_data_symbol_date
    ON gold.finance_data(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_finance_data_date_symbol
    ON gold.finance_data(date DESC, symbol);

CREATE INDEX IF NOT EXISTS idx_gold_earnings_data_symbol_date
    ON gold.earnings_data(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_earnings_data_date_symbol
    ON gold.earnings_data(date DESC, symbol);

CREATE INDEX IF NOT EXISTS idx_gold_price_target_data_symbol_obs_date
    ON gold.price_target_data(symbol, obs_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_price_target_data_obs_date_symbol
    ON gold.price_target_data(obs_date DESC, symbol);

CREATE OR REPLACE VIEW gold.market_data_by_date AS
SELECT * FROM gold.market_data;

CREATE OR REPLACE VIEW gold.finance_data_by_date AS
SELECT * FROM gold.finance_data;

CREATE OR REPLACE VIEW gold.earnings_data_by_date AS
SELECT * FROM gold.earnings_data;

CREATE OR REPLACE VIEW gold.price_target_data_by_date AS
SELECT * FROM gold.price_target_data;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA gold TO backtest_service;
    GRANT SELECT ON ALL TABLES IN SCHEMA gold TO backtest_service;
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO backtest_service;

    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.gold_sync_state TO backtest_service;
  END IF;
END $$;

COMMIT;
