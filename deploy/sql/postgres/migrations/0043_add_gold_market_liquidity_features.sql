BEGIN;

ALTER TABLE gold.market_data
    ADD COLUMN IF NOT EXISTS dist_prev_week_high_atr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dist_prev_week_low_atr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dist_prev_month_high_atr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dist_prev_month_low_atr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS position_in_20d_range DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS position_in_55d_range DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS swept_sr_resistance_1 INTEGER,
    ADD COLUMN IF NOT EXISTS swept_sr_support_1 INTEGER,
    ADD COLUMN IF NOT EXISTS bearish_sweep_magnitude_atr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bullish_sweep_magnitude_atr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bearish_sweep_reclaim_frac DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bullish_sweep_reclaim_frac DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bars_since_bearish_sweep INTEGER,
    ADD COLUMN IF NOT EXISTS bars_since_bullish_sweep INTEGER,
    ADD COLUMN IF NOT EXISTS bearish_confirm_after_sweep INTEGER,
    ADD COLUMN IF NOT EXISTS bullish_confirm_after_sweep INTEGER,
    ADD COLUMN IF NOT EXISTS amihud_20d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS amihud_z_252d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dollar_volume_20d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dollar_volume_z_252d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS liquidity_stress_score DOUBLE PRECISION;

CREATE OR REPLACE VIEW gold.market_data_by_date AS
SELECT * FROM gold.market_data;

COMMIT;
