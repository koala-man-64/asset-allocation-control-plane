BEGIN;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS platinum;

-- Core Symbols Table
CREATE TABLE IF NOT EXISTS core.symbols (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT,
    country TEXT,
    is_optionable BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Gold Market Data
CREATE TABLE IF NOT EXISTS gold.market_data (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    return_1d DOUBLE PRECISION,
    vol_20d DOUBLE PRECISION,
    atr_14d DOUBLE PRECISION,
    sma_20d DOUBLE PRECISION,
    sma_50d DOUBLE PRECISION,
    sma_200d DOUBLE PRECISION,
    bb_width_20d DOUBLE PRECISION,
    features JSONB, -- Stores additional indicators without rigid schema
    PRIMARY KEY (date, symbol)
);

-- Gold Earnings Data
CREATE TABLE IF NOT EXISTS gold.earnings_data (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    reported_eps DOUBLE PRECISION,
    eps_estimate DOUBLE PRECISION,
    surprise_pct DOUBLE PRECISION,
    surprise_mean_4q DOUBLE PRECISION,
    beat_rate_8q DOUBLE PRECISION,
    is_earnings_day BOOLEAN,
    last_earnings_date DATE,
    days_since_earnings INTEGER,
    PRIMARY KEY (date, symbol)
);

-- Gold Finance Data
CREATE TABLE IF NOT EXISTS gold.finance_data (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    revenue DOUBLE PRECISION,
    net_income DOUBLE PRECISION,
    ebitda DOUBLE PRECISION,
    free_cash_flow DOUBLE PRECISION,
    gross_margin DOUBLE PRECISION,
    op_margin DOUBLE PRECISION,
    fcf_margin DOUBLE PRECISION,
    pe_ratio DOUBLE PRECISION,
    ev_ebitda DOUBLE PRECISION,
    piotroski_f_score INTEGER,
    features JSONB, -- Stores additional ratios/metrics
    PRIMARY KEY (date, symbol)
);

-- Gold Price Target Data
CREATE TABLE IF NOT EXISTS gold.price_target_data (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    mean_target DOUBLE PRECISION,
    high_target DOUBLE PRECISION,
    low_target DOUBLE PRECISION,
    num_analysts INTEGER,
    dispersion_norm DOUBLE PRECISION,
    dispersion_z DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_gold_market_symbol ON gold.market_data (symbol);
CREATE INDEX IF NOT EXISTS idx_gold_market_date ON gold.market_data (date);
CREATE INDEX IF NOT EXISTS idx_gold_earnings_symbol ON gold.earnings_data (symbol);
CREATE INDEX IF NOT EXISTS idx_gold_finance_symbol ON gold.finance_data (symbol);
CREATE INDEX IF NOT EXISTS idx_gold_targets_symbol ON gold.price_target_data (symbol);

-- Permissions
DO $$
BEGIN
  -- Backtest service (read-only on gold generally, but potentially writable in future? sticking to explicit needs)
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA gold TO backtest_service;
    GRANT SELECT ON ALL TABLES IN SCHEMA gold TO backtest_service;
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO backtest_service;

    GRANT SELECT ON TABLE core.symbols TO backtest_service;
  END IF;
END $$;

COMMIT;
