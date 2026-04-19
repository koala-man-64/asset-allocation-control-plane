BEGIN;

CREATE TABLE IF NOT EXISTS core.symbol_profiles (
    symbol TEXT PRIMARY KEY REFERENCES core.symbols(symbol) ON DELETE CASCADE,
    security_type_norm TEXT,
    exchange_mic TEXT,
    country_of_risk TEXT,
    sector_norm TEXT,
    industry_group_norm TEXT,
    industry_norm TEXT,
    is_adr BOOLEAN,
    is_etf BOOLEAN,
    is_cef BOOLEAN,
    is_preferred BOOLEAN,
    share_class TEXT,
    listing_status_norm TEXT,
    issuer_summary_short TEXT,
    source_kind TEXT NOT NULL DEFAULT 'ai',
    source_fingerprint TEXT,
    ai_model TEXT,
    ai_confidence DOUBLE PRECISION,
    validation_status TEXT NOT NULL DEFAULT 'accepted',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_kind IN ('provider', 'ai', 'derived', 'override')),
    CHECK (validation_status IN ('accepted', 'rejected', 'pending', 'locked'))
);

CREATE INDEX IF NOT EXISTS idx_core_symbol_profiles_updated_at
    ON core.symbol_profiles(updated_at DESC);

CREATE TABLE IF NOT EXISTS core.symbol_profile_history (
    history_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL REFERENCES core.symbols(symbol) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    previous_value JSONB,
    new_value JSONB,
    source_kind TEXT NOT NULL,
    ai_model TEXT,
    ai_confidence DOUBLE PRECISION,
    change_reason TEXT,
    run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_kind IN ('provider', 'ai', 'derived', 'override'))
);

CREATE INDEX IF NOT EXISTS idx_core_symbol_profile_history_symbol_updated_at
    ON core.symbol_profile_history(symbol, updated_at DESC);

CREATE TABLE IF NOT EXISTS core.symbol_profile_overrides (
    symbol TEXT NOT NULL REFERENCES core.symbols(symbol) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    value_json JSONB,
    is_locked BOOLEAN NOT NULL DEFAULT FALSE,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, field_name)
);

CREATE INDEX IF NOT EXISTS idx_core_symbol_profile_overrides_locked
    ON core.symbol_profile_overrides(is_locked)
    WHERE is_locked = TRUE;

CREATE TABLE IF NOT EXISTS core.symbol_cleanup_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'queued',
    mode TEXT NOT NULL DEFAULT 'fill_missing',
    queued_count INTEGER NOT NULL DEFAULT 0,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    accepted_update_count INTEGER NOT NULL DEFAULT 0,
    rejected_update_count INTEGER NOT NULL DEFAULT 0,
    locked_skip_count INTEGER NOT NULL DEFAULT 0,
    overwrite_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    CHECK (mode IN ('fill_missing', 'full_reconcile'))
);

CREATE INDEX IF NOT EXISTS idx_core_symbol_cleanup_runs_created_at
    ON core.symbol_cleanup_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS core.symbol_cleanup_work_queue (
    work_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES core.symbol_cleanup_runs(run_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL REFERENCES core.symbols(symbol) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'queued',
    requested_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    execution_name TEXT,
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    accepted_update_count INTEGER NOT NULL DEFAULT 0,
    rejected_update_count INTEGER NOT NULL DEFAULT 0,
    locked_skip_count INTEGER NOT NULL DEFAULT 0,
    overwrite_count INTEGER NOT NULL DEFAULT 0,
    result_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, symbol),
    CHECK (status IN ('queued', 'claimed', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_core_symbol_cleanup_work_queue_status_created_at
    ON core.symbol_cleanup_work_queue(status, created_at);

CREATE INDEX IF NOT EXISTS idx_core_symbol_cleanup_work_queue_run_id
    ON core.symbol_cleanup_work_queue(run_id);

CREATE OR REPLACE VIEW core.symbol_catalog_current AS
WITH latest_finance AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        market_cap
    FROM gold.finance_data
    ORDER BY symbol, date DESC
),
market_liquidity AS (
    SELECT
        symbol,
        AVG(COALESCE(close, 0) * COALESCE(volume, 0)) FILTER (WHERE rn <= 20) AS avg_dollar_volume_20d
    FROM (
        SELECT
            symbol,
            close,
            volume,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM gold.market_data
    ) ranked
    GROUP BY symbol
)
SELECT
    s.symbol,
    s.name,
    s.description,
    s.sector,
    s.industry,
    s.industry_2,
    s.country,
    s.exchange,
    s.asset_type,
    s.ipo_date,
    s.delisting_date,
    s.status,
    COALESCE(
        s.is_optionable,
        CASE
            WHEN upper(trim(COALESCE(s.optionable, ''))) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
            WHEN upper(trim(COALESCE(s.optionable, ''))) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
            ELSE NULL
        END
    ) AS is_optionable,
    s.source_nasdaq,
    s.source_massive,
    s.source_alpha_vantage,
    p.security_type_norm,
    p.exchange_mic,
    p.country_of_risk,
    p.sector_norm,
    p.industry_group_norm,
    p.industry_norm,
    p.is_adr,
    p.is_etf,
    p.is_cef,
    p.is_preferred,
    p.share_class,
    p.listing_status_norm,
    p.issuer_summary_short,
    p.source_kind,
    p.source_fingerprint,
    p.ai_model,
    p.ai_confidence,
    p.validation_status,
    p.updated_at,
    latest_finance.market_cap AS market_cap_usd,
    CASE
        WHEN latest_finance.market_cap >= 200000000000 THEN 'mega'
        WHEN latest_finance.market_cap >= 10000000000 THEN 'large'
        WHEN latest_finance.market_cap >= 2000000000 THEN 'mid'
        WHEN latest_finance.market_cap >= 300000000 THEN 'small'
        WHEN latest_finance.market_cap IS NOT NULL THEN 'micro'
        ELSE NULL
    END AS market_cap_bucket,
    market_liquidity.avg_dollar_volume_20d,
    CASE
        WHEN market_liquidity.avg_dollar_volume_20d >= 50000000 THEN 'high'
        WHEN market_liquidity.avg_dollar_volume_20d >= 10000000 THEN 'medium'
        WHEN market_liquidity.avg_dollar_volume_20d IS NOT NULL THEN 'low'
        ELSE NULL
    END AS liquidity_bucket,
    CASE
        WHEN LOWER(COALESCE(s.asset_type, '')) IN ('stock', 'common stock', 'equity')
             AND COALESCE(p.is_etf, FALSE) = FALSE
             AND COALESCE(p.is_cef, FALSE) = FALSE
             AND COALESCE(p.is_preferred, FALSE) = FALSE
        THEN TRUE
        ELSE FALSE
    END AS is_tradeable_common_equity,
    (
        (
            CASE WHEN COALESCE(NULLIF(trim(s.name), ''), NULL) IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(NULLIF(trim(s.exchange), ''), NULL) IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(NULLIF(trim(COALESCE(p.sector_norm, s.sector)), ''), NULL) IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(NULLIF(trim(COALESCE(p.industry_norm, s.industry)), ''), NULL) IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN market_liquidity.avg_dollar_volume_20d IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN latest_finance.market_cap IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(NULLIF(trim(p.security_type_norm), ''), NULL) IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(NULLIF(trim(p.listing_status_norm), ''), NULL) IS NOT NULL THEN 1 ELSE 0 END
        ) / 8.0
    ) AS data_completeness_score
FROM core.symbols AS s
LEFT JOIN core.symbol_profiles AS p
    ON p.symbol = s.symbol
LEFT JOIN latest_finance
    ON latest_finance.symbol = s.symbol
LEFT JOIN market_liquidity
    ON market_liquidity.symbol = s.symbol;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE ON TABLE core.symbol_profiles TO backtest_service;
    GRANT SELECT, INSERT ON TABLE core.symbol_profile_history TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.symbol_profile_overrides TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.symbol_cleanup_runs TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.symbol_cleanup_work_queue TO backtest_service;
    GRANT SELECT ON TABLE core.symbol_catalog_current TO backtest_service;
  END IF;
END $$;

COMMIT;
