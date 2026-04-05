BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS gold;

ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS industry_2 TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS optionable TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS exchange TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS asset_type TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS ipo_date TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS delisting_date TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_nasdaq BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_massive BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_alpha_vantage BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_alphavantage BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS is_optionable BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE core.symbols
SET optionable = CASE
      WHEN is_optionable IS TRUE THEN 'Y'
      WHEN is_optionable IS FALSE THEN 'N'
      ELSE optionable
    END
WHERE optionable IS NULL AND is_optionable IS NOT NULL;

UPDATE core.symbols
SET is_optionable = CASE
      WHEN upper(trim(optionable)) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
      WHEN upper(trim(optionable)) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
      ELSE is_optionable
    END
WHERE optionable IS NOT NULL AND is_optionable IS NULL;

CREATE TABLE IF NOT EXISTS core.symbol_sync_state (
  id SMALLINT PRIMARY KEY,
  last_refreshed_at TIMESTAMPTZ,
  last_refreshed_sources JSONB,
  last_refresh_error TEXT
);

DO $$
DECLARE
  public_symbols_has_source_alpha_vantage BOOLEAN;
  public_symbols_has_source_alphavantage BOOLEAN;
  source_alpha_vantage_expr TEXT;
  source_alphavantage_expr TEXT;
BEGIN
  IF to_regclass('public.symbols') IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'symbols'
        AND column_name = 'source_alpha_vantage'
    )
    INTO public_symbols_has_source_alpha_vantage;

    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'symbols'
        AND column_name = 'source_alphavantage'
    )
    INTO public_symbols_has_source_alphavantage;

    source_alpha_vantage_expr := CASE
      WHEN public_symbols_has_source_alpha_vantage AND public_symbols_has_source_alphavantage
        THEN 'COALESCE(p.source_alpha_vantage, p.source_alphavantage, FALSE)'
      WHEN public_symbols_has_source_alpha_vantage
        THEN 'COALESCE(p.source_alpha_vantage, FALSE)'
      WHEN public_symbols_has_source_alphavantage
        THEN 'COALESCE(p.source_alphavantage, FALSE)'
      ELSE 'FALSE'
    END;

    source_alphavantage_expr := CASE
      WHEN public_symbols_has_source_alphavantage AND public_symbols_has_source_alpha_vantage
        THEN 'COALESCE(p.source_alphavantage, p.source_alpha_vantage, FALSE)'
      WHEN public_symbols_has_source_alphavantage
        THEN 'COALESCE(p.source_alphavantage, FALSE)'
      WHEN public_symbols_has_source_alpha_vantage
        THEN 'COALESCE(p.source_alpha_vantage, FALSE)'
      ELSE 'FALSE'
    END;

    EXECUTE format($symbols_move$
      INSERT INTO core.symbols AS s (
        symbol,
        name,
        description,
        sector,
        industry,
        industry_2,
        optionable,
        is_optionable,
        country,
        exchange,
        asset_type,
        ipo_date,
        delisting_date,
        status,
        source_nasdaq,
        source_massive,
        source_alpha_vantage,
        source_alphavantage,
        updated_at
      )
      SELECT
        p.symbol,
        p.name,
        p.description,
        p.sector,
        p.industry,
        p.industry_2,
        p.optionable,
        CASE
          WHEN upper(trim(COALESCE(p.optionable, ''))) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
          WHEN upper(trim(COALESCE(p.optionable, ''))) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
          ELSE NULL
        END AS is_optionable,
        p.country,
        p.exchange,
        p.asset_type,
        p.ipo_date,
        p.delisting_date,
        p.status,
        p.source_nasdaq,
        p.source_massive,
        %s,
        %s,
        p.updated_at
      FROM public.symbols AS p
      ON CONFLICT (symbol) DO UPDATE
      SET name = COALESCE(EXCLUDED.name, s.name),
          description = COALESCE(EXCLUDED.description, s.description),
          sector = COALESCE(EXCLUDED.sector, s.sector),
          industry = COALESCE(EXCLUDED.industry, s.industry),
          industry_2 = COALESCE(EXCLUDED.industry_2, s.industry_2),
          optionable = COALESCE(EXCLUDED.optionable, s.optionable),
          is_optionable = COALESCE(EXCLUDED.is_optionable, s.is_optionable),
          country = COALESCE(EXCLUDED.country, s.country),
          exchange = COALESCE(EXCLUDED.exchange, s.exchange),
          asset_type = COALESCE(EXCLUDED.asset_type, s.asset_type),
          ipo_date = COALESCE(EXCLUDED.ipo_date, s.ipo_date),
          delisting_date = COALESCE(EXCLUDED.delisting_date, s.delisting_date),
          status = COALESCE(EXCLUDED.status, s.status),
          source_nasdaq = COALESCE(EXCLUDED.source_nasdaq, s.source_nasdaq),
          source_massive = COALESCE(EXCLUDED.source_massive, s.source_massive),
          source_alpha_vantage = COALESCE(EXCLUDED.source_alpha_vantage, s.source_alpha_vantage),
          source_alphavantage = COALESCE(EXCLUDED.source_alphavantage, s.source_alphavantage),
          updated_at = GREATEST(s.updated_at, EXCLUDED.updated_at)
    $symbols_move$, source_alpha_vantage_expr, source_alphavantage_expr);

    DROP TABLE public.symbols;
  END IF;

  IF to_regclass('public.symbol_sync_state') IS NOT NULL THEN
    INSERT INTO core.symbol_sync_state AS s (
      id,
      last_refreshed_at,
      last_refreshed_sources,
      last_refresh_error
    )
    SELECT
      id,
      last_refreshed_at,
      last_refreshed_sources,
      last_refresh_error
    FROM public.symbol_sync_state
    ON CONFLICT (id) DO UPDATE
    SET last_refreshed_at = COALESCE(EXCLUDED.last_refreshed_at, s.last_refreshed_at),
        last_refreshed_sources = COALESCE(EXCLUDED.last_refreshed_sources, s.last_refreshed_sources),
        last_refresh_error = COALESCE(EXCLUDED.last_refresh_error, s.last_refresh_error);

    DROP TABLE public.symbol_sync_state;
  END IF;
END $$;

INSERT INTO core.symbol_sync_state(id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_core_symbols_sector ON core.symbols(sector);
CREATE INDEX IF NOT EXISTS idx_core_symbols_industry ON core.symbols(industry);
CREATE INDEX IF NOT EXISTS idx_core_symbols_status ON core.symbols(status);
CREATE INDEX IF NOT EXISTS idx_core_symbols_exchange ON core.symbols(exchange);
CREATE INDEX IF NOT EXISTS idx_core_symbols_updated_at ON core.symbols(updated_at DESC);

COMMIT;
