BEGIN;

CREATE TABLE IF NOT EXISTS public.symbols (
  symbol TEXT PRIMARY KEY,
  name TEXT,
  description TEXT,
  sector TEXT,
  industry TEXT,
  industry_2 TEXT,
  optionable TEXT,
  country TEXT,
  exchange TEXT,
  asset_type TEXT,
  ipo_date TEXT,
  delisting_date TEXT,
  status TEXT,
  source_nasdaq BOOLEAN,
  source_alpha_vantage BOOLEAN,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS name TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS sector TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS industry TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS industry_2 TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS optionable TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS country TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS exchange TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS asset_type TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS ipo_date TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS delisting_date TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS source_nasdaq BOOLEAN;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS source_alpha_vantage BOOLEAN;
ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

DELETE FROM public.symbols AS s1
USING public.symbols AS s2
WHERE s1.symbol = s2.symbol
  AND s1.ctid < s2.ctid;

CREATE UNIQUE INDEX IF NOT EXISTS symbols_symbol_uidx ON public.symbols(symbol);

CREATE TABLE IF NOT EXISTS public.symbol_sync_state (
  id SMALLINT PRIMARY KEY,
  last_refreshed_at TIMESTAMPTZ,
  last_refreshed_sources JSONB,
  last_refresh_error TEXT
);

INSERT INTO public.symbol_sync_state(id) VALUES (1) ON CONFLICT DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'core' AND table_name = 'symbols'
  ) THEN
    INSERT INTO public.symbols (
      symbol,
      name,
      sector,
      industry,
      country,
      optionable,
      updated_at
    )
    SELECT
      s.symbol,
      s.name,
      s.sector,
      s.industry,
      s.country,
      CASE
        WHEN s.is_optionable IS TRUE THEN 'Y'
        WHEN s.is_optionable IS FALSE THEN 'N'
        ELSE NULL
      END AS optionable,
      now()
    FROM core.symbols AS s
    ON CONFLICT (symbol) DO NOTHING;
  END IF;
END $$;

COMMIT;
