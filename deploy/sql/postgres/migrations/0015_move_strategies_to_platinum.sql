BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

DO $$
BEGIN
  CREATE TABLE IF NOT EXISTS core.strategies (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    type TEXT NOT NULL DEFAULT 'configured',
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
  );

  IF to_regclass('public.strategies') IS NOT NULL THEN
    INSERT INTO core.strategies AS s (name, description, type, config, created_at, updated_at)
    SELECT name, description, type, config, created_at, updated_at
    FROM public.strategies
    ON CONFLICT (name) DO UPDATE
    SET description = EXCLUDED.description,
        type = EXCLUDED.type,
        config = EXCLUDED.config,
        created_at = COALESCE(LEAST(s.created_at, EXCLUDED.created_at), s.created_at, EXCLUDED.created_at),
        updated_at = COALESCE(GREATEST(s.updated_at, EXCLUDED.updated_at), s.updated_at, EXCLUDED.updated_at)
    WHERE s.updated_at IS NULL
       OR EXCLUDED.updated_at >= s.updated_at;

    DROP TABLE public.strategies;
  END IF;

  IF to_regclass('platinum.strategies') IS NOT NULL THEN
    INSERT INTO core.strategies AS s (name, description, type, config, created_at, updated_at)
    SELECT name, description, type, config, created_at, updated_at
    FROM platinum.strategies
    ON CONFLICT (name) DO UPDATE
    SET description = EXCLUDED.description,
        type = EXCLUDED.type,
        config = EXCLUDED.config,
        created_at = COALESCE(LEAST(s.created_at, EXCLUDED.created_at), s.created_at, EXCLUDED.created_at),
        updated_at = COALESCE(GREATEST(s.updated_at, EXCLUDED.updated_at), s.updated_at, EXCLUDED.updated_at)
    WHERE s.updated_at IS NULL
       OR EXCLUDED.updated_at >= s.updated_at;

    DROP TABLE platinum.strategies;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_core_strategies_type
  ON core.strategies(type);
CREATE INDEX IF NOT EXISTS idx_core_strategies_updated_at
  ON core.strategies(updated_at DESC);

COMMIT;
