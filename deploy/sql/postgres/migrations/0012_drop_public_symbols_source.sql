BEGIN;

DROP INDEX IF EXISTS idx_public_symbols_source;
ALTER TABLE public.symbols DROP COLUMN IF EXISTS source;

COMMIT;
