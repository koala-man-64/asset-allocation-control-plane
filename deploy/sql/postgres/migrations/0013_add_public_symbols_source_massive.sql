BEGIN;

ALTER TABLE public.symbols
  ADD COLUMN IF NOT EXISTS source_massive BOOLEAN;

UPDATE public.symbols
SET source_massive = FALSE
WHERE source_massive IS NULL;

COMMIT;
