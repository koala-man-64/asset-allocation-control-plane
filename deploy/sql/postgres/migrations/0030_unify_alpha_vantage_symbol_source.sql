BEGIN;

ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_alpha_vantage BOOLEAN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'core'
      AND table_name = 'symbols'
      AND column_name = 'source_alphavantage'
  ) THEN
    EXECUTE '
      UPDATE core.symbols
      SET source_alpha_vantage = COALESCE(source_alpha_vantage, source_alphavantage, FALSE)
      WHERE source_alpha_vantage IS NULL OR source_alpha_vantage = FALSE
    ';

    EXECUTE 'ALTER TABLE core.symbols DROP COLUMN source_alphavantage';
  END IF;
END $$;

UPDATE core.symbols
SET source_alpha_vantage = COALESCE(source_alpha_vantage, FALSE)
WHERE source_alpha_vantage IS NULL;

COMMIT;
