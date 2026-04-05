BEGIN;

DO $$
BEGIN
  IF to_regclass('core.debug_symbols') IS NOT NULL THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'core'
        AND table_name = 'runtime_config'
        AND column_name = 'enabled'
    ) THEN
      INSERT INTO core.runtime_config(scope, key, enabled, value, description, updated_at, updated_by)
      SELECT
        'global' AS scope,
        'DEBUG_SYMBOLS' AS key,
        COALESCE(enabled, FALSE) AS enabled,
        COALESCE(symbols, '') AS value,
        'Comma-separated allowlist of symbols applied when debug filtering is enabled.' AS description,
        COALESCE(updated_at, now()) AS updated_at,
        updated_by
      FROM core.debug_symbols
      WHERE id = 1
        AND (
          COALESCE(enabled, FALSE)
          OR COALESCE(symbols, '') <> ''
          OR updated_by IS NOT NULL
        )
      ON CONFLICT (scope, key) DO NOTHING;
    ELSE
      INSERT INTO core.runtime_config(scope, key, value, description, updated_at, updated_by)
      SELECT
        'global' AS scope,
        'DEBUG_SYMBOLS' AS key,
        COALESCE(symbols, '') AS value,
        'Comma-separated allowlist of symbols applied when debug filtering is enabled.' AS description,
        COALESCE(updated_at, now()) AS updated_at,
        updated_by
      FROM core.debug_symbols
      WHERE id = 1
        AND (
          COALESCE(enabled, FALSE)
          OR COALESCE(symbols, '') <> ''
          OR updated_by IS NOT NULL
        )
      ON CONFLICT (scope, key) DO NOTHING;
    END IF;
  END IF;
END $$;

COMMIT;
