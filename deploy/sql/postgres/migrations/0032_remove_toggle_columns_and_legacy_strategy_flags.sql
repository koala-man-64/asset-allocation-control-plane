BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'core'
      AND table_name = 'runtime_config'
      AND column_name = 'enabled'
  ) THEN
    EXECUTE 'DELETE FROM core.runtime_config WHERE enabled = FALSE';
    EXECUTE 'ALTER TABLE core.runtime_config DROP COLUMN enabled';
  END IF;
END $$;

DROP INDEX IF EXISTS idx_purge_rules_enabled_next_run;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'core'
      AND table_name = 'purge_rules'
      AND column_name = 'enabled'
  ) THEN
    EXECUTE 'DELETE FROM core.purge_rules WHERE enabled = FALSE';
    EXECUTE 'ALTER TABLE core.purge_rules DROP COLUMN enabled';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION core.normalize_strategy_toggle_shape(payload JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  normalized JSONB := COALESCE(payload, '{}'::jsonb);
  regime_policy JSONB;
  exits JSONB;
BEGIN
  IF jsonb_typeof(normalized) <> 'object' THEN
    RETURN normalized;
  END IF;

  regime_policy := normalized -> 'regimePolicy';
  IF jsonb_typeof(regime_policy) = 'object' THEN
    IF regime_policy @> '{"enabled": false}'::jsonb THEN
      normalized := normalized - 'regimePolicy';
    ELSE
      normalized := jsonb_set(
        normalized,
        '{regimePolicy}',
        regime_policy - 'enabled',
        TRUE
      );
    END IF;
  END IF;

  exits := normalized -> 'exits';
  IF jsonb_typeof(exits) = 'array' THEN
    normalized := jsonb_set(
      normalized,
      '{exits}',
      COALESCE(
        (
          SELECT jsonb_agg(
            CASE
              WHEN jsonb_typeof(item) = 'object' THEN item - 'enabled'
              ELSE item
            END
          )
          FROM jsonb_array_elements(exits) AS item
          WHERE NOT (
            jsonb_typeof(item) = 'object'
            AND item @> '{"enabled": false}'::jsonb
          )
        ),
        '[]'::jsonb
      ),
      TRUE
    );
  END IF;

  RETURN normalized;
END;
$$;

UPDATE core.strategies
SET config = core.normalize_strategy_toggle_shape(config)
WHERE config IS NOT NULL;

UPDATE core.strategy_revisions
SET config = core.normalize_strategy_toggle_shape(config)
WHERE config IS NOT NULL;

DROP FUNCTION core.normalize_strategy_toggle_shape(JSONB);

COMMIT;
