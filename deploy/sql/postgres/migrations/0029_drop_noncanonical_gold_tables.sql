BEGIN;

DO $$
DECLARE
  retired_table RECORD;
BEGIN
  FOR retired_table IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'gold'
      AND tablename NOT IN (
        'market_data',
        'finance_data',
        'earnings_data',
        'price_target_data',
        'regime_inputs_daily',
        'regime_history',
        'regime_latest',
        'regime_transitions'
      )
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS gold.%I', retired_table.tablename);
  END LOOP;
END $$;

COMMIT;
