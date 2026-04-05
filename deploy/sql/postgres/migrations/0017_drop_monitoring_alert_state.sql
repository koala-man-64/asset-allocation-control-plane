BEGIN;

DROP TABLE IF EXISTS monitoring.alert_state;
DROP FUNCTION IF EXISTS monitoring.set_updated_at();

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'monitoring')
     AND NOT EXISTS (
       SELECT 1
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'monitoring'
     )
     AND NOT EXISTS (
       SELECT 1
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'monitoring'
     ) THEN
    EXECUTE 'DROP SCHEMA monitoring';
  END IF;
END $$;

COMMIT;
