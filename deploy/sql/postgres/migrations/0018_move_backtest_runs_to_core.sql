BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ NULL,
  completed_at TIMESTAMPTZ NULL,
  run_name TEXT NULL,
  start_date TEXT NULL,
  end_date TEXT NULL,
  output_dir TEXT NULL,
  adls_container TEXT NULL,
  adls_prefix TEXT NULL,
  error TEXT NULL,
  config_json TEXT NOT NULL,
  effective_config_json TEXT NOT NULL
);

DO $$
BEGIN
  IF to_regclass('backtest.runs') IS NOT NULL THEN
    INSERT INTO core.runs AS r (
      run_id,
      status,
      submitted_at,
      started_at,
      completed_at,
      run_name,
      start_date,
      end_date,
      output_dir,
      adls_container,
      adls_prefix,
      error,
      config_json,
      effective_config_json
    )
    SELECT
      run_id,
      status,
      submitted_at,
      started_at,
      completed_at,
      run_name,
      start_date,
      end_date,
      output_dir,
      adls_container,
      adls_prefix,
      error,
      config_json,
      effective_config_json
    FROM backtest.runs
    ON CONFLICT (run_id) DO UPDATE
    SET status = EXCLUDED.status,
        submitted_at = EXCLUDED.submitted_at,
        started_at = EXCLUDED.started_at,
        completed_at = EXCLUDED.completed_at,
        run_name = EXCLUDED.run_name,
        start_date = EXCLUDED.start_date,
        end_date = EXCLUDED.end_date,
        output_dir = EXCLUDED.output_dir,
        adls_container = EXCLUDED.adls_container,
        adls_prefix = EXCLUDED.adls_prefix,
        error = EXCLUDED.error,
        config_json = EXCLUDED.config_json,
        effective_config_json = EXCLUDED.effective_config_json;

    DROP TABLE backtest.runs;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_core_runs_submitted_at
  ON core.runs(submitted_at);

CREATE INDEX IF NOT EXISTS idx_core_runs_status
  ON core.runs(status);

CREATE INDEX IF NOT EXISTS idx_core_runs_status_submitted_at
  ON core.runs(status, submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_runs_completed_at
  ON core.runs(completed_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.runs TO backtest_service;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'backtest')
     AND NOT EXISTS (
       SELECT 1
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'backtest'
     )
     AND NOT EXISTS (
       SELECT 1
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'backtest'
     ) THEN
    EXECUTE 'DROP SCHEMA backtest';
  END IF;
END $$;

COMMIT;
