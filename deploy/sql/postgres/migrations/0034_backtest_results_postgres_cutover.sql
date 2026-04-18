BEGIN;

ALTER TABLE core.runs
  ADD COLUMN IF NOT EXISTS results_ready_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS results_schema_version SMALLINT NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS core.backtest_run_summary (
  run_id TEXT PRIMARY KEY REFERENCES core.runs(run_id) ON DELETE CASCADE,
  total_return DOUBLE PRECISION,
  annualized_return DOUBLE PRECISION,
  annualized_volatility DOUBLE PRECISION,
  sharpe_ratio DOUBLE PRECISION,
  max_drawdown DOUBLE PRECISION,
  trades INTEGER NOT NULL DEFAULT 0 CHECK (trades >= 0),
  initial_cash DOUBLE PRECISION,
  final_equity DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS core.backtest_timeseries (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  bar_ts TIMESTAMPTZ NOT NULL,
  portfolio_value DOUBLE PRECISION NOT NULL,
  drawdown DOUBLE PRECISION NOT NULL,
  daily_return DOUBLE PRECISION,
  period_return DOUBLE PRECISION,
  cumulative_return DOUBLE PRECISION,
  cash DOUBLE PRECISION,
  gross_exposure DOUBLE PRECISION,
  net_exposure DOUBLE PRECISION,
  turnover DOUBLE PRECISION,
  commission DOUBLE PRECISION,
  slippage_cost DOUBLE PRECISION,
  trade_count INTEGER,
  PRIMARY KEY (run_id, bar_ts)
);

CREATE TABLE IF NOT EXISTS core.backtest_rolling_metrics (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  bar_ts TIMESTAMPTZ NOT NULL,
  window_days INTEGER NOT NULL CHECK (window_days >= 2),
  window_periods INTEGER,
  rolling_return DOUBLE PRECISION,
  rolling_volatility DOUBLE PRECISION,
  rolling_sharpe DOUBLE PRECISION,
  rolling_max_drawdown DOUBLE PRECISION,
  turnover_sum DOUBLE PRECISION,
  commission_sum DOUBLE PRECISION,
  slippage_cost_sum DOUBLE PRECISION,
  n_trades_sum DOUBLE PRECISION,
  gross_exposure_avg DOUBLE PRECISION,
  net_exposure_avg DOUBLE PRECISION,
  PRIMARY KEY (run_id, window_days, bar_ts)
);

ALTER TABLE core.backtest_timeseries
  ADD COLUMN IF NOT EXISTS period_return DOUBLE PRECISION;

ALTER TABLE core.backtest_rolling_metrics
  ADD COLUMN IF NOT EXISTS window_periods INTEGER;

CREATE TABLE IF NOT EXISTS core.backtest_trades (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  trade_seq INTEGER NOT NULL CHECK (trade_seq >= 1),
  execution_ts TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  quantity DOUBLE PRECISION NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  notional DOUBLE PRECISION NOT NULL,
  commission DOUBLE PRECISION NOT NULL DEFAULT 0,
  slippage_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
  cash_after DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (run_id, trade_seq)
);

CREATE TABLE IF NOT EXISTS core.backtest_selection_trace (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  rebalance_ts TIMESTAMPTZ NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
  symbol TEXT NOT NULL,
  score DOUBLE PRECISION,
  selected BOOLEAN NOT NULL,
  target_weight DOUBLE PRECISION,
  PRIMARY KEY (run_id, rebalance_ts, ordinal),
  CONSTRAINT uq_backtest_selection_trace_symbol UNIQUE (run_id, rebalance_ts, symbol)
);

CREATE TABLE IF NOT EXISTS core.backtest_regime_trace (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  bar_ts TIMESTAMPTZ NOT NULL,
  session_date DATE NOT NULL,
  model_name TEXT,
  model_version INTEGER,
  as_of_date DATE,
  effective_from_date DATE,
  regime_code TEXT,
  regime_status TEXT,
  matched_rule_id TEXT,
  halt_flag BOOLEAN NOT NULL DEFAULT FALSE,
  halt_reason TEXT,
  blocked BOOLEAN NOT NULL DEFAULT FALSE,
  blocked_reason TEXT,
  blocked_action TEXT,
  exposure_multiplier DOUBLE PRECISION,
  PRIMARY KEY (run_id, bar_ts)
);

CREATE INDEX IF NOT EXISTS idx_core_runs_completed_at
  ON core.runs(completed_at DESC)
  WHERE completed_at IS NOT NULL;

ALTER TABLE core.runs
  DROP COLUMN IF EXISTS summary_json,
  DROP COLUMN IF EXISTS artifact_manifest_path,
  DROP COLUMN IF EXISTS output_dir,
  DROP COLUMN IF EXISTS adls_container,
  DROP COLUMN IF EXISTS adls_prefix;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_run_summary TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_timeseries TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_rolling_metrics TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_trades TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_selection_trace TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_regime_trace TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.runs TO backtest_service;
  END IF;
END $$;

COMMIT;
