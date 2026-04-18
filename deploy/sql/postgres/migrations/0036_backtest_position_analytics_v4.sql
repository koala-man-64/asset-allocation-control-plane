BEGIN;

ALTER TABLE IF EXISTS core.backtest_run_summary
  ADD COLUMN IF NOT EXISTS closed_positions INTEGER,
  ADD COLUMN IF NOT EXISTS winning_positions INTEGER,
  ADD COLUMN IF NOT EXISTS losing_positions INTEGER,
  ADD COLUMN IF NOT EXISTS hit_rate DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS avg_win_pnl DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS avg_loss_pnl DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS avg_win_return DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS avg_loss_return DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS payoff_ratio DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS profit_factor DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS expectancy_pnl DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS expectancy_return DOUBLE PRECISION;

ALTER TABLE IF EXISTS core.backtest_trades
  ADD COLUMN IF NOT EXISTS position_id TEXT,
  ADD COLUMN IF NOT EXISTS trade_role TEXT;

CREATE TABLE IF NOT EXISTS core.backtest_closed_positions (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  position_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ NOT NULL,
  holding_period_bars INTEGER NOT NULL DEFAULT 0 CHECK (holding_period_bars >= 0),
  average_cost DOUBLE PRECISION NOT NULL CHECK (average_cost >= 0),
  exit_price DOUBLE PRECISION NOT NULL,
  max_quantity DOUBLE PRECISION NOT NULL CHECK (max_quantity > 0),
  resize_count INTEGER NOT NULL DEFAULT 0 CHECK (resize_count >= 0),
  realized_pnl DOUBLE PRECISION NOT NULL,
  realized_return DOUBLE PRECISION,
  total_commission DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_slippage_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_transaction_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
  exit_reason TEXT,
  exit_rule_id TEXT,
  PRIMARY KEY (run_id, position_id)
);

CREATE INDEX IF NOT EXISTS idx_backtest_closed_positions_run_closed_at
  ON core.backtest_closed_positions(run_id, closed_at, position_id);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_closed_positions TO backtest_service;
  END IF;
END $$;

COMMIT;
