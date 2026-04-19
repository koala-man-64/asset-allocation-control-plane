BEGIN;

CREATE TABLE IF NOT EXISTS core.portfolio_definitions (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  benchmark_symbol TEXT,
  status TEXT NOT NULL DEFAULT 'draft',
  latest_version INTEGER,
  active_version INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('draft', 'active', 'archived'))
);

CREATE TABLE IF NOT EXISTS core.portfolio_revisions (
  portfolio_name TEXT NOT NULL REFERENCES core.portfolio_definitions(name) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  benchmark_symbol TEXT,
  allocations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes TEXT NOT NULL DEFAULT '',
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT,
  PRIMARY KEY (portfolio_name, version)
);

CREATE TABLE IF NOT EXISTS core.portfolio_accounts (
  account_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'draft',
  mode TEXT NOT NULL DEFAULT 'internal_model_managed',
  accounting_depth TEXT NOT NULL DEFAULT 'position_level',
  cadence_mode TEXT NOT NULL DEFAULT 'strategy_native',
  base_currency TEXT NOT NULL DEFAULT 'USD',
  benchmark_symbol TEXT,
  inception_date DATE NOT NULL,
  mandate TEXT NOT NULL DEFAULT '',
  latest_revision INTEGER,
  active_revision INTEGER,
  active_portfolio_name TEXT REFERENCES core.portfolio_definitions(name) ON DELETE SET NULL,
  active_portfolio_version INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_materialized_at TIMESTAMPTZ,
  open_alert_count INTEGER NOT NULL DEFAULT 0,
  CHECK (status IN ('draft', 'active', 'archived')),
  CHECK (mode = 'internal_model_managed'),
  CHECK (accounting_depth = 'position_level'),
  CHECK (cadence_mode = 'strategy_native'),
  CHECK (char_length(base_currency) = 3)
);

CREATE TABLE IF NOT EXISTS core.portfolio_account_revisions (
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  mandate TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'draft',
  mode TEXT NOT NULL DEFAULT 'internal_model_managed',
  accounting_depth TEXT NOT NULL DEFAULT 'position_level',
  cadence_mode TEXT NOT NULL DEFAULT 'strategy_native',
  base_currency TEXT NOT NULL DEFAULT 'USD',
  benchmark_symbol TEXT,
  inception_date DATE NOT NULL,
  notes TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT,
  PRIMARY KEY (account_id, version),
  CHECK (status IN ('draft', 'active', 'archived')),
  CHECK (mode = 'internal_model_managed'),
  CHECK (accounting_depth = 'position_level'),
  CHECK (cadence_mode = 'strategy_native'),
  CHECK (char_length(base_currency) = 3)
);

CREATE TABLE IF NOT EXISTS core.portfolio_assignments (
  assignment_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  account_version INTEGER NOT NULL,
  portfolio_name TEXT NOT NULL,
  portfolio_version INTEGER NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE,
  status TEXT NOT NULL DEFAULT 'scheduled',
  notes TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_portfolio_assignment_account_revision
    FOREIGN KEY (account_id, account_version)
    REFERENCES core.portfolio_account_revisions(account_id, version)
    ON DELETE RESTRICT,
  CONSTRAINT fk_portfolio_assignment_portfolio_revision
    FOREIGN KEY (portfolio_name, portfolio_version)
    REFERENCES core.portfolio_revisions(portfolio_name, version)
    ON DELETE RESTRICT,
  CHECK (status IN ('scheduled', 'active', 'ended')),
  CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE TABLE IF NOT EXISTS core.portfolio_ledger_events (
  event_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  effective_at TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD',
  cash_amount DOUBLE PRECISION NOT NULL,
  symbol TEXT,
  quantity DOUBLE PRECISION,
  price DOUBLE PRECISION,
  commission DOUBLE PRECISION NOT NULL DEFAULT 0,
  slippage_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
  description TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (event_type IN ('opening_balance', 'deposit', 'withdrawal', 'fee', 'dividend', 'rebalance_buy', 'rebalance_sell', 'correction')),
  CHECK (char_length(currency) = 3)
);

CREATE TABLE IF NOT EXISTS core.portfolio_rebalance_proposals (
  proposal_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  as_of DATE NOT NULL,
  portfolio_name TEXT NOT NULL REFERENCES core.portfolio_definitions(name) ON DELETE RESTRICT,
  portfolio_version INTEGER NOT NULL,
  blocked BOOLEAN NOT NULL DEFAULT FALSE,
  warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  blocked_reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  estimated_cash_impact DOUBLE PRECISION NOT NULL DEFAULT 0,
  estimated_turnover DOUBLE PRECISION NOT NULL DEFAULT 0,
  trades_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at TIMESTAMPTZ,
  CONSTRAINT fk_portfolio_rebalance_portfolio_revision
    FOREIGN KEY (portfolio_name, portfolio_version)
    REFERENCES core.portfolio_revisions(portfolio_name, version)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS core.portfolio_snapshots (
  account_id TEXT PRIMARY KEY REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  account_name TEXT NOT NULL,
  as_of DATE NOT NULL,
  nav DOUBLE PRECISION NOT NULL,
  cash DOUBLE PRECISION NOT NULL,
  gross_exposure DOUBLE PRECISION NOT NULL,
  net_exposure DOUBLE PRECISION NOT NULL,
  since_inception_pnl DOUBLE PRECISION NOT NULL,
  since_inception_return DOUBLE PRECISION NOT NULL,
  current_drawdown DOUBLE PRECISION NOT NULL,
  max_drawdown DOUBLE PRECISION,
  open_alert_count INTEGER NOT NULL DEFAULT 0,
  active_assignment_json JSONB,
  freshness_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  slices_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.portfolio_history (
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  as_of DATE NOT NULL,
  nav DOUBLE PRECISION NOT NULL,
  cash DOUBLE PRECISION NOT NULL,
  gross_exposure DOUBLE PRECISION NOT NULL,
  net_exposure DOUBLE PRECISION NOT NULL,
  period_pnl DOUBLE PRECISION,
  period_return DOUBLE PRECISION,
  cumulative_pnl DOUBLE PRECISION,
  cumulative_return DOUBLE PRECISION,
  drawdown DOUBLE PRECISION,
  turnover DOUBLE PRECISION,
  cost_drag_bps DOUBLE PRECISION,
  PRIMARY KEY (account_id, as_of)
);

CREATE TABLE IF NOT EXISTS core.portfolio_positions (
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  as_of DATE NOT NULL,
  symbol TEXT NOT NULL,
  quantity DOUBLE PRECISION NOT NULL,
  market_value DOUBLE PRECISION NOT NULL,
  weight DOUBLE PRECISION NOT NULL,
  average_cost DOUBLE PRECISION,
  last_price DOUBLE PRECISION,
  unrealized_pnl DOUBLE PRECISION,
  realized_pnl DOUBLE PRECISION,
  contributors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  PRIMARY KEY (account_id, as_of, symbol)
);

CREATE TABLE IF NOT EXISTS core.portfolio_alerts (
  alert_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  severity TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  code TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  detected_at TIMESTAMPTZ NOT NULL,
  acknowledged_at TIMESTAMPTZ,
  acknowledged_by TEXT,
  resolved_at TIMESTAMPTZ,
  as_of DATE,
  CHECK (severity IN ('info', 'warning', 'critical')),
  CHECK (status IN ('open', 'acknowledged', 'resolved'))
);

CREATE TABLE IF NOT EXISTS core.portfolio_materialization_state (
  account_id TEXT PRIMARY KEY REFERENCES core.portfolio_accounts(account_id) ON DELETE CASCADE,
  dependency_fingerprint TEXT,
  dependency_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'dirty',
  claim_token TEXT,
  claimed_by TEXT,
  claimed_at TIMESTAMPTZ,
  claim_expires_at TIMESTAMPTZ,
  last_materialized_at TIMESTAMPTZ,
  last_snapshot_as_of DATE,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('dirty', 'claimed', 'failed', 'idle'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_core_portfolio_assignments_active_account
  ON core.portfolio_assignments(account_id)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_core_portfolio_assignments_account_effective
  ON core.portfolio_assignments(account_id, effective_from DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_portfolio_ledger_events_account_effective_at
  ON core.portfolio_ledger_events(account_id, effective_at DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_portfolio_rebalance_proposals_account_created_at
  ON core.portfolio_rebalance_proposals(account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_portfolio_history_account_as_of
  ON core.portfolio_history(account_id, as_of DESC);

CREATE INDEX IF NOT EXISTS idx_core_portfolio_positions_account_as_of_market_value
  ON core.portfolio_positions(account_id, as_of DESC, market_value DESC);

CREATE INDEX IF NOT EXISTS idx_core_portfolio_alerts_account_status_detected_at
  ON core.portfolio_alerts(account_id, status, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_portfolio_materialization_state_status_updated_at
  ON core.portfolio_materialization_state(status, updated_at ASC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_definitions TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_revisions TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_accounts TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_account_revisions TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_assignments TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_ledger_events TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_rebalance_proposals TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_snapshots TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_history TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_positions TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_alerts TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.portfolio_materialization_state TO backtest_service;
  END IF;
END $$;

COMMIT;
