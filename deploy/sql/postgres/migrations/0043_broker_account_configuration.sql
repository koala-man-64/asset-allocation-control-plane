BEGIN;

CREATE TABLE IF NOT EXISTS core.broker_account_configurations (
  account_id TEXT PRIMARY KEY REFERENCES core.trade_accounts(account_id) ON DELETE CASCADE,
  configuration_version INTEGER NOT NULL DEFAULT 1,
  requested_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  effective_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  allocation_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT
);

CREATE TABLE IF NOT EXISTS core.broker_account_configuration_audit (
  audit_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES core.trade_accounts(account_id) ON DELETE CASCADE,
  category TEXT NOT NULL,
  outcome TEXT NOT NULL,
  actor TEXT,
  request_id TEXT,
  granted_roles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  summary TEXT NOT NULL DEFAULT '',
  before_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  after_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  denial_reason TEXT,
  audit_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (category IN ('trading_policy', 'allocation')),
  CHECK (outcome IN ('saved', 'denied', 'warning'))
);

CREATE INDEX IF NOT EXISTS idx_broker_account_configuration_audit_account_created_at
  ON core.broker_account_configuration_audit(account_id, created_at DESC);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.broker_account_configurations TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.broker_account_configuration_audit TO backtest_service;
  END IF;
END $$;

COMMIT;
