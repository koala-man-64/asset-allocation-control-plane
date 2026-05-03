BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.strategy_publication_reconcile_signals (
  job_key TEXT NOT NULL,
  source_fingerprint TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  published_at TIMESTAMPTZ,
  processed_at TIMESTAMPTZ,
  error TEXT,
  claim_token TEXT,
  claimed_by TEXT,
  claimed_at TIMESTAMPTZ,
  claim_expires_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (job_key, source_fingerprint),
  CHECK (job_key ~ '^[a-z0-9][a-z0-9-]*$'),
  CHECK (source_fingerprint <> ''),
  CHECK (status IN ('pending', 'processed', 'error')),
  CHECK (jsonb_typeof(metadata) = 'object'),
  CHECK (attempt_count >= 0)
);

ALTER TABLE core.strategy_publication_reconcile_signals
  ADD COLUMN IF NOT EXISTS claim_token TEXT,
  ADD COLUMN IF NOT EXISTS claimed_by TEXT,
  ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_core_strategy_publication_reconcile_status_updated
  ON core.strategy_publication_reconcile_signals(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_core_strategy_publication_reconcile_pending
  ON core.strategy_publication_reconcile_signals(next_attempt_at, updated_at)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_core_strategy_publication_reconcile_claims
  ON core.strategy_publication_reconcile_signals(claim_expires_at, updated_at)
  WHERE status = 'pending' AND claim_token IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_core_runs_canonical_target_fingerprint_active
  ON core.runs(canonical_target_id, canonical_fingerprint)
  WHERE canonical_target_id IS NOT NULL
    AND canonical_fingerprint IS NOT NULL
    AND status IN ('queued', 'running', 'completed');

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.strategy_publication_reconcile_signals TO backtest_service;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'strategy_publication_producer') THEN
    GRANT USAGE ON SCHEMA core TO strategy_publication_producer;
    GRANT SELECT, INSERT, UPDATE ON TABLE core.strategy_publication_reconcile_signals TO strategy_publication_producer;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'strategy_publication_reconciler') THEN
    GRANT USAGE ON SCHEMA core TO strategy_publication_reconciler;
    GRANT SELECT, UPDATE ON TABLE core.strategy_publication_reconcile_signals TO strategy_publication_reconciler;
  END IF;
END $$;

COMMIT;
