BEGIN;

ALTER TABLE core.portfolio_accounts
  ADD COLUMN IF NOT EXISTS rebalance_cadence TEXT NOT NULL DEFAULT 'weekly';

ALTER TABLE core.portfolio_accounts
  ADD COLUMN IF NOT EXISTS rebalance_anchor TEXT NOT NULL DEFAULT 'Strategy native cadence';

ALTER TABLE core.portfolio_account_revisions
  ADD COLUMN IF NOT EXISTS rebalance_cadence TEXT NOT NULL DEFAULT 'weekly';

ALTER TABLE core.portfolio_account_revisions
  ADD COLUMN IF NOT EXISTS rebalance_anchor TEXT NOT NULL DEFAULT 'Strategy native cadence';

UPDATE core.portfolio_accounts
SET
  rebalance_cadence = COALESCE(NULLIF(TRIM(rebalance_cadence), ''), 'weekly'),
  rebalance_anchor = COALESCE(NULLIF(TRIM(rebalance_anchor), ''), 'Strategy native cadence');

UPDATE core.portfolio_account_revisions
SET
  rebalance_cadence = COALESCE(NULLIF(TRIM(rebalance_cadence), ''), 'weekly'),
  rebalance_anchor = COALESCE(NULLIF(TRIM(rebalance_anchor), ''), 'Strategy native cadence');

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_core_portfolio_accounts_rebalance_cadence'
  ) THEN
    ALTER TABLE core.portfolio_accounts
      ADD CONSTRAINT chk_core_portfolio_accounts_rebalance_cadence
      CHECK (rebalance_cadence IN ('daily', 'weekly', 'monthly'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_core_portfolio_account_revisions_rebalance_cadence'
  ) THEN
    ALTER TABLE core.portfolio_account_revisions
      ADD CONSTRAINT chk_core_portfolio_account_revisions_rebalance_cadence
      CHECK (rebalance_cadence IN ('daily', 'weekly', 'monthly'));
  END IF;
END $$;

COMMIT;
