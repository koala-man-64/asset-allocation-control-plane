BEGIN;

ALTER TABLE IF EXISTS core.portfolio_revisions
  ADD COLUMN IF NOT EXISTS allocation_mode TEXT NOT NULL DEFAULT 'percent',
  ADD COLUMN IF NOT EXISTS allocatable_capital DOUBLE PRECISION;

DO $$
BEGIN
  IF to_regclass('core.portfolio_revisions') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_core_portfolio_revisions_allocation_mode'
      AND conrelid = to_regclass('core.portfolio_revisions')
  ) THEN
    ALTER TABLE core.portfolio_revisions
      ADD CONSTRAINT ck_core_portfolio_revisions_allocation_mode
      CHECK (allocation_mode IN ('percent', 'notional_base_ccy'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_core_portfolio_revisions_allocatable_capital'
      AND conrelid = to_regclass('core.portfolio_revisions')
  ) THEN
    ALTER TABLE core.portfolio_revisions
      ADD CONSTRAINT ck_core_portfolio_revisions_allocatable_capital
      CHECK (
        allocatable_capital IS NULL
        OR allocatable_capital > 0
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_core_portfolio_revisions_notional_capital'
      AND conrelid = to_regclass('core.portfolio_revisions')
  ) THEN
    ALTER TABLE core.portfolio_revisions
      ADD CONSTRAINT ck_core_portfolio_revisions_notional_capital
      CHECK (
        allocation_mode = 'percent'
        OR allocatable_capital IS NOT NULL
      );
  END IF;
END $$;

COMMIT;
