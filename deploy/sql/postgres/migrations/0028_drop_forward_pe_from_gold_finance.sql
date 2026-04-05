BEGIN;

DROP VIEW IF EXISTS gold.finance_data_by_date;

ALTER TABLE IF EXISTS gold.finance_data
DROP COLUMN IF EXISTS forward_pe;

CREATE OR REPLACE VIEW gold.finance_data_by_date AS
SELECT * FROM gold.finance_data;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT ON TABLE gold.finance_data_by_date TO backtest_service;
  END IF;
END $$;

COMMIT;
