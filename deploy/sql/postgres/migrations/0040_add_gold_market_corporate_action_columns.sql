BEGIN;

ALTER TABLE gold.market_data
    ADD COLUMN IF NOT EXISTS dividend_amount DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS split_coefficient DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS is_dividend_day INTEGER,
    ADD COLUMN IF NOT EXISTS is_split_day INTEGER;

CREATE OR REPLACE VIEW gold.market_data_by_date AS
SELECT * FROM gold.market_data;

COMMIT;
