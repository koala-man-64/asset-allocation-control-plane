BEGIN;

ALTER TABLE gold.earnings_data
    ADD COLUMN IF NOT EXISTS next_earnings_date DATE,
    ADD COLUMN IF NOT EXISTS days_until_next_earnings INTEGER,
    ADD COLUMN IF NOT EXISTS next_earnings_estimate DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS next_earnings_time_of_day TEXT,
    ADD COLUMN IF NOT EXISTS next_earnings_fiscal_date_ending DATE,
    ADD COLUMN IF NOT EXISTS has_upcoming_earnings INTEGER,
    ADD COLUMN IF NOT EXISTS is_scheduled_earnings_day INTEGER;

COMMIT;
