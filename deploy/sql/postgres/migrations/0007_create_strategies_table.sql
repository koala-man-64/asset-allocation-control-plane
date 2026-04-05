-- Up Migration
CREATE TABLE IF NOT EXISTS strategies (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    type TEXT NOT NULL DEFAULT 'configured', -- e.g., 'configured', 'custom_class'
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Down Migration
-- DROP TABLE IF EXISTS strategies;