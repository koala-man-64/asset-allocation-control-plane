BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.column_lookup (
  schema_name TEXT NOT NULL DEFAULT 'gold',
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  data_type TEXT NOT NULL,
  description TEXT NOT NULL,
  is_nullable BOOLEAN NOT NULL DEFAULT TRUE,
  calculation_type TEXT NOT NULL DEFAULT 'source',
  calculation_notes TEXT,
  calculation_expression TEXT,
  calculation_dependencies TEXT[] NOT NULL DEFAULT '{}',
  source_job TEXT,
  status TEXT NOT NULL DEFAULT 'draft',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL DEFAULT CURRENT_USER,
  PRIMARY KEY (schema_name, table_name, column_name),
  CONSTRAINT chk_column_lookup_schema_gold CHECK (schema_name = 'gold'),
  CONSTRAINT chk_column_lookup_calc_type CHECK (
    calculation_type IN ('source', 'derived_sql', 'derived_python', 'external', 'manual')
  ),
  CONSTRAINT chk_column_lookup_status CHECK (
    status IN ('draft', 'reviewed', 'approved')
  )
);

CREATE INDEX IF NOT EXISTS idx_gold_column_lookup_schema_table
  ON gold.column_lookup(schema_name, table_name);

CREATE INDEX IF NOT EXISTS idx_gold_column_lookup_status
  ON gold.column_lookup(status);

CREATE INDEX IF NOT EXISTS idx_gold_column_lookup_calc_deps
  ON gold.column_lookup
  USING GIN (calculation_dependencies);

COMMIT;
