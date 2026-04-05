from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_move_public_tables_to_core_handles_prior_public_symbols_shape() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0016_move_public_tables_to_core.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "IF to_regclass('public.symbols') IS NOT NULL THEN" in text, (
        "0016 must only move public.symbols when the prior table still exists"
    )
    assert "information_schema.columns" in text, (
        "0016 must inspect prior public.symbols columns before referencing them"
    )
    assert "column_name = 'source_alpha_vantage'" in text, (
        "0016 must detect the prior source_alpha_vantage column"
    )
    assert "column_name = 'source_alphavantage'" in text, (
        "0016 must tolerate environments where source_alphavantage exists instead"
    )
    assert "EXECUTE format($symbols_move$" in text, (
        "0016 must build the public.symbols move dynamically to avoid invalid column references"
    )
    assert "COALESCE(source_alpha_vantage, source_alphavantage, FALSE)" not in text, (
        "0016 must not statically reference both previous source columns in the SELECT list"
    )


def test_apply_postgres_migrations_streams_file_inputs_to_docker_psql() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "data" / "apply_postgres_migrations.ps1"
    text = script.read_text(encoding="utf-8")

    assert '$dockerArgs += "-f"' in text, (
        "apply_postgres_migrations must preserve -f when rewriting Docker psql args"
    )
    assert '$dockerArgs += "-"' in text, (
        "apply_postgres_migrations must rewrite Docker file inputs to stdin"
    )
    assert 'Get-Content -Path $dockerStdinPath -Raw -Encoding UTF8 | & docker @cmd' in text, (
        "apply_postgres_migrations must stream migration SQL into dockerized psql"
    )


def test_gold_sync_migration_rebuilds_incompatible_gold_tables_without_backup_renames() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0019_gold_postgres_sync.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "DROP TABLE gold.market_data;" in text
    assert "DROP TABLE gold.finance_data;" in text
    assert "DROP TABLE gold.earnings_data;" in text
    assert "DROP TABLE gold.price_target_data;" in text
    assert "ALTER TABLE gold.market_data RENAME TO" not in text
    assert "_0006" not in text


def test_cleanup_migration_drops_noncanonical_gold_tables() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0029_drop_noncanonical_gold_tables.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "FROM pg_tables" in text
    assert "schemaname = 'gold'" in text
    assert "tablename NOT IN (" in text
    assert "DROP TABLE IF EXISTS gold.%I" in text


def test_drop_forward_pe_migration_rebuilds_finance_view_before_column_drop() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0028_drop_forward_pe_from_gold_finance.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "DROP VIEW IF EXISTS gold.finance_data_by_date;" in text
    assert "ALTER TABLE IF EXISTS gold.finance_data" in text
    assert "DROP COLUMN IF EXISTS forward_pe;" in text
    assert "CREATE OR REPLACE VIEW gold.finance_data_by_date AS" in text
    assert "SELECT * FROM gold.finance_data;" in text
    assert "GRANT SELECT ON TABLE gold.finance_data_by_date TO backtest_service;" in text


def test_alpha_vantage_source_unification_migration_drops_legacy_alias_column() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0030_unify_alpha_vantage_symbol_source.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_alpha_vantage BOOLEAN;" in text
    assert "column_name = 'source_alphavantage'" in text
    assert "COALESCE(source_alpha_vantage, source_alphavantage, FALSE)" in text
    assert "DROP COLUMN source_alphavantage" in text


def test_gold_column_lookup_migration_defines_constraints_and_indexes() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0031_gold_column_lookup.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS gold.column_lookup" in text
    assert "PRIMARY KEY (schema_name, table_name, column_name)" in text
    assert "CHECK (schema_name = 'gold')" in text
    assert "calculation_type IN ('source', 'derived_sql', 'derived_python', 'external', 'manual')" in text
    assert "status IN ('draft', 'reviewed', 'approved')" in text
    assert "idx_gold_column_lookup_schema_table" in text
    assert "idx_gold_column_lookup_status" in text
    assert "USING GIN (calculation_dependencies)" in text


def test_add_gold_finance_ratio_columns_migration_rebuilds_view_and_adds_ratio_columns() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0033_add_gold_finance_ratio_columns.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "DROP VIEW IF EXISTS gold.finance_data_by_date;" in text
    assert "ALTER TABLE IF EXISTS gold.finance_data" in text
    assert "ADD COLUMN IF NOT EXISTS price_to_book DOUBLE PRECISION" in text
    assert "ADD COLUMN IF NOT EXISTS current_ratio DOUBLE PRECISION" in text
    assert "ADD COLUMN IF NOT EXISTS free_cash_flow DOUBLE PRECISION" in text
    assert "CREATE OR REPLACE VIEW gold.finance_data_by_date AS" in text
    assert "SELECT * FROM gold.finance_data;" in text
    assert "GRANT SELECT ON TABLE gold.finance_data_by_date TO backtest_service;" in text


def test_provision_azure_postgres_uses_valid_do_block_sql_for_app_user_creation() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_postgres.ps1"
    text = script.read_text(encoding="utf-8")

    assert "function ConvertTo-SqlLiteral" in text, (
        "provision_azure_postgres must escape SQL string literals when building role SQL"
    )
    assert "DO \\$\\$" not in text, (
        "provision_azure_postgres must not emit backslash-escaped DO block delimiters"
    )
    assert "DO $$" in text, (
        "provision_azure_postgres must emit literal DO block delimiters for psql"
    )
    assert "$sqlTemplate = @'" in text, (
        "provision_azure_postgres should build the role-creation SQL from a single-quoted here-string template"
    )


def test_provision_azure_postgres_uses_unqualified_index_names_in_supporting_index_sql() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_postgres.ps1"
    text = script.read_text(encoding="utf-8")

    assert "CREATE INDEX IF NOT EXISTS platinum.idx_" not in text, (
        "provision_azure_postgres must not schema-qualify index names in CREATE INDEX IF NOT EXISTS statements"
    )
    assert "CREATE INDEX IF NOT EXISTS public.idx_" not in text, (
        "provision_azure_postgres must not schema-qualify public index names in CREATE INDEX IF NOT EXISTS statements"
    )
    assert "CREATE INDEX IF NOT EXISTS idx_platinum_strategies_type ON platinum.strategies(type);" in text
    assert "CREATE INDEX IF NOT EXISTS idx_public_strategies_type ON public.strategies(type);" in text


def test_provision_azure_postgres_auto_falls_back_to_dockerized_psql_when_local_psql_is_missing() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_postgres.ps1"
    text = script.read_text(encoding="utf-8")

    assert "Local psql is not installed; falling back to Dockerized psql." in text, (
        "provision_azure_postgres should automatically switch to Dockerized psql when local psql is unavailable"
    )
    assert "$UseDockerPsql = $true" in text, (
        "provision_azure_postgres must enable UseDockerPsql after detecting docker fallback"
    )
