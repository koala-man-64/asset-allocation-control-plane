from __future__ import annotations

from pathlib import Path


def test_gold_market_corporate_actions_migration_adds_columns_and_refreshes_view() -> None:
    migration_path = (
        Path(__file__).resolve().parents[2]
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0040_add_gold_market_corporate_action_columns.sql"
    )

    sql = migration_path.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS dividend_amount DOUBLE PRECISION" in sql
    assert "ADD COLUMN IF NOT EXISTS split_coefficient DOUBLE PRECISION" in sql
    assert "ADD COLUMN IF NOT EXISTS is_dividend_day INTEGER" in sql
    assert "ADD COLUMN IF NOT EXISTS is_split_day INTEGER" in sql
    assert "CREATE OR REPLACE VIEW gold.market_data_by_date AS" in sql
