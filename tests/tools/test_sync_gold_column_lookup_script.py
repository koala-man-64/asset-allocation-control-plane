from __future__ import annotations

import pytest

from scripts.ops.data import sync_gold_column_lookup as sync


def test_build_lookup_row_preserves_existing_curated_description_by_default() -> None:
    row = sync._build_lookup_row(  # type: ignore[attr-defined]
        live_row={
            "schema_name": "gold",
            "table_name": "market_data",
            "column_name": "close",
            "data_type": "double precision",
            "is_nullable": True,
        },
        existing={
            "description": "Existing curated description",
            "calculation_type": "source",
            "calculation_notes": "Existing note",
            "calculation_expression": None,
            "calculation_dependencies": [],
            "source_job": "tasks.market_data.gold_market_data",
            "status": "reviewed",
        },
        seed={
            "description": "Seed description should not override",
            "calculation_type": "derived_python",
        },
        updated_by="test",
        force_metadata=False,
    )

    assert row["description"] == "Existing curated description"
    assert row["calculation_type"] == "source"
    assert row["status"] == "reviewed"


def test_build_lookup_row_uses_placeholder_for_new_columns_without_seed() -> None:
    row = sync._build_lookup_row(  # type: ignore[attr-defined]
        live_row={
            "schema_name": "gold",
            "table_name": "market_data",
            "column_name": "new_feature",
            "data_type": "double precision",
            "is_nullable": True,
        },
        existing=None,
        seed=None,
        updated_by="test",
        force_metadata=False,
    )

    assert row["description"].startswith("TODO: Describe gold.market_data.new_feature")
    assert row["status"] == "draft"


def test_build_lookup_row_rejects_approved_placeholder() -> None:
    with pytest.raises(sync.PostgresError, match="Approved metadata cannot use placeholder description"):
        sync._build_lookup_row(  # type: ignore[attr-defined]
            live_row={
                "schema_name": "gold",
                "table_name": "market_data",
                "column_name": "close",
                "data_type": "double precision",
                "is_nullable": True,
            },
            existing={
                "description": "TODO: Describe gold.market_data.close.",
                "calculation_type": "source",
                "calculation_notes": None,
                "calculation_expression": None,
                "calculation_dependencies": [],
                "source_job": "tasks.market_data.gold_market_data",
                "status": "approved",
            },
            seed=None,
            updated_by="test",
            force_metadata=False,
        )


def test_build_lookup_row_uses_seeded_liquidity_metadata_for_new_columns() -> None:
    row = sync._build_lookup_row(  # type: ignore[attr-defined]
        live_row={
            "schema_name": "gold",
            "table_name": "market_data",
            "column_name": "liquidity_stress_score",
            "data_type": "double precision",
            "is_nullable": True,
        },
        existing=None,
        seed={
            "description": "Composite daily liquidity gate.",
            "calculation_type": "derived_python",
            "calculation_expression": "amihud_z_252d - dollar_volume_z_252d + abs(gap_atr)",
            "calculation_dependencies": ["amihud_z_252d", "dollar_volume_z_252d", "gap_atr"],
            "source_job": "tasks.market_data.gold_market_data",
            "status": "reviewed",
        },
        updated_by="test",
        force_metadata=False,
    )

    assert row["description"] == "Composite daily liquidity gate."
    assert row["calculation_type"] == "derived_python"
    assert row["calculation_dependencies"] == ["amihud_z_252d", "dollar_volume_z_252d", "gap_atr"]
    assert row["status"] == "reviewed"
