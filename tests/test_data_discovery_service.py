from api.service.data_discovery import (
    PostgresColumnMetadata,
    TableMetadataResponse,
    _GoldLookupFieldMetadata,
    _build_data_discovery_fields,
    _load_gold_lookup_field_map_safely,
)
from core.strategy_engine.universe import UniverseColumnSpec, UniverseTableSpec


def test_build_data_discovery_fields_prefers_gold_lookup_metadata() -> None:
    metadata = TableMetadataResponse(
        schema_name="gold",
        table_name="market_data",
        primary_key=["symbol", "date"],
        can_edit=True,
        columns=[
            PostgresColumnMetadata(
                name="close",
                data_type="DOUBLE PRECISION",
                description="Physical close description.",
                nullable=True,
                primary_key=False,
                editable=True,
                edit_reason=None,
            )
        ],
    )
    gold_lookup = {
        "close": _GoldLookupFieldMetadata(
            description="Lookup close description.",
            calculation_type="derived_python",
            calculation_dependencies=("symbol", "date"),
            calculation_notes="Derived from the canonical sync job.",
        )
    }
    gold_table_spec = UniverseTableSpec(
        schema="gold",
        name="market_data",
        as_of_column="date",
        columns={
            "close": UniverseColumnSpec(
                name="close",
                data_type="double precision",
                value_kind="number",
                operators=(),
            )
        },
    )

    fields = _build_data_discovery_fields(
        schema_name="gold",
        table_name="market_data",
        metadata=metadata,
        gold_lookup_by_column=gold_lookup,
        gold_table_spec=gold_table_spec,
    )

    assert len(fields) == 1
    assert fields[0].description == "Lookup close description."
    assert fields[0].valueKind == "number"
    assert fields[0].logicalFieldId == "market.close"
    assert fields[0].calculationType == "derived_python"
    assert fields[0].calculationDependencies == ["symbol", "date"]


def test_load_gold_lookup_field_map_safely_returns_warning_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "api.service.data_discovery._load_gold_lookup_field_map",
        lambda _engine, *, table_name: (_ for _ in ()).throw(RuntimeError(f"boom:{table_name}")),
    )

    metadata, warnings = _load_gold_lookup_field_map_safely(
        object(),
        schema_name="gold",
        table_name="market_data",
    )

    assert metadata == {}
    assert warnings == [
        "Gold semantic metadata unavailable for gold.market_data: RuntimeError: boom:market_data"
    ]
