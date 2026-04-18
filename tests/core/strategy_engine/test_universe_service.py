from __future__ import annotations

from dataclasses import dataclass

import pytest

from core.strategy_engine import universe as universe_service


@dataclass
class _FakeConn:
    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None


def test_build_table_specs_keeps_only_gold_tables_with_symbol_and_as_of() -> None:
    specs = universe_service._build_table_specs(
        [
            ("market_data", "symbol", "text", "text"),
            ("market_data", "date", "date", "date"),
            ("market_data", "close", "double precision", "float8"),
            ("market_data", "metadata", "jsonb", "jsonb"),
            ("finance_data", "symbol", "text", "text"),
            ("finance_data", "obs_date", "date", "date"),
            ("finance_data", "f_score", "integer", "int4"),
            ("orphan_table", "symbol", "text", "text"),
            ("orphan_table", "close", "double precision", "float8"),
        ]
    )

    assert sorted(specs.keys()) == ["finance_data", "market_data"]
    assert specs["market_data"].as_of_column == "date"
    assert "close" in specs["market_data"].columns
    assert "metadata" not in specs["market_data"].columns
    assert specs["finance_data"].as_of_column == "obs_date"


def test_build_table_specs_marks_timestamp_tables_as_intraday() -> None:
    specs = universe_service._build_table_specs(
        [
            ("intraday_features", "symbol", "text", "text"),
            ("intraday_features", "as_of_ts", "timestamp with time zone", "timestamptz"),
            ("intraday_features", "signal_strength", "double precision", "float8"),
        ]
    )

    assert specs["intraday_features"].as_of_column == "as_of_ts"
    assert specs["intraday_features"].as_of_kind == "intraday"


def test_catalog_table_name_filter_excludes_noncanonical_gold_tables() -> None:
    assert universe_service._is_catalog_table_name("market_data")
    assert not universe_service._is_catalog_table_name("market_data_backup")
    assert not universe_service._is_catalog_table_name("market_data_by_date")


def test_list_gold_universe_catalog_returns_public_fields(monkeypatch) -> None:
    specs = universe_service._build_table_specs(
        [
            ("gold", "market_data", "symbol", "text", "text"),
            ("gold", "market_data", "date", "date", "date"),
            ("gold", "market_data", "close", "double precision", "float8"),
            ("gold", "market_data", "return_20d", "double precision", "float8"),
            ("core", "symbols", "symbol", "text", "text"),
            ("core", "symbols", "updated_at", "timestamp with time zone", "timestamptz"),
            ("core", "symbols", "status", "text", "text"),
        ]
    )

    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    catalog = universe_service.list_gold_universe_catalog("postgresql://test")

    assert catalog["source"] == "postgres_gold"
    assert [field["id"] for field in catalog["fields"]] == [
        "market.close",
        "market.timestamp",
        "market.trade_date",
        "returns.return_20d",
        "security.is_active",
    ]
    assert catalog["fields"][0]["label"] == "Close Price"
    assert catalog["fields"][4]["valueKind"] == "boolean"
    assert all("label" in field and "valueKind" in field and "operators" in field for field in catalog["fields"])


def test_preview_gold_universe_combines_nested_and_or_groups(monkeypatch) -> None:
    universe = {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 10,
                },
                {
                    "kind": "group",
                    "operator": "or",
                    "clauses": [
                        {
                            "kind": "condition",
                            "field": "quality.piotroski_f_score",
                            "operator": "gte",
                            "value": 7,
                        },
                        {
                            "kind": "condition",
                            "field": "earnings.surprise_pct",
                            "operator": "gt",
                            "value": 0,
                        },
                    ],
                },
            ],
        },
    }

    specs = {
        "market_data": universe_service.UniverseTableSpec(
            schema="gold",
            name="market_data",
            as_of_column="date",
            columns={
                "close": universe_service.UniverseColumnSpec(
                    name="close",
                    data_type="double precision",
                    value_kind="number",
                    operators=universe_service._NUMBER_OPERATORS,
                )
            },
        ),
        "finance_data": universe_service.UniverseTableSpec(
            schema="gold",
            name="finance_data",
            as_of_column="obs_date",
            columns={
                "piotroski_f_score": universe_service.UniverseColumnSpec(
                    name="piotroski_f_score",
                    data_type="integer",
                    value_kind="number",
                    operators=universe_service._NUMBER_OPERATORS,
                )
            },
        ),
        "earnings_data": universe_service.UniverseTableSpec(
            schema="gold",
            name="earnings_data",
            as_of_column="date",
            columns={
                "surprise_pct": universe_service.UniverseColumnSpec(
                    name="surprise_pct",
                    data_type="double precision",
                    value_kind="number",
                    operators=universe_service._NUMBER_OPERATORS,
                )
            },
        ),
    }

    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    monkeypatch.setattr(universe_service, "connect", lambda _dsn: _FakeConn())

    condition_results = {
        "market.close": {"AAPL", "MSFT"},
        "quality.piotroski_f_score": {"AAPL"},
        "earnings.surprise_pct": {"MSFT", "NVDA"},
    }

    monkeypatch.setattr(
        universe_service,
        "_fetch_condition_symbols",
        lambda _conn, _table_spec, field_spec, _condition: set(
            condition_results[field_spec.field_id]
        ),
    )

    preview = universe_service.preview_gold_universe("postgresql://test", universe, sample_limit=2)

    assert preview["symbolCount"] == 2
    assert preview["sampleSymbols"] == ["AAPL", "MSFT"]
    assert preview["fieldsUsed"] == [
        "earnings.surprise_pct",
        "market.close",
        "quality.piotroski_f_score",
    ]
    assert preview["warnings"] == []


def test_preview_gold_universe_warns_when_no_symbols_match(monkeypatch) -> None:
    universe = {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 10,
                }
            ],
        },
    }

    specs = {
        "market_data": universe_service.UniverseTableSpec(
            schema="gold",
            name="market_data",
            as_of_column="date",
            columns={
                "close": universe_service.UniverseColumnSpec(
                    name="close",
                    data_type="double precision",
                    value_kind="number",
                    operators=universe_service._NUMBER_OPERATORS,
                )
            },
        )
    }

    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    monkeypatch.setattr(universe_service, "connect", lambda _dsn: _FakeConn())
    monkeypatch.setattr(universe_service, "_fetch_condition_symbols", lambda *_args, **_kwargs: set())

    preview = universe_service.preview_gold_universe("postgresql://test", universe)

    assert preview["symbolCount"] == 0
    assert preview["sampleSymbols"] == []
    assert preview["warnings"] == ["Universe preview matched zero symbols."]


def test_preview_gold_universe_rejects_unknown_field_id() -> None:
    universe = {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "unknown.metric",
                    "operator": "eq",
                    "value": 1,
                }
            ],
        },
    }

    try:
        universe_service.preview_gold_universe("postgresql://test", universe)
        raise AssertionError("Expected preview_gold_universe to fail.")
    except ValueError as exc:
        assert "Unknown universe field 'unknown.metric'." in str(exc)


def test_validate_universe_definition_support_rejects_unavailable_contract_field(monkeypatch) -> None:
    universe = {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "returns.return_126d",
                    "operator": "gt",
                    "value": 0,
                }
            ],
        },
    }
    specs = universe_service._build_table_specs(
        [
            ("gold", "market_data", "symbol", "text", "text"),
            ("gold", "market_data", "date", "date", "date"),
            ("gold", "market_data", "close", "double precision", "float8"),
            ("gold", "market_data", "return_20d", "double precision", "float8"),
        ]
    )

    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)

    with pytest.raises(ValueError, match="returns\\.return_126d"):
        universe_service.validate_universe_definition_support("postgresql://test", universe)
