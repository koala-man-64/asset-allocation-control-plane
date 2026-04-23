from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Date, MetaData, Numeric, String, Table

from api.service.app import create_app
from api.service.auth import AuthContext
from api.service.data_discovery import _GoldLookupFieldMetadata
from core.strategy_engine.universe import UniverseColumnSpec, UniverseTableSpec
from tests.api._client import get_test_client


def _configure_discovery_env(monkeypatch: pytest.MonkeyPatch, *, dsn: str = "postgresql://user:pass@localhost/db") -> None:
    monkeypatch.setenv("POSTGRES_DSN", dsn)
    monkeypatch.setenv("DATA_DISCOVERY_CACHE_TTL_SECONDS", "0")


def _configure_deployed_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("DATA_DISCOVERY_CACHE_TTL_SECONDS", "0")


def _build_gold_table_specs() -> dict[str, UniverseTableSpec]:
    return {
        "market_data": UniverseTableSpec(
            schema="gold",
            name="market_data",
            as_of_column="date",
            columns={
                "symbol": UniverseColumnSpec(name="symbol", data_type="text", value_kind="string", operators=()),
                "date": UniverseColumnSpec(name="date", data_type="date", value_kind="date", operators=()),
                "close": UniverseColumnSpec(name="close", data_type="double precision", value_kind="number", operators=()),
            },
        ),
        "finance_data": UniverseTableSpec(
            schema="gold",
            name="finance_data",
            as_of_column="date",
            columns={
                "symbol": UniverseColumnSpec(name="symbol", data_type="text", value_kind="string", operators=()),
                "date": UniverseColumnSpec(name="date", data_type="date", value_kind="date", operators=()),
                "revenue": UniverseColumnSpec(name="revenue", data_type="bigint", value_kind="number", operators=()),
            },
        ),
    }


def _mock_discovery_inspector() -> MagicMock:
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = [
        "public",
        "information_schema",
        "core",
        "gold",
        "platinum",
    ]

    table_names = {
        "core": ["symbols"],
        "gold": ["market_data", "market_data_backup", "finance_data"],
        "platinum": ["portfolio_scores"],
    }
    primary_keys = {
        ("core", "symbols"): ["symbol"],
        ("gold", "market_data"): ["symbol", "date"],
        ("gold", "finance_data"): ["symbol", "date"],
        ("platinum", "portfolio_scores"): ["symbol", "as_of_date"],
    }
    columns = {
        ("core", "symbols"): [
            {"name": "symbol", "type": "TEXT", "nullable": False},
            {"name": "updated_at", "type": "TIMESTAMP", "nullable": False},
        ],
        ("gold", "market_data"): [
            {"name": "symbol", "type": "TEXT", "nullable": False},
            {"name": "date", "type": "DATE", "nullable": False},
            {"name": "close", "type": "DOUBLE PRECISION", "nullable": True},
        ],
        ("gold", "finance_data"): [
            {"name": "symbol", "type": "TEXT", "nullable": False},
            {"name": "date", "type": "DATE", "nullable": False},
            {"name": "revenue", "type": "BIGINT", "nullable": True},
        ],
        ("platinum", "portfolio_scores"): [
            {"name": "symbol", "type": "TEXT", "nullable": False},
            {"name": "as_of_date", "type": "DATE", "nullable": False},
            {"name": "score", "type": "DOUBLE PRECISION", "nullable": True},
        ],
    }

    mock_inspector.get_table_names.side_effect = lambda schema=None: table_names.get(schema, [])
    mock_inspector.get_pk_constraint.side_effect = (
        lambda table_name, schema=None: {"constrained_columns": primary_keys.get((schema, table_name), [])}
    )
    mock_inspector.get_columns.side_effect = lambda table_name, schema=None: columns.get((schema, table_name), [])
    return mock_inspector


@pytest.mark.asyncio
async def test_discovery_catalog_filters_hidden_and_noncanonical_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = _mock_discovery_inspector()

    _configure_discovery_env(monkeypatch)

    with patch("api.service.data_discovery.create_engine", return_value=mock_engine):
        with patch("api.service.data_discovery.inspect", return_value=mock_inspector):
            with patch("api.service.data_discovery._load_gold_table_specs_safely", return_value=(_build_gold_table_specs(), [])):
                with patch("api.service.data_discovery._load_gold_lookup_field_map_safely", return_value=({}, [])):
                    with patch("api.service.data_discovery._load_postgres_column_descriptions", return_value={}):
                        with patch("api.service.data_discovery._load_postgres_table_description", return_value=None):
                            with patch("api.service.data_discovery._load_gold_freshness", return_value=(None, [])):
                                app = create_app()
                                async with get_test_client(app) as client:
                                    resp = await client.get("/api/system/discovery/catalog")

    assert resp.status_code == 200
    datasets = {
        (item["scope"], item["schemaName"], item["tableName"])
        for item in resp.json()["datasets"]
    }
    assert datasets == {
        ("control_plane", "core", "symbols"),
        ("control_plane", "platinum", "portfolio_scores"),
        ("gold", "gold", "finance_data"),
        ("gold", "gold", "market_data"),
    }


@pytest.mark.asyncio
async def test_discovery_dataset_detail_returns_enriched_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol", "date"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "date", "type": "DATE", "nullable": False},
        {"name": "close", "type": "DOUBLE PRECISION", "nullable": True},
    ]

    _configure_discovery_env(monkeypatch)

    with patch("api.service.data_discovery.create_engine", return_value=mock_engine):
        with patch("api.service.data_discovery.inspect", return_value=mock_inspector):
            with patch("api.service.data_discovery._load_gold_table_specs_safely", return_value=(_build_gold_table_specs(), [])):
                with patch(
                    "api.service.data_discovery._load_gold_lookup_field_map_safely",
                    return_value=(
                        {
                            "close": _GoldLookupFieldMetadata(
                                description="Official adjusted close price.",
                                calculation_type="derived_python",
                                calculation_dependencies=("symbol", "date"),
                                calculation_notes="Adjusted for splits and dividends.",
                            )
                        },
                        [],
                    ),
                ):
                    with patch(
                        "api.service.data_discovery._load_postgres_column_descriptions",
                        return_value={"close": "Physical close description."},
                    ):
                        with patch("api.service.data_discovery._load_postgres_table_description", return_value="Market data table."):
                            with patch("api.service.data_discovery._load_gold_freshness", return_value=(None, [])):
                                app = create_app()
                                async with get_test_client(app) as client:
                                    resp = await client.get("/api/system/discovery/datasets/gold/market_data")

    assert resp.status_code == 200
    payload = resp.json()
    close_field = next(field for field in payload["fields"] if field["name"] == "close")
    assert payload["description"] == "Market data table."
    assert payload["defaultSort"] == [
        {"column": "date", "direction": "desc"},
        {"column": "symbol", "direction": "asc"},
    ]
    assert close_field["description"] == "Official adjusted close price."
    assert close_field["valueKind"] == "number"
    assert close_field["logicalFieldId"] == "market.close"
    assert close_field["calculationType"] == "derived_python"
    assert close_field["calculationDependencies"] == ["symbol", "date"]
    assert close_field["calculationNotes"] == "Adjusted for splits and dividends."


@pytest.mark.asyncio
async def test_discovery_dataset_detail_degrades_gracefully_when_semantics_are_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol", "date"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "date", "type": "DATE", "nullable": False},
        {"name": "close", "type": "DOUBLE PRECISION", "nullable": True},
    ]

    _configure_discovery_env(monkeypatch)

    with patch("api.service.data_discovery.create_engine", return_value=mock_engine):
        with patch("api.service.data_discovery.inspect", return_value=mock_inspector):
            with patch("api.service.data_discovery._load_gold_table_specs_safely", return_value=(_build_gold_table_specs(), [])):
                with patch(
                    "api.service.data_discovery._load_gold_lookup_field_map_safely",
                    return_value=({}, ["Gold semantic metadata unavailable for gold.market_data: RuntimeError: boom"]),
                ):
                    with patch(
                        "api.service.data_discovery._load_postgres_column_descriptions",
                        return_value={"close": "Physical close description."},
                    ):
                        with patch("api.service.data_discovery._load_postgres_table_description", return_value=None):
                            with patch("api.service.data_discovery._load_gold_freshness", return_value=(None, [])):
                                app = create_app()
                                async with get_test_client(app) as client:
                                    resp = await client.get("/api/system/discovery/datasets/gold/market_data")

    assert resp.status_code == 200
    payload = resp.json()
    close_field = next(field for field in payload["fields"] if field["name"] == "close")
    assert close_field["description"] == "Physical close description."
    assert "Gold semantic metadata unavailable for gold.market_data: RuntimeError: boom" in payload["warnings"]


@pytest.mark.asyncio
async def test_discovery_sample_preview_returns_rows_and_sort(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol", "date"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "date", "type": "DATE", "nullable": False},
        {"name": "close", "type": "DOUBLE PRECISION", "nullable": True},
    ]

    reflected_table = Table(
        "market_data",
        MetaData(),
        Column("symbol", String),
        Column("date", Date),
        Column("close", Numeric),
        schema="gold",
    )
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [
        {"symbol": "AAPL", "date": "2026-04-22", "close": 185.0},
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_connect = MagicMock()
    mock_connect.__enter__.return_value = mock_conn
    mock_connect.__exit__.return_value = False
    mock_engine.connect.return_value = mock_connect

    _configure_discovery_env(monkeypatch)

    with patch("api.service.data_discovery.create_engine", return_value=mock_engine):
        with patch("api.service.data_discovery.inspect", return_value=mock_inspector):
            with patch("api.service.data_discovery._reflect_table", return_value=reflected_table):
                with patch("api.service.data_discovery._load_gold_table_specs_safely", return_value=(_build_gold_table_specs(), [])):
                    with patch("api.service.data_discovery._load_gold_lookup_field_map_safely", return_value=({}, [])):
                        with patch("api.service.data_discovery._load_postgres_column_descriptions", return_value={}):
                            with patch("api.service.data_discovery._load_postgres_table_description", return_value=None):
                                with patch("api.service.data_discovery._load_gold_freshness", return_value=(None, [])):
                                    app = create_app()
                                    async with get_test_client(app) as client:
                                        resp = await client.get(
                                            "/api/system/discovery/datasets/gold/market_data/sample?limit=1"
                                        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["sortApplied"] == [
        {"column": "date", "direction": "desc"},
        {"column": "symbol", "direction": "asc"},
    ]
    assert payload["rows"] == [{"symbol": "AAPL", "date": "2026-04-22", "close": 185.0}]


@pytest.mark.asyncio
async def test_discovery_sample_limit_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_DISCOVERY_SAMPLE_MAX_LIMIT", "2")
    monkeypatch.setenv("DATA_DISCOVERY_CACHE_TTL_SECONDS", "0")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/discovery/datasets/gold/market_data/sample?limit=3")

    assert resp.status_code == 400
    assert resp.json() == {"detail": "Sample limit must be <= 2."}


@pytest.mark.asyncio
async def test_discovery_missing_dataset_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]

    _configure_discovery_env(monkeypatch)

    with patch("api.service.data_discovery.create_engine", return_value=mock_engine):
        with patch("api.service.data_discovery.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/discovery/datasets/gold/missing_table")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_discovery_missing_postgres_dsn_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setenv("DATA_DISCOVERY_CACHE_TTL_SECONDS", "0")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/discovery/catalog")

    assert resp.status_code == 503
    assert resp.json() == {"detail": "Postgres is not configured (POSTGRES_DSN)."}


@pytest.mark.asyncio
async def test_discovery_catalog_requires_read_role_when_deployed(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_deployed_auth(monkeypatch)

    app = create_app()
    monkeypatch.setattr(
        app.state.auth,
        "authenticate_headers",
        lambda _headers: AuthContext(mode="oidc", subject="user-1", claims={"roles": ["AssetAllocation.Access"]}),
    )

    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/system/discovery/catalog",
            headers={"Authorization": "Bearer token"},
        )

    assert resp.status_code == 403
    assert resp.json() == {"detail": "Missing required roles: AssetAllocation.DataDiscovery.Read."}
