from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table
from sqlalchemy.dialects import postgresql

from api.service.app import create_app
from api.service.auth import AuthContext
from tests.api._auth import install_auth_stub
from tests.api._client import get_test_client


def _configure_discovery_env(monkeypatch: pytest.MonkeyPatch, *, dsn: str = "postgresql://user:pass@localhost/db") -> None:
    monkeypatch.setenv("POSTGRES_DSN", dsn)
    monkeypatch.setenv("DATA_DISCOVERY_CACHE_TTL_SECONDS", "0")


def _configure_deployed_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("DATA_DISCOVERY_CACHE_TTL_SECONDS", "0")


@pytest.mark.asyncio
async def test_list_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public", "information_schema", "core", "gold", "platinum"]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas")

    assert resp.status_code == 200
    assert resp.json() == ["core", "gold", "platinum"]


@pytest.mark.asyncio
async def test_list_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["core", "gold"]
    mock_inspector.get_table_names.return_value = ["table1", "table2"]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/core/tables")

    assert resp.status_code == 200
    assert resp.json() == ["table1", "table2"]


@pytest.mark.asyncio
async def test_list_tables_hides_noncanonical_gold_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = [
        "market_data",
        "market_data_backup",
        "finance_data",
    ]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/gold/tables")

    assert resp.status_code == 200
    assert resp.json() == ["finance_data", "market_data"]


@pytest.mark.asyncio
async def test_list_tables_404_schema_when_hidden_or_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["core", "gold"]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/public/tables")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_query_table_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["core"]
    mock_inspector.get_table_names.return_value = ["test_table"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    mock_inspector.get_columns.return_value = [
        {"name": "col1", "type": "INTEGER", "nullable": False},
        {"name": "col2", "type": "TEXT", "nullable": True},
    ]
    reflected_table = Table(
        "test_table",
        MetaData(),
        Column("col1", Integer),
        Column("col2", String),
        schema="core",
    )
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"},
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_connect = MagicMock()
    mock_connect.__enter__.return_value = mock_conn
    mock_connect.__exit__.return_value = False
    mock_engine.connect.return_value = mock_connect

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            with patch("api.endpoints.postgres._reflect_table", return_value=reflected_table):
                app = create_app()
                async with get_test_client(app) as client:
                    resp = await client.post(
                        "/api/system/postgres/query",
                        json={
                            "schema_name": "core",
                            "table_name": "test_table",
                            "limit": 10,
                        },
                    )

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["col1"] == 1

    statement = mock_conn.execute.call_args[0][0]
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "FROM core.test_table" in compiled
    assert "LIMIT 10" in compiled


@pytest.mark.asyncio
async def test_query_table_applies_server_side_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["core"]
    mock_inspector.get_table_names.return_value = ["test_table"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "price", "type": "DOUBLE PRECISION", "nullable": True},
    ]
    reflected_table = Table(
        "test_table",
        MetaData(),
        Column("symbol", String),
        Column("price", Float),
        schema="core",
    )
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [{"symbol": "AAPL", "price": 10}]
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_connect = MagicMock()
    mock_connect.__enter__.return_value = mock_conn
    mock_connect.__exit__.return_value = False
    mock_engine.connect.return_value = mock_connect

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            with patch("api.endpoints.postgres._reflect_table", return_value=reflected_table):
                app = create_app()
                async with get_test_client(app) as client:
                    resp = await client.post(
                        "/api/system/postgres/query",
                        json={
                            "schema_name": "core",
                            "table_name": "test_table",
                            "limit": 10,
                            "filters": [
                                {
                                    "column_name": "symbol",
                                    "operator": "contains",
                                    "value": "AAP",
                                },
                                {
                                    "column_name": "price",
                                    "operator": "gte",
                                    "value": "5",
                                },
                            ],
                        },
                    )

    assert resp.status_code == 200
    statement = mock_conn.execute.call_args[0][0]
    compiled = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "WHERE" in compiled
    assert "ILIKE" in compiled
    assert ">= 5.0" in compiled


@pytest.mark.asyncio
async def test_query_table_returns_404_for_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["core"]
    mock_inspector.get_table_names.return_value = ["test_table"]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/query",
                    json={
                        "schema_name": "core",
                        "table_name": "missing_table",
                    },
                )

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_get_table_metadata_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol", "date"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "date", "type": "DATE", "nullable": False},
        {"name": "surprise", "type": "DOUBLE PRECISION", "nullable": True},
        {"name": "source_hash", "type": "TEXT", "nullable": False, "computed": {"sqltext": "md5('x')"}},
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value = [
        SimpleNamespace(column_name="symbol", description="Ticker symbol"),
        SimpleNamespace(column_name="date", description="Trading date"),
        SimpleNamespace(column_name="surprise", description="EPS surprise signal"),
        SimpleNamespace(column_name="source_hash", description=None),
    ]
    mock_connect = MagicMock()
    mock_connect.__enter__.return_value = mock_conn
    mock_connect.__exit__.return_value = False
    mock_engine.connect.return_value = mock_connect

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/gold/tables/market_data/metadata")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["schema_name"] == "gold"
    assert payload["table_name"] == "market_data"
    assert payload["primary_key"] == ["symbol", "date"]
    assert payload["can_edit"] is True
    assert any(
        col["name"] == "source_hash" and col["editable"] is False for col in payload["columns"]
    )
    assert any(
        col["name"] == "symbol" and col.get("description") == "Ticker symbol"
        for col in payload["columns"]
    )
    assert any(
        col["name"] == "source_hash" and col.get("description") is None
        for col in payload["columns"]
    )


@pytest.mark.asyncio
async def test_update_row_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "surprise", "type": "INTEGER", "nullable": True},
    ]

    mock_result = MagicMock()
    mock_result.rowcount = 1
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_begin = MagicMock()
    mock_begin.__enter__.return_value = mock_conn
    mock_begin.__exit__.return_value = False
    mock_engine.begin.return_value = mock_begin

    reflected_table = Table(
        "market_data",
        MetaData(),
        Column("symbol", String, primary_key=True),
        Column("surprise", Integer),
        schema="gold",
    )

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            with patch("api.endpoints.postgres._reflect_table", return_value=reflected_table):
                app = create_app()
                async with get_test_client(app) as client:
                    resp = await client.post(
                        "/api/system/postgres/update",
                        json={
                            "schema_name": "gold",
                            "table_name": "market_data",
                            "match": {"symbol": "AAPL"},
                            "values": {"surprise": 7},
                        },
                    )

    assert resp.status_code == 200
    assert resp.json() == {
        "schema_name": "gold",
        "table_name": "market_data",
        "row_count": 1,
        "updated_columns": ["surprise"],
    }
    assert mock_conn.execute.call_count == 1


@pytest.mark.asyncio
async def test_update_row_requires_primary_key(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    mock_inspector.get_columns.return_value = [
        {"name": "surprise", "type": "INTEGER", "nullable": True},
    ]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/update",
                    json={
                        "schema_name": "gold",
                        "table_name": "market_data",
                        "match": {},
                        "values": {"surprise": 7},
                    },
                )

    assert resp.status_code == 400
    assert "primary key" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_purge_table_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_result = MagicMock()
    mock_result.rowcount = 7
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_begin = MagicMock()
    mock_begin.__enter__.return_value = mock_conn
    mock_begin.__exit__.return_value = False
    mock_engine.begin.return_value = mock_begin

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/purge",
                    json={
                        "schema_name": "gold",
                        "table_name": "market_data",
                    },
                )

    assert resp.status_code == 200
    assert resp.json() == {
        "schema_name": "gold",
        "table_name": "market_data",
        "row_count": 7,
    }
    statement = mock_conn.execute.call_args[0][0]
    assert str(statement) == 'DELETE FROM "gold"."market_data"'


@pytest.mark.asyncio
async def test_purge_table_returns_404_for_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/purge",
                    json={
                        "schema_name": "gold",
                        "table_name": "missing_table",
                    },
                )

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_list_gold_lookup_tables_success(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [("market_data",), ("regime_latest",)]
    mock_conn.execute.return_value = mock_result
    mock_connect = MagicMock()
    mock_connect.__enter__.return_value = mock_conn
    mock_connect.__exit__.return_value = False
    mock_engine.connect.return_value = mock_connect

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/postgres/gold-column-lookup/tables")

    assert resp.status_code == 200
    assert resp.json() == ["market_data", "regime_latest"]


@pytest.mark.asyncio
async def test_list_gold_column_lookup_applies_filters_and_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [
        {
            "schema_name": "gold",
            "table_name": "market_data",
            "column_name": "trend_50_200",
            "data_type": "double precision",
            "description": "Trend signal",
            "calculation_type": "derived_python",
            "calculation_notes": "Computed from moving averages.",
            "calculation_expression": None,
            "calculation_dependencies": ["sma_50d", "sma_200d"],
            "source_job": "tasks.market_data.gold_market_data",
            "status": "reviewed",
            "updated_at": "2026-03-15T00:00:00+00:00",
        },
        {
            "schema_name": "gold",
            "table_name": "market_data",
            "column_name": "trend_20_50",
            "data_type": "double precision",
            "description": "Secondary trend signal",
            "calculation_type": "derived_python",
            "calculation_notes": "Computed from moving averages.",
            "calculation_expression": None,
            "calculation_dependencies": ["sma_20d", "sma_50d"],
            "source_job": "tasks.market_data.gold_market_data",
            "status": "reviewed",
            "updated_at": "2026-03-15T00:00:00+00:00",
        },
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_connect = MagicMock()
    mock_connect.__enter__.return_value = mock_conn
    mock_connect.__exit__.return_value = False
    mock_engine.connect.return_value = mock_connect

    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get(
                "/api/system/postgres/gold-column-lookup"
                "?table=market_data&q=trend&status=reviewed&limit=1&offset=0"
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["limit"] == 1
    assert payload["offset"] == 0
    assert payload["has_more"] is True
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["table"] == "market_data"
    assert payload["rows"][0]["column"] == "trend_50_200"
    assert payload["rows"][0]["calculation_dependencies"] == ["sma_50d", "sma_200d"]

    query_params = mock_conn.execute.call_args[0][1]
    assert query_params["table_name"] == "market_data"
    assert query_params["status"] == "reviewed"
    assert query_params["search"] == "%trend%"


@pytest.mark.asyncio
async def test_list_gold_column_lookup_rejects_unsupported_table(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_engine = MagicMock()
    _configure_discovery_env(monkeypatch)

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get(
                "/api/system/postgres/gold-column-lookup?table=not_supported"
            )

    assert resp.status_code == 404
    assert "not supported" in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "json_payload"),
    [
        ("get", "/api/system/postgres/schemas", None),
        ("get", "/api/system/postgres/schemas/gold/tables/market_data/metadata", None),
        (
            "post",
            "/api/system/postgres/query",
            {
                "schema_name": "gold",
                "table_name": "market_data",
            },
        ),
    ],
)
async def test_postgres_read_routes_require_read_role_when_deployed(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    json_payload: dict[str, object] | None,
) -> None:
    _configure_deployed_auth(monkeypatch)

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(mode="oidc", subject="user-1", claims={"roles": ["AssetAllocation.Access"]}),
    )

    async with get_test_client(app) as client:
        request_kwargs = {"headers": {"Authorization": "Bearer token"}}
        if json_payload is not None:
            request_kwargs["json"] = json_payload
        response = await getattr(client, method)(path, **request_kwargs)

    assert response.status_code == 403
    assert response.json() == {"detail": "Missing required roles: AssetAllocation.DataDiscovery.Read."}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "json_payload"),
    [
        (
            "/api/system/postgres/update",
            {
                "schema_name": "gold",
                "table_name": "market_data",
                "match": {"symbol": "AAPL"},
                "values": {"surprise": 7},
            },
        ),
        (
            "/api/system/postgres/purge",
            {
                "schema_name": "gold",
                "table_name": "market_data",
            },
        ),
    ],
)
async def test_postgres_write_routes_require_write_role_when_deployed(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    json_payload: dict[str, object],
) -> None:
    _configure_deployed_auth(monkeypatch)

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-1",
            claims={"roles": ["AssetAllocation.DataDiscovery.Read"]},
        ),
    )

    async with get_test_client(app) as client:
        response = await client.post(
            path,
            headers={"Authorization": "Bearer token"},
            json=json_payload,
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "Missing required roles: AssetAllocation.DataDiscovery.Write."}
