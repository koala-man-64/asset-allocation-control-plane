from __future__ import annotations

import pytest

from api.endpoints import rankings as ranking_endpoints
from api.service.app import create_app
from core.ranking_repository import RankingRepository
from core.universe_repository import UniverseRepository
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_list_ranking_schemas_returns_repo_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        RankingRepository,
        "list_ranking_schemas",
        lambda self: [{"name": "quality", "description": "desc", "version": 1, "updated_at": "2026-03-08T00:00:00Z"}],
    )

    app = create_app()
    async with get_test_client(app) as client:
      response = await client.get("/api/rankings/")

    assert response.status_code == 200
    assert response.json()[0]["name"] == "quality"


@pytest.mark.asyncio
async def test_ranking_catalog_endpoint_returns_gold_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        ranking_endpoints,
        "list_gold_ranking_catalog",
        lambda _dsn: {
            "source": "postgres_gold",
            "tables": [
                {
                    "name": "market_data",
                    "asOfColumn": "date",
                    "columns": [{"name": "return_20d", "dataType": "double precision", "valueKind": "number"}],
                }
            ],
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/rankings/catalog")

    assert response.status_code == 200
    assert response.json()["tables"][0]["columns"][0]["name"] == "return_20d"


@pytest.mark.asyncio
async def test_preview_rankings_accepts_draft_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        ranking_endpoints,
        "preview_strategy_rankings",
        lambda _dsn, *, strategy_name, schema, as_of_date, limit=25: {
            "strategyName": strategy_name,
            "asOfDate": as_of_date,
            "rowCount": 1,
            "rows": [{"symbol": "AAPL", "rank": 1, "score": 0.91}],
            "warnings": [],
        },
    )

    payload = {
        "strategyName": "mom-spy-res",
        "asOfDate": "2026-03-08",
        "schema": {
            "universeConfigName": "large-cap-quality",
            "groups": [
                {
                    "name": "quality",
                    "weight": 1,
                    "factors": [
                        {
                            "name": "f1",
                            "table": "market_data",
                            "column": "return_20d",
                            "weight": 1,
                            "direction": "desc",
                            "missingValuePolicy": "exclude",
                            "transforms": [{"type": "zscore", "params": {}}],
                        }
                    ],
                    "transforms": [],
                }
            ],
            "overallTransforms": [],
        },
    }

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/rankings/preview", json=payload)

    assert response.status_code == 200
    assert response.json()["rows"][0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_save_ranking_schema_rejects_unknown_universe_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(UniverseRepository, "get_universe_config", lambda self, name: None)

    payload = {
        "name": "quality",
        "description": "Quality composite",
        "config": {
            "universeConfigName": "missing-universe",
            "groups": [
                {
                    "name": "quality",
                    "weight": 1,
                    "factors": [
                        {
                            "name": "f1",
                            "table": "market_data",
                            "column": "return_20d",
                            "weight": 1,
                            "direction": "desc",
                            "missingValuePolicy": "exclude",
                            "transforms": [],
                        }
                    ],
                    "transforms": [],
                }
            ],
            "overallTransforms": [],
        },
    }

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/rankings/", json=payload)

    assert response.status_code == 400
    assert "Universe config 'missing-universe' not found." in response.text


@pytest.mark.asyncio
async def test_materialize_rankings_returns_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        ranking_endpoints,
        "materialize_strategy_rankings",
        lambda _dsn, *, strategy_name, start_date=None, end_date=None, triggered_by="api": {
            "runId": "run-1",
            "strategyName": strategy_name,
            "rankingSchemaName": "quality",
            "rankingSchemaVersion": 2,
            "outputTableName": "mom_spy_res",
            "startDate": "2026-03-01",
            "endDate": "2026-03-08",
            "rowCount": 10,
            "dateCount": 5,
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/rankings/materialize", json={"strategyName": "mom-spy-res"})

    assert response.status_code == 200
    assert response.json()["outputTableName"] == "mom_spy_res"
