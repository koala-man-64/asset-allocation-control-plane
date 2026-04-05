from __future__ import annotations

import pytest

from api.endpoints import strategies as strategy_endpoints
from api.service.app import create_app
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository
from tests.api._client import get_test_client


def _sample_universe_payload() -> dict:
    return {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "table": "market_data",
                    "column": "close",
                    "operator": "gt",
                    "value": 10,
                }
            ],
        },
    }


@pytest.mark.asyncio
async def test_save_strategy_persists_universe_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    captured: dict[str, object] = {}

    def fake_save(self, *, name, config, strategy_type="configured", description=""):  # type: ignore[no-untyped-def]
        captured.update(
            {
                "name": name,
                "config": config,
                "strategy_type": strategy_type,
                "description": description,
            }
        )

    monkeypatch.setattr(StrategyRepository, "save_strategy", fake_save)
    monkeypatch.setattr(
        UniverseRepository,
        "get_universe_config",
        lambda self, name: {"name": name, "config": _sample_universe_payload()},
    )

    payload = {
        "name": "mom-spy-res",
        "type": "configured",
        "description": "Structured universe strategy",
        "config": {
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 20,
            "lookbackWindow": 63,
            "holdingPeriod": 21,
            "costModel": "default",
            "intrabarConflictPolicy": "stop_first",
            "exits": [],
        },
    }

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/strategies/", json=payload)

    assert response.status_code == 200
    assert captured["name"] == "mom-spy-res"
    assert captured["strategy_type"] == "configured"
    assert captured["description"] == "Structured universe strategy"
    assert captured["config"] == payload["config"]


@pytest.mark.asyncio
async def test_save_strategy_rejects_unknown_universe_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(UniverseRepository, "get_universe_config", lambda self, name: None)

    payload = {
        "name": "mom-spy-res",
        "type": "configured",
        "description": "Structured universe strategy",
        "config": {
            "universeConfigName": "missing-universe",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 20,
            "lookbackWindow": 63,
            "holdingPeriod": 21,
            "costModel": "default",
            "intrabarConflictPolicy": "stop_first",
            "exits": [],
        },
    }

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/strategies/", json=payload)

    assert response.status_code == 400
    assert "Universe config 'missing-universe' not found." in response.text


@pytest.mark.asyncio
async def test_get_strategy_detail_round_trips_structured_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    strategy_payload = {
        "name": "mom-spy-res",
        "type": "configured",
        "description": "Structured universe strategy",
        "updated_at": "2026-03-08T00:00:00Z",
        "config": {
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 20,
            "lookbackWindow": 63,
            "holdingPeriod": 21,
            "costModel": "default",
            "intrabarConflictPolicy": "stop_first",
            "exits": [],
        },
    }

    monkeypatch.setattr(StrategyRepository, "get_strategy", lambda self, name: strategy_payload)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/strategies/mom-spy-res/detail")

    assert response.status_code == 200
    assert response.json()["config"]["universeConfigName"] == "large-cap-quality"


@pytest.mark.asyncio
async def test_universe_catalog_endpoint_returns_gold_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    monkeypatch.setattr(
        strategy_endpoints,
        "list_gold_universe_catalog",
        lambda _dsn: {
            "source": "postgres_gold",
            "tables": [
                {
                    "name": "market_data",
                    "asOfColumn": "date",
                    "columns": [
                        {
                            "name": "close",
                            "dataType": "double precision",
                            "valueKind": "number",
                            "operators": ["eq", "gt"],
                        }
                    ],
                }
            ],
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/strategies/universe/catalog")

    assert response.status_code == 200
    payload = response.json()
    assert payload["tables"][0]["name"] == "market_data"
    assert payload["tables"][0]["columns"][0]["name"] == "close"


@pytest.mark.asyncio
async def test_universe_preview_endpoint_maps_validation_errors_to_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def fake_preview(_dsn: str, _universe, *, sample_limit: int = 25):  # type: ignore[no-untyped-def]
        raise ValueError("Unknown gold table 'bad_table'.")

    monkeypatch.setattr(strategy_endpoints, "preview_gold_universe", fake_preview)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/strategies/universe/preview",
            json={"universe": _sample_universe_payload(), "sampleLimit": 10},
        )

    assert response.status_code == 400
    assert "Unknown gold table" in response.text
