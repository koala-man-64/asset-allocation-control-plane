from __future__ import annotations

import pytest

from api.endpoints import universes as universe_endpoints
from api.service.app import create_app
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
                    "field": "market.close",
                    "operator": "gt",
                    "value": 10,
                }
            ],
        },
    }


@pytest.mark.asyncio
async def test_list_universe_configs_returns_repo_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        UniverseRepository,
        "list_universe_configs",
        lambda self: [{"name": "large-cap-quality", "description": "desc", "version": 1, "updated_at": "2026-03-08T00:00:00Z"}],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/universes/")

    assert response.status_code == 200
    assert response.json()[0]["name"] == "large-cap-quality"


@pytest.mark.asyncio
async def test_universe_catalog_endpoint_returns_gold_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        universe_endpoints,
        "list_gold_universe_catalog",
        lambda _dsn: {
            "source": "postgres_gold",
            "fields": [
                {
                    "id": "market.close",
                    "label": "Market Close",
                    "valueKind": "number",
                    "operators": ["eq", "gt"],
                }
            ],
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/universes/catalog")

    assert response.status_code == 200
    assert response.json()["fields"][0]["id"] == "market.close"


@pytest.mark.asyncio
async def test_preview_universe_accepts_draft_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        universe_endpoints,
        "preview_gold_universe",
        lambda _dsn, _universe, *, sample_limit=25: {
            "source": "postgres_gold",
            "symbolCount": 2,
            "sampleSymbols": ["AAPL", "MSFT"],
            "fieldsUsed": ["market.close"],
            "warnings": [],
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/universes/preview", json={"universe": _sample_universe_payload()})

    assert response.status_code == 200
    assert response.json()["symbolCount"] == 2
    assert response.json()["fieldsUsed"] == ["market.close"]


@pytest.mark.asyncio
async def test_delete_universe_rejects_referenced_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        UniverseRepository,
        "get_universe_config_references",
        lambda self, name: {"strategies": ["mom-spy-res"], "rankingSchemas": ["quality-rank"]},
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.delete("/api/universes/large-cap-quality")

    assert response.status_code == 409
    assert "mom-spy-res" in response.text
    assert "quality-rank" in response.text
