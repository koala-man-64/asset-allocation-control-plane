from __future__ import annotations

import pytest

from api.endpoints import regimes as regime_endpoints
from api.service.app import create_app
from asset_allocation_runtime_common.domain.regime import canonical_default_regime_model_config
from core.regime_repository import RegimeRepository
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


async def _get_json(client, url: str) -> dict:
    response = await client.get(url)
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


async def _post_json(client, url: str, payload: dict) -> dict:
    response = await client.post(url, json=payload)
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    return body


async def test_get_current_regime_returns_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        RegimeRepository,
        "get_regime_latest",
        lambda self, **kwargs: {
            "as_of_date": "2026-03-07",
            "effective_from_date": "2026-03-10",
            "model_name": kwargs["model_name"],
            "model_version": kwargs.get("model_version") or 1,
            "active_regimes": ["trending_up", "low_volatility"],
            "signals": [
                {
                    "regime_code": "trending_up",
                    "display_name": "Trending (Up)",
                    "signal_state": "active",
                    "score": 0.9,
                    "activation_threshold": 0.6,
                    "is_active": True,
                    "matched_rule_id": "trending_up",
                    "evidence": {},
                }
            ],
            "halt_flag": False,
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        payload = await _get_json(client, "/api/regimes/current")

    assert payload["model_name"] == "default-regime"
    assert payload["active_regimes"] == ["trending_up", "low_volatility"]
    assert payload["signals"][0]["regime_code"] == "trending_up"


async def test_create_regime_model_returns_saved_revision(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    canonical_config = canonical_default_regime_model_config()
    monkeypatch.setattr(
        RegimeRepository,
        "save_regime_model",
        lambda self, **kwargs: {
            "name": kwargs["name"],
            "version": 2,
            "description": kwargs["description"],
            "config": kwargs["config"],
        },
    )
    monkeypatch.setattr(
        RegimeRepository,
        "get_active_regime_model_revision",
        lambda self, name: {"name": name, "version": 1, "config": {}},
    )

    app = create_app()
    async with get_test_client(app) as client:
        payload = await _post_json(
            client,
            "/api/regimes/models",
            {
                "name": "default-regime",
                "description": "Updated",
                "config": canonical_config,
            },
        )

    assert payload["model"]["name"] == "default-regime"
    assert payload["model"]["version"] == 2
    assert payload["activeRevision"]["version"] == 1


async def test_create_regime_model_rejects_noncanonical_default_regime_signal_config(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    called = {"save": 0}
    monkeypatch.setattr(
        RegimeRepository,
        "save_regime_model",
        lambda self, **kwargs: called.__setitem__("save", called["save"] + 1) or kwargs,
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/regimes/models",
            json={
                "name": "default-regime",
                "description": "Invalid",
                "config": {"activationThreshold": 0.7},
            },
        )

    assert response.status_code == 422
    assert "canonical v3 semantics" in response.json()["detail"]
    assert called["save"] == 0


async def test_activate_regime_model_triggers_job_when_configured(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("REGIME_ACA_JOB_NAME", "gold-regime-job")
    canonical_config = canonical_default_regime_model_config()
    monkeypatch.setattr(
        RegimeRepository,
        "get_regime_model_revision",
        lambda self, name, version=None: {"name": name, "version": version or 1, "config": canonical_config},
    )
    monkeypatch.setattr(
        RegimeRepository,
        "activate_regime_model",
        lambda self, **kwargs: {
            "name": kwargs["name"],
            "version": kwargs.get("version") or 1,
            "config": canonical_config,
        },
    )
    monkeypatch.setattr(
        regime_endpoints,
        "_trigger_regime_job_if_configured",
        lambda: {"status": "queued", "executionName": "job-run-1"},
    )

    app = create_app()
    async with get_test_client(app) as client:
        payload = await _post_json(client, "/api/regimes/models/default-regime/activate", {"version": 1})

    assert payload["model"] == "default-regime"
    assert payload["activatedRevision"]["version"] == 1
    assert payload["jobTrigger"]["executionName"] == "job-run-1"


async def test_activate_regime_model_rejects_noncanonical_default_regime_revision(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    called = {"activate": 0}
    monkeypatch.setattr(
        RegimeRepository,
        "get_regime_model_revision",
        lambda self, name, version=None: {
            "name": name,
            "version": version or 1,
            "config": {"activationThreshold": 0.7},
        },
    )
    monkeypatch.setattr(
        RegimeRepository,
        "activate_regime_model",
        lambda self, **kwargs: called.__setitem__("activate", called["activate"] + 1) or kwargs,
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/regimes/models/default-regime/activate", json={"version": 1})

    assert response.status_code == 409
    assert "canonical v3 semantics" in response.json()["detail"]
    assert called["activate"] == 0
