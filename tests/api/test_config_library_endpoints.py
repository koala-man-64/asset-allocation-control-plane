from __future__ import annotations

import pytest

from api.service.app import create_app
from core.config_library_repository import ConfigLibraryRepository
from core.regime_repository import RegimeRepository
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_save_regime_policy_validates_model_revision_and_persists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        RegimeRepository,
        "get_regime_model_revision",
        lambda self, name, version=None: {"name": name, "version": version},
    )

    def fake_save(self, family_key, *, name, config, description=""):  # type: ignore[no-untyped-def]
        captured.update({"family": family_key, "name": name, "config": config, "description": description})
        return {"name": name, "version": 2, "description": description, "config": config}

    monkeypatch.setattr(ConfigLibraryRepository, "save_config", fake_save)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/regime-policies/",
            json={
                "name": "observe-default",
                "description": "Observe default model",
                "config": {"modelName": "default-regime", "modelVersion": 3, "mode": "observe_only"},
            },
        )

    assert response.status_code == 200
    assert response.json()["version"] == 2
    assert captured["family"] == "regimePolicy"
    assert captured["config"] == {"modelName": "default-regime", "modelVersion": 3, "mode": "observe_only"}


@pytest.mark.asyncio
async def test_save_risk_policy_persists_strategy_risk_policy_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    captured: dict[str, object] = {}

    def fake_save(self, family_key, *, name, config, description=""):  # type: ignore[no-untyped-def]
        captured.update({"family": family_key, "name": name, "config": config, "description": description})
        return {"name": name, "version": 1, "description": description, "config": config}

    monkeypatch.setattr(ConfigLibraryRepository, "save_config", fake_save)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/risk-policies/",
            json={
                "name": "balanced-risk",
                "config": {
                    "policy": {
                        "scope": "strategy",
                        "stopLoss": {"thresholdPct": 8, "action": "reduce_exposure", "reductionPct": 50},
                    }
                },
            },
        )

    assert response.status_code == 200
    assert captured["family"] == "riskPolicy"
    assert captured["config"]["policy"]["stopLoss"]["thresholdPct"] == 8


@pytest.mark.asyncio
async def test_save_exit_rule_set_rejects_duplicate_rule_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/exit-rule-sets/",
            json={
                "name": "bad-exits",
                "config": {
                    "exits": [
                        {"id": "stop", "type": "stop_loss_fixed", "value": 0.05},
                        {"id": "stop", "type": "take_profit_fixed", "value": 0.10},
                    ]
                },
            },
        )

    assert response.status_code == 422
    assert "Duplicate exit rule id" in response.text
