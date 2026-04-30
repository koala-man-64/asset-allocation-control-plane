from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


class _FakeRunArmClient:
    started_urls: list[str] = []

    def __init__(self, _cfg: Any) -> None:
        return None

    def __enter__(self) -> "_FakeRunArmClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"/subscriptions/sub/resourceGroups/rg/providers/{provider}/{resource_type}/{name}"

    def post_json(self, url: str):
        self.started_urls.append(url)
        return {"id": f"{url}/executions/run-001", "name": "run-001"}


def _set_arm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")


@pytest.mark.asyncio
async def test_trigger_job_allows_any_valid_job_when_allowlist_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_arm_env(monkeypatch)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)
    _FakeRunArmClient.started_urls.clear()

    with patch("api.endpoints.system.AzureArmClient", _FakeRunArmClient):
        app = create_app()
        async with get_test_client(app) as client:
            response = await client.post("/api/system/jobs/new-container-job/run")

    assert response.status_code == 202
    assert response.json()["jobName"] == "new-container-job"
    assert _FakeRunArmClient.started_urls == [
        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/new-container-job/start"
    ]


@pytest.mark.asyncio
async def test_trigger_job_still_honors_explicit_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_arm_env(monkeypatch)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "known-job")
    _FakeRunArmClient.started_urls.clear()

    with patch("api.endpoints.system.AzureArmClient", _FakeRunArmClient):
        app = create_app()
        async with get_test_client(app) as client:
            response = await client.post("/api/system/jobs/new-container-job/run")

    assert response.status_code == 404
    assert _FakeRunArmClient.started_urls == []
