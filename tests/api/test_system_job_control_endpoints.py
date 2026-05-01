from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
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


class _FakeAuthManager:
    def __init__(self, roles: list[str]) -> None:
        self._roles = roles

    def authenticate_request(
        self,
        headers: dict[str, str],
        cookies: dict[str, str],
        *,
        request_context: dict[str, Any] | None = None,
    ) -> AuthContext:
        return AuthContext(mode="oidc", subject="user-1", claims={"roles": self._roles})

    def authenticate_headers(
        self,
        headers: dict[str, str],
        *,
        request_context: dict[str, Any] | None = None,
    ) -> AuthContext:
        return self.authenticate_request(headers, {}, request_context=request_context)


def _set_arm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")


def _configure_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://app:password@db.example.com:5432/app")


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
async def test_trigger_job_allows_authenticated_user_without_job_operate_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_oidc(monkeypatch)
    _set_arm_env(monkeypatch)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)
    _FakeRunArmClient.started_urls.clear()

    with patch("api.endpoints.system.AzureArmClient", _FakeRunArmClient):
        app = create_app()
        app.state.auth = _FakeAuthManager(["AssetAllocation.Access"])
        async with get_test_client(app) as client:
            response = await client.post("/api/system/jobs/new-container-job/run")

    assert response.status_code == 202
    assert response.json()["jobName"] == "new-container-job"
    assert _FakeRunArmClient.started_urls == [
        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/new-container-job/start"
    ]


@pytest.mark.asyncio
async def test_job_state_controls_still_require_job_operate_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    _set_arm_env(monkeypatch)
    _FakeRunArmClient.started_urls.clear()

    app = create_app()
    app.state.auth = _FakeAuthManager(["AssetAllocation.Access"])
    async with get_test_client(app) as client:
        response = await client.post("/api/system/jobs/new-container-job/suspend")

    assert response.status_code == 403
    assert response.json() == {"detail": "Missing required roles: AssetAllocation.Jobs.Operate."}
    assert _FakeRunArmClient.started_urls == []


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
