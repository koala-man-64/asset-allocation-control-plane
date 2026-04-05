from __future__ import annotations

from unittest.mock import patch

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


class _FakeArmClient:
    def __init__(self, _cfg) -> None:
        self.posted_urls: list[str] = []

    def __enter__(self) -> "_FakeArmClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"/subscriptions/sub/resourceGroups/rg/providers/{provider}/{resource_type}/{name}"

    def get_json(self, url: str):
        if url.endswith("/containerApps/asset-allocation-api"):
            return {
                "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/asset-allocation-api",
                "properties": {
                    "provisioningState": "Succeeded",
                    "runningStatus": "Running",
                    "latestReadyRevisionName": "asset-allocation-api--000001",
                    "configuration": {"ingress": {"fqdn": "api.example.internal"}},
                },
            }
        if url.endswith("/containerApps/asset-allocation-ui"):
            return {
                "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/asset-allocation-ui",
                "properties": {
                    "provisioningState": "Succeeded",
                    "runningStatus": "Running",
                    "latestReadyRevisionName": "asset-allocation-ui--000001",
                    "configuration": {"ingress": {"fqdn": "ui.example.internal"}},
                },
            }
        raise ValueError(f"Unexpected ARM URL: {url}")

    def post_json(self, url: str):
        self.posted_urls.append(url)
        return {
            "properties": {
                "provisioningState": "Succeeded",
                "runningStatus": "Running" if url.endswith("/start") else "Stopped",
            }
        }


class _FakeLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_FakeLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "msg", "type": "string"},
                    ],
                    "rows": [
                        ["2026-02-10T00:00:00Z", "api booted"],
                        ["2026-02-10T00:00:02Z", "health check passed"],
                    ],
                }
            ]
        }


def _set_container_app_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "asset-allocation-api,asset-allocation-ui")


@pytest.mark.asyncio
async def test_list_container_apps_with_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_container_app_env(monkeypatch)

    probe_results = [
        {
            "status": "healthy",
            "url": "https://api.example.internal/healthz",
            "httpStatus": 200,
            "checkedAt": "2026-02-10T00:00:00Z",
            "error": None,
        },
        {
            "status": "healthy",
            "url": "https://ui.example.internal/",
            "httpStatus": 200,
            "checkedAt": "2026-02-10T00:00:01Z",
            "error": None,
        },
    ]

    with patch("api.endpoints.system.AzureArmClient", _FakeArmClient), patch(
        "api.endpoints.system._probe_container_app_health", side_effect=probe_results
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/container-apps?probe=true")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["probed"] is True
    assert len(payload["apps"]) == 2

    by_name = {item["name"]: item for item in payload["apps"]}
    assert by_name["asset-allocation-api"]["health"]["status"] == "healthy"
    assert by_name["asset-allocation-ui"]["health"]["url"] == "https://ui.example.internal/"


@pytest.mark.asyncio
async def test_start_container_app(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_container_app_env(monkeypatch)

    fake_arm = _FakeArmClient(None)
    with patch("api.endpoints.system.AzureArmClient", return_value=fake_arm):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post("/api/system/container-apps/asset-allocation-api/start")

    assert resp.status_code == 202
    payload = resp.json()
    assert payload["appName"] == "asset-allocation-api"
    assert payload["action"] == "start"
    assert any(url.endswith("/containerApps/asset-allocation-api/start") for url in fake_arm.posted_urls)


@pytest.mark.asyncio
async def test_stop_container_app(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_container_app_env(monkeypatch)

    fake_arm = _FakeArmClient(None)
    with patch("api.endpoints.system.AzureArmClient", return_value=fake_arm):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post("/api/system/container-apps/asset-allocation-ui/stop")

    assert resp.status_code == 202
    payload = resp.json()
    assert payload["appName"] == "asset-allocation-ui"
    assert payload["action"] == "stop"
    assert any(url.endswith("/containerApps/asset-allocation-ui/stop") for url in fake_arm.posted_urls)


@pytest.mark.asyncio
async def test_container_app_not_allowlisted(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_container_app_env(monkeypatch)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.post("/api/system/container-apps/not-allowed/start")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_container_app_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_container_app_env(monkeypatch)
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace-id")

    fake_logs = _FakeLogAnalyticsClient()
    with patch("api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/container-apps/asset-allocation-api/logs?minutes=30&tail=5")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["appName"] == "asset-allocation-api"
    assert payload["lookbackMinutes"] == 30
    assert payload["tailLines"] == 5
    assert payload["logs"] == ["api booted", "health check passed"]

    assert len(fake_logs.queries) == 1
    workspace_id, query, timespan = fake_logs.queries[0]
    assert workspace_id == "workspace-id"
    assert "asset-allocation-api" in query
    assert timespan is not None


@pytest.mark.asyncio
async def test_get_container_app_logs_not_allowlisted(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_container_app_env(monkeypatch)
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace-id")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/container-apps/not-allowed/logs")

    assert resp.status_code == 404
