from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from api.endpoints import system as system_routes
from api.service.app import create_app
from api.service.auth import AuthContext
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


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


def _configure_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://app:password@db.example.com:5432/app")


async def test_runtime_config_write_rejects_base_authenticated_user(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    app = create_app()
    app.state.auth = _FakeAuthManager(["AssetAllocation.Access"])

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/system/runtime-config",
            json={"key": "SYSTEM_HEALTH_TTL_SECONDS", "value": "300"},
        )

    assert response.status_code == 403
    assert "AssetAllocation.RuntimeConfig.Write" in response.text


async def test_runtime_config_write_allows_operator_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setattr(system_routes, "_get_actor", lambda request: "operator@example.com")
    monkeypatch.setattr(
        system_routes,
        "upsert_runtime_config",
        lambda **kwargs: SimpleNamespace(
            scope=kwargs["scope"],
            key=kwargs["key"],
            value=kwargs["value"],
            description=kwargs["description"],
            updated_at=datetime(2026, 4, 26, tzinfo=timezone.utc),
            updated_by=kwargs["actor"],
        ),
    )

    app = create_app()
    app.state.auth = _FakeAuthManager(["AssetAllocation.Access", "AssetAllocation.RuntimeConfig.Write"])

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/system/runtime-config",
            json={"key": "SYSTEM_HEALTH_TTL_SECONDS", "value": "300"},
        )

    assert response.status_code == 200
    assert response.json()["key"] == "SYSTEM_HEALTH_TTL_SECONDS"
    assert response.json()["updatedBy"] == "operator@example.com"
