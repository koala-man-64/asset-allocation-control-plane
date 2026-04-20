from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from tests.api._client import get_test_client


def _configure_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")


@pytest.mark.asyncio
async def test_schwab_callback_route_is_unauthenticated(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback",
            params={"code": "auth-code-123", "state": "opaque", "session": "session-1"},
        )

    assert response.status_code == 200
    assert response.json() == {"provider": "schwab", "authorizationReceived": True}
    assert "auth-code-123" not in response.text


@pytest.mark.asyncio
async def test_schwab_callback_url_route_requires_auth_when_deployed(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/providers/schwab/connect/callback-url")

    assert response.status_code == 401
    assert response.headers.get("www-authenticate") == "Bearer"


@pytest.mark.asyncio
async def test_schwab_callback_url_route_returns_resolved_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback-url",
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"callback_url": "https://api.example.com/api/providers/schwab/connect/callback"}


@pytest.mark.asyncio
async def test_schwab_callback_url_route_returns_503_when_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback-url",
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Schwab callback URL is not configured. Set SCHWAB_APP_CALLBACK_URL or API_PUBLIC_BASE_URL."
