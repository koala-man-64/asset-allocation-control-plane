from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext, AuthError
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_auth_session_endpoint_returns_anonymous_local_session() -> None:
    app = create_app()

    async with get_test_client(app) as client:
        resp = await client.get("/api/auth/session")

    assert resp.status_code == 200
    assert resp.headers.get("cache-control") == "no-store"
    assert resp.json() == {
        "authMode": "anonymous",
        "subject": "anonymous",
        "displayName": None,
        "username": None,
        "requiredRoles": [],
        "grantedRoles": [],
    }


@pytest.mark.asyncio
async def test_auth_session_endpoint_returns_oidc_claim_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("API_OIDC_REQUIRED_ROLES", "AssetAllocation.Access,Another.Role")

    app = create_app()

    async with get_test_client(app) as client:
        monkeypatch.setattr(
            app.state.auth,
            "authenticate_headers",
            lambda _headers, **_kwargs: AuthContext(
                mode="oidc",
                subject="user-123",
                claims={
                    "name": "Ada Lovelace",
                    "preferred_username": "ada@example.com",
                    "roles": ["Another.Role", "AssetAllocation.Access", "Another.Role"],
                },
            ),
        )
        resp = await client.get("/api/auth/session", headers={"Authorization": "Bearer token"})

    assert resp.status_code == 200
    assert resp.headers.get("cache-control") == "no-store"
    assert resp.json() == {
        "authMode": "oidc",
        "subject": "user-123",
        "displayName": "Ada Lovelace",
        "username": "ada@example.com",
        "requiredRoles": ["AssetAllocation.Access", "Another.Role"],
        "grantedRoles": ["Another.Role", "AssetAllocation.Access"],
    }


@pytest.mark.asyncio
async def test_auth_session_endpoint_propagates_forbidden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")

    app = create_app()

    async with get_test_client(app) as client:
        monkeypatch.setattr(
            app.state.auth,
            "authenticate_headers",
            lambda _headers, **_kwargs: (_ for _ in ()).throw(
                AuthError(status_code=403, detail="Missing required roles: AssetAllocation.Access.")
            ),
        )
        resp = await client.get("/api/auth/session", headers={"Authorization": "Bearer token"})

    assert resp.status_code == 403
    assert resp.json() == {"detail": "Missing required roles: AssetAllocation.Access."}
