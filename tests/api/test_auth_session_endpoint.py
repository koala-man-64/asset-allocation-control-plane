from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext, AuthError
from tests.api._client import get_test_client

SESSION_SECRET = "test-session-secret-key-value-at-least-32-chars"


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
            "authenticate_request",
            lambda _headers, _cookies, **_kwargs: AuthContext(
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
            "authenticate_request",
            lambda _headers, _cookies, **_kwargs: (_ for _ in ()).throw(
                AuthError(status_code=403, detail="Missing required roles: AssetAllocation.Access.")
            ),
        )
        resp = await client.get("/api/auth/session", headers={"Authorization": "Bearer token"})

    assert resp.status_code == 403
    assert resp.json() == {"detail": "Missing required roles: AssetAllocation.Access."}


@pytest.mark.asyncio
async def test_auth_session_post_sets_cookie_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)

    app = create_app()

    async with get_test_client(app) as client:
        monkeypatch.setattr(
            app.state.auth,
            "authenticate_bearer_headers",
            lambda _headers, **_kwargs: AuthContext(
                mode="oidc",
                subject="user-123",
                claims={
                    "sub": "user-123",
                    "name": "Ada Lovelace",
                    "preferred_username": "ada@example.com",
                    "roles": ["AssetAllocation.Access"],
                },
            ),
        )
        create_resp = await client.post("/api/auth/session", headers={"Authorization": "Bearer token"})
        get_resp = await client.get("/api/auth/session")

    assert create_resp.status_code == 200
    assert "aa_session_dev" in create_resp.cookies
    assert "aa_csrf_dev" in create_resp.cookies
    assert create_resp.cookies.get("aa_csrf_dev")
    assert create_resp.json()["subject"] == "user-123"
    assert get_resp.status_code == 200
    assert get_resp.json()["username"] == "ada@example.com"


@pytest.mark.asyncio
async def test_auth_session_rejects_tampered_cookie(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)

    app = create_app()

    async with get_test_client(app) as client:
        client.cookies.set("aa_session_dev", "tampered")
        resp = await client.get("/api/auth/session")

    assert resp.status_code == 401
    assert resp.json() == {"detail": "Invalid auth session cookie."}


@pytest.mark.asyncio
async def test_cookie_auth_unsafe_request_requires_csrf(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)

    app = create_app()

    async with get_test_client(app) as client:
        monkeypatch.setattr(
            app.state.auth,
            "authenticate_bearer_headers",
            lambda _headers, **_kwargs: AuthContext(
                mode="oidc",
                subject="user-123",
                claims={
                    "sub": "user-123",
                    "name": "Ada Lovelace",
                    "roles": ["AssetAllocation.Access"],
                },
            ),
        )
        create_resp = await client.post("/api/auth/session", headers={"Authorization": "Bearer token"})
        csrf_token = create_resp.cookies.get("aa_csrf_dev")

        missing_csrf_resp = await client.post("/api/realtime/ticket")
        valid_csrf_resp = await client.post(
            "/api/realtime/ticket",
            headers={"X-CSRF-Token": csrf_token or ""},
        )

    assert missing_csrf_resp.status_code == 403
    assert missing_csrf_resp.json() == {"detail": "CSRF token is missing or invalid."}
    assert valid_csrf_resp.status_code == 200
    assert isinstance(valid_csrf_resp.json().get("ticket"), str)


@pytest.mark.asyncio
async def test_auth_session_delete_clears_session_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)

    app = create_app()

    async with get_test_client(app) as client:
        resp = await client.delete("/api/auth/session")

    assert resp.status_code == 204
    set_cookie = resp.headers.get_list("set-cookie")
    assert any("aa_session_dev=" in value and "Max-Age=0" in value for value in set_cookie)
    assert any("aa_csrf_dev=" in value and "Max-Age=0" in value for value in set_cookie)
