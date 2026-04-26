from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext, AuthError
from tests.api._client import get_test_client
from tests.api._password_auth import password_verifier_for

SESSION_SECRET = "test-session-secret-key-value-at-least-32-chars"
PASSWORD_SECRET = "operator-secret"
TEST_ORIGIN = "http://test"


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
        create_resp = await client.post(
            "/api/auth/session",
            headers={"Authorization": "Bearer token", "Origin": TEST_ORIGIN},
        )
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
        create_resp = await client.post(
            "/api/auth/session",
            headers={"Authorization": "Bearer token", "Origin": TEST_ORIGIN},
        )
        csrf_token = create_resp.cookies.get("aa_csrf_dev")

        missing_csrf_resp = await client.post("/api/realtime/ticket", headers={"Origin": TEST_ORIGIN})
        valid_csrf_resp = await client.post(
            "/api/realtime/ticket",
            headers={"Origin": TEST_ORIGIN, "X-CSRF-Token": csrf_token or ""},
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


@pytest.mark.asyncio
async def test_password_auth_session_sets_cookie_and_returns_password_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)
    monkeypatch.setenv("UI_AUTH_PROVIDER", "password")
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for(PASSWORD_SECRET))

    app = create_app()

    async with get_test_client(app) as client:
        create_resp = await client.post(
            "/api/auth/session",
            json={"password": PASSWORD_SECRET},
            headers={"Origin": TEST_ORIGIN},
        )
        get_resp = await client.get("/api/auth/session")

    assert create_resp.status_code == 200
    assert create_resp.json()["authMode"] == "password"
    assert create_resp.json()["subject"] == "operator"
    assert create_resp.cookies.get("aa_session_dev")
    assert create_resp.cookies.get("aa_csrf_dev")
    assert get_resp.status_code == 200
    assert get_resp.json()["authMode"] == "password"
    assert get_resp.json()["requiredRoles"] == []
    assert get_resp.json()["username"] == "operator"


@pytest.mark.asyncio
async def test_password_auth_requires_same_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)
    monkeypatch.setenv("UI_AUTH_PROVIDER", "password")
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for(PASSWORD_SECRET))

    app = create_app()

    async with get_test_client(app) as client:
        resp = await client.post(
            "/api/auth/session",
            json={"password": PASSWORD_SECRET},
            headers={"Origin": "https://evil.example.com"},
        )

    assert resp.status_code == 403
    assert resp.json() == {"detail": "Origin or Referer does not match the expected UI origin."}


@pytest.mark.asyncio
async def test_password_auth_rate_limits_failed_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)
    monkeypatch.setenv("UI_AUTH_PROVIDER", "password")
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for(PASSWORD_SECRET))
    monkeypatch.setenv("UI_PASSWORD_RATE_LIMIT_MAX_ATTEMPTS_PER_IP", "2")
    monkeypatch.setenv("UI_PASSWORD_RATE_LIMIT_MAX_ATTEMPTS_GLOBAL", "2")

    app = create_app()

    async with get_test_client(app) as client:
        first = await client.post(
            "/api/auth/session",
            json={"password": "wrong-one"},
            headers={"Origin": TEST_ORIGIN},
        )
        second = await client.post(
            "/api/auth/session",
            json={"password": "wrong-two"},
            headers={"Origin": TEST_ORIGIN},
        )
        third = await client.post(
            "/api/auth/session",
            json={"password": "wrong-three"},
            headers={"Origin": TEST_ORIGIN},
        )

    assert first.status_code == 401
    assert second.status_code == 401
    assert third.status_code == 429
    assert third.json() == {"detail": "Too many failed login attempts. Try again later."}


@pytest.mark.asyncio
async def test_password_auth_deployed_cookie_uses_host_prefix_and_secure_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", SESSION_SECRET)
    monkeypatch.setenv("UI_AUTH_PROVIDER", "password")
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for(PASSWORD_SECRET))

    app = create_app()

    async with get_test_client(app) as client:
        resp = await client.post(
            "/api/auth/session",
            json={"password": PASSWORD_SECRET},
            headers={"Origin": TEST_ORIGIN},
        )

    assert resp.status_code == 200
    set_cookie = resp.headers.get_list("set-cookie")
    assert any("__Host-aa_session=" in value and "HttpOnly" in value and "SameSite=lax" in value for value in set_cookie)
    assert any("__Host-aa_session=" in value and "Secure" in value and "Path=/" in value for value in set_cookie)
