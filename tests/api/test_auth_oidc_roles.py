from __future__ import annotations

from typing import Any

import jwt
import pytest
from jwt.exceptions import InvalidAudienceError, InvalidIssuerError

from api.service.auth import AuthError, AuthManager
from api.service.settings import ServiceSettings


def _settings(*, required_roles: str = "AssetAllocation.Access") -> ServiceSettings:
    return ServiceSettings(
        api_root_prefix="",
        api_public_base_url=None,
        oidc_auth_enabled=True,
        anonymous_local_auth_enabled=False,
        oidc_issuer="https://issuer.example.com",
        oidc_audience=["asset-allocation-api"],
        oidc_jwks_url="https://issuer.example.com/jwks",
        oidc_required_scopes=[],
        oidc_required_roles=[required_roles] if required_roles else [],
        postgres_dsn=None,
        browser_oidc_enabled=False,
        ui_oidc_config={},
    )


def test_auth_manager_accepts_valid_role_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = AuthManager(_settings())

    monkeypatch.setattr(manager, "_get_public_key_for_token", lambda _token: object())

    def fake_decode(token: str, signing_key: object, **kwargs: Any) -> dict[str, Any]:
        assert token == "valid-token"
        assert kwargs["issuer"] == "https://issuer.example.com"
        assert kwargs["audience"] == ["asset-allocation-api"]
        return {
            "sub": "user-123",
            "oid": "oid-123",
            "tid": "tenant-123",
            "azp": "spa-client-id",
            "roles": ["AssetAllocation.Access"],
        }

    monkeypatch.setattr(jwt, "decode", fake_decode)

    ctx = manager.authenticate_headers({"Authorization": "Bearer valid-token"})

    assert ctx.mode == "oidc"
    assert ctx.subject == "user-123"


def test_auth_manager_rejects_missing_required_role(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = AuthManager(_settings())

    monkeypatch.setattr(manager, "_get_public_key_for_token", lambda _token: object())
    monkeypatch.setattr(
        jwt,
        "decode",
        lambda *_args, **_kwargs: {"sub": "user-123", "roles": ["Different.Role"]},
    )

    with pytest.raises(AuthError, match="Missing required roles: AssetAllocation.Access."):
        manager.authenticate_headers({"Authorization": "Bearer valid-token"})


@pytest.mark.parametrize(
    ("error_factory", "expected_detail"),
    [
        (lambda: InvalidAudienceError("wrong audience"), "Invalid bearer token."),
        (lambda: InvalidIssuerError("wrong issuer"), "Invalid bearer token."),
    ],
)
def test_auth_manager_rejects_invalid_token_claims(
    monkeypatch: pytest.MonkeyPatch,
    error_factory,
    expected_detail: str,
) -> None:
    manager = AuthManager(_settings())

    monkeypatch.setattr(manager, "_get_public_key_for_token", lambda _token: object())

    def fail_decode(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise error_factory()

    monkeypatch.setattr(jwt, "decode", fail_decode)

    with pytest.raises(AuthError, match=expected_detail):
        manager.authenticate_headers({"Authorization": "Bearer invalid-token"})
