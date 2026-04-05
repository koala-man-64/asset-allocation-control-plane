from __future__ import annotations

import pytest

from api.service.settings import ServiceSettings


def _configure_browser_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("UI_OIDC_CLIENT_ID", "spa-client-id")
    monkeypatch.setenv("UI_OIDC_AUTHORITY", "https://login.microsoftonline.com/tenant-id")
    monkeypatch.setenv("UI_OIDC_SCOPES", "api://asset-allocation-api/user_impersonation")


def test_browser_oidc_requires_redirect_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_browser_oidc(monkeypatch)

    with pytest.raises(ValueError, match="UI_OIDC_REDIRECT_URI is required"):
        ServiceSettings.from_env()


def test_browser_oidc_rejects_relative_redirect_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_browser_oidc(monkeypatch)
    monkeypatch.setenv("UI_OIDC_REDIRECT_URI", "/auth/callback")

    with pytest.raises(ValueError, match="UI_OIDC_REDIRECT_URI must be an absolute http"):
        ServiceSettings.from_env()


def test_browser_oidc_accepts_localhost_http_redirect_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_browser_oidc(monkeypatch)
    monkeypatch.setenv("UI_OIDC_REDIRECT_URI", "http://localhost:5174/auth/callback")

    settings = ServiceSettings.from_env()

    assert settings.browser_oidc_enabled is True
    assert settings.ui_oidc_config["redirectUri"] == "http://localhost:5174/auth/callback"


def test_deployed_runtime_requires_api_oidc_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.delenv("API_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("API_OIDC_AUDIENCE", raising=False)

    with pytest.raises(ValueError, match="Deployed runtime requires API OIDC configuration."):
        ServiceSettings.from_env()
