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
    assert settings.ui_oidc_config["postLogoutRedirectUri"] == "http://localhost:5174/auth/logout-complete"


def test_deployed_runtime_requires_api_oidc_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.delenv("API_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("API_OIDC_AUDIENCE", raising=False)

    with pytest.raises(ValueError, match="Deployed runtime requires API OIDC configuration."):
        ServiceSettings.from_env()


def test_symbol_enrichment_requires_allowed_jobs_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ENABLED", "true")
    monkeypatch.delenv("SYMBOL_ENRICHMENT_ALLOWED_JOBS", raising=False)

    with pytest.raises(ValueError, match="SYMBOL_ENRICHMENT_ALLOWED_JOBS is required"):
        ServiceSettings.from_env()


def test_symbol_enrichment_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ENABLED", "true")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_MODEL", "gpt-5.4")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_CONFIDENCE_MIN", "0.82")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_MAX_SYMBOLS_PER_RUN", "750")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ALLOWED_JOBS", "symbol-cleanup-job,symbol-cleanup-backfill-job")

    settings = ServiceSettings.from_env()

    assert settings.symbol_enrichment.enabled is True
    assert settings.symbol_enrichment.model == "gpt-5.4"
    assert settings.symbol_enrichment.confidence_min == pytest.approx(0.82)
    assert settings.symbol_enrichment.max_symbols_per_run == 750
    assert settings.symbol_enrichment.allowed_jobs == [
        "symbol-cleanup-job",
        "symbol-cleanup-backfill-job",
    ]
