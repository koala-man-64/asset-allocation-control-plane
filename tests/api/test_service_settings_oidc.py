from __future__ import annotations

import pytest

from api.service.settings import ServiceSettings
from tests.api._password_auth import password_verifier_for


def _configure_browser_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("UI_OIDC_CLIENT_ID", "spa-client-id")
    monkeypatch.setenv("UI_OIDC_AUTHORITY", "https://login.microsoftonline.com/tenant-id")
    monkeypatch.setenv("UI_OIDC_SCOPES", "api://asset-allocation-api/user_impersonation")


def _configure_break_glass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UI_AUTH_PROVIDER", "password")
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for("operator-secret"))
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED", "true")
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS", "127.0.0.1/32")
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_EXPIRES_AT", "2099-01-01T00:00:00Z")
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_ROLES", "AssetAllocation.System.Read")


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
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", "test-session-secret-key-value-at-least-32-chars")
    monkeypatch.setenv("UI_OIDC_REDIRECT_URI", "http://localhost:5174/auth/callback")

    settings = ServiceSettings.from_env()

    assert settings.browser_oidc_enabled is True
    assert settings.ui_oidc_config["redirectUri"] == "http://localhost:5174/auth/callback"
    assert settings.ui_oidc_config["postLogoutRedirectUri"] == "http://localhost:5174/auth/logout-complete"


def test_deployed_runtime_requires_api_oidc_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.delenv("API_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("API_OIDC_AUDIENCE", raising=False)

    with pytest.raises(
        ValueError,
        match="Deployed runtime requires API OIDC configuration or explicitly enabled break-glass password auth.",
    ):
        ServiceSettings.from_env()


def test_deployed_runtime_accepts_break_glass_password_auth_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", "test-session-secret-key-value-at-least-32-chars")
    _configure_break_glass(monkeypatch)

    settings = ServiceSettings.from_env()

    assert settings.password_auth.enabled is True
    assert settings.ui_auth_provider == "password"
    assert settings.auth_session_mode == "cookie"
    assert settings.auth_required is True
    assert settings.password_auth.session_roles == ["AssetAllocation.System.Read"]
    assert settings.password_auth.allowed_cidrs == ["127.0.0.1/32"]


def test_ui_auth_provider_oidc_requires_cookie_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_browser_oidc(monkeypatch)
    monkeypatch.setenv("UI_OIDC_REDIRECT_URI", "https://asset-allocation.example.com/auth/callback")
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "bearer")

    with pytest.raises(ValueError, match="UI_AUTH_PROVIDER=oidc requires API_AUTH_SESSION_MODE=cookie."):
        ServiceSettings.from_env()


def test_break_glass_requires_explicit_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for("operator-secret"))
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED", "true")
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS", "127.0.0.1/32")
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_EXPIRES_AT", "2099-01-01T00:00:00Z")
    monkeypatch.setenv("UI_BREAK_GLASS_PASSWORD_ROLES", "AssetAllocation.System.Read")

    with pytest.raises(ValueError, match="UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED requires UI_AUTH_PROVIDER=password."):
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


def test_system_access_role_defaults() -> None:
    settings = ServiceSettings.from_env()

    assert settings.system_access.read_required_roles == ["AssetAllocation.System.Read"]
    assert settings.system_access.logs_read_required_roles == ["AssetAllocation.System.Logs.Read"]
    assert settings.system_access.operate_required_roles == ["AssetAllocation.System.Operate"]
    assert settings.system_access.runtime_config_write_required_roles == [
        "AssetAllocation.RuntimeConfig.Write",
    ]
    assert settings.system_access.job_operate_required_roles == ["AssetAllocation.Jobs.Operate"]
    assert settings.system_access.purge_write_required_roles == ["AssetAllocation.Purge.Write"]


def test_system_access_roles_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_OPERATE_REQUIRED_ROLES", "Ops.One,Ops.Two")
    monkeypatch.setenv("JOB_OPERATE_REQUIRED_ROLES", "Jobs.One")
    monkeypatch.setenv("PURGE_WRITE_REQUIRED_ROLES", "Purge.One")

    settings = ServiceSettings.from_env()

    assert settings.system_access.operate_required_roles == ["Ops.One", "Ops.Two"]
    assert settings.system_access.job_operate_required_roles == ["Jobs.One"]
    assert settings.system_access.purge_write_required_roles == ["Purge.One"]


def test_quiver_requires_api_key_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.delenv("QUIVER_API_KEY", raising=False)

    with pytest.raises(ValueError, match="QUIVER_API_KEY is required"):
        ServiceSettings.from_env()


def test_quiver_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")
    monkeypatch.setenv("QUIVER_BASE_URL", "https://api.quiverquant.com")
    monkeypatch.setenv("QUIVER_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("QUIVER_RATE_LIMIT_PER_MIN", "60")
    monkeypatch.setenv("QUIVER_MAX_CONCURRENCY", "4")
    monkeypatch.setenv("QUIVER_MAX_RETRIES", "5")
    monkeypatch.setenv("QUIVER_BACKOFF_BASE_SECONDS", "1.5")
    monkeypatch.setenv("QUIVER_REQUIRED_ROLES", "AssetAllocation.Quiver.Read,AssetAllocation.Admin")

    settings = ServiceSettings.from_env()

    assert settings.quiver.enabled is True
    assert settings.quiver.api_key == "quiver-key"
    assert settings.quiver.base_url == "https://api.quiverquant.com"
    assert settings.quiver.timeout_seconds == pytest.approx(45.0)
    assert settings.quiver.rate_limit_per_min == 60
    assert settings.quiver.max_concurrency == 4
    assert settings.quiver.max_retries == 5
    assert settings.quiver.backoff_base_seconds == pytest.approx(1.5)
    assert settings.quiver.required_roles == ["AssetAllocation.Quiver.Read", "AssetAllocation.Admin"]


@pytest.mark.parametrize(
    ("env_name", "env_value", "expected_message"),
    [
        ("QUIVER_TIMEOUT_SECONDS", "not-a-number", "QUIVER_TIMEOUT_SECONDS must be a number."),
        ("QUIVER_RATE_LIMIT_PER_MIN", "not-an-int", "QUIVER_RATE_LIMIT_PER_MIN must be an integer."),
    ],
)
def test_quiver_settings_reject_invalid_numeric_values(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    env_value: str,
    expected_message: str,
) -> None:
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValueError, match=expected_message):
        ServiceSettings.from_env()


def test_etrade_trading_requires_etrade_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETRADE_TRADING_ENABLED", "true")
    monkeypatch.delenv("ETRADE_ENABLED", raising=False)

    with pytest.raises(ValueError, match="ETRADE_TRADING_ENABLED requires ETRADE_ENABLED=true."):
        ServiceSettings.from_env()


def test_etrade_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETRADE_ENABLED", "true")
    monkeypatch.setenv("ETRADE_TRADING_ENABLED", "true")
    monkeypatch.setenv("ETRADE_CALLBACK_URL", "http://localhost:8000/api/providers/etrade/connect/callback")
    monkeypatch.setenv("ETRADE_TIMEOUT_SECONDS", "20")
    monkeypatch.setenv("ETRADE_READ_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("ETRADE_READ_RETRY_BASE_DELAY_SECONDS", "1.5")
    monkeypatch.setenv("ETRADE_PENDING_AUTH_TTL_SECONDS", "300")
    monkeypatch.setenv("ETRADE_PREVIEW_TTL_SECONDS", "180")
    monkeypatch.setenv("ETRADE_IDLE_RENEW_SECONDS", "7200")
    monkeypatch.setenv("ETRADE_SESSION_EXPIRY_GUARD_SECONDS", "420")
    monkeypatch.setenv("ETRADE_REQUIRED_ROLES", "AssetAllocation.ETrade.Read")
    monkeypatch.setenv("ETRADE_TRADING_REQUIRED_ROLES", "AssetAllocation.ETrade.Trade,AssetAllocation.Admin")
    monkeypatch.setenv("ETRADE_SANDBOX_CONSUMER_KEY", "sandbox-key")
    monkeypatch.setenv("ETRADE_SANDBOX_CONSUMER_SECRET", "sandbox-secret")
    monkeypatch.setenv("ETRADE_LIVE_CONSUMER_KEY", "live-key")
    monkeypatch.setenv("ETRADE_LIVE_CONSUMER_SECRET", "live-secret")

    settings = ServiceSettings.from_env()

    assert settings.etrade.enabled is True
    assert settings.etrade.trading_enabled is True
    assert settings.etrade.callback_url == "http://localhost:8000/api/providers/etrade/connect/callback"
    assert settings.etrade.timeout_seconds == pytest.approx(20.0)
    assert settings.etrade.read_retry_attempts == 3
    assert settings.etrade.read_retry_base_delay_seconds == pytest.approx(1.5)
    assert settings.etrade.pending_auth_ttl_seconds == 300
    assert settings.etrade.preview_ttl_seconds == 180
    assert settings.etrade.idle_renew_seconds == 7200
    assert settings.etrade.session_expiry_guard_seconds == 420
    assert settings.etrade.required_roles == ["AssetAllocation.ETrade.Read"]
    assert settings.etrade.trading_required_roles == [
        "AssetAllocation.ETrade.Trade",
        "AssetAllocation.Admin",
    ]
    assert settings.etrade.sandbox_consumer_key == "sandbox-key"
    assert settings.etrade.live_consumer_secret == "live-secret"


def test_schwab_trading_requires_schwab_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHWAB_TRADING_ENABLED", "true")
    monkeypatch.delenv("SCHWAB_ENABLED", raising=False)

    with pytest.raises(ValueError, match="SCHWAB_TRADING_ENABLED requires SCHWAB_ENABLED=true."):
        ServiceSettings.from_env()


def test_schwab_settings_require_client_credentials_together(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHWAB_CLIENT_ID", "client-id")
    monkeypatch.delenv("SCHWAB_CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET are required together."):
        ServiceSettings.from_env()


def test_schwab_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("SCHWAB_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("SCHWAB_REQUIRED_ROLES", "AssetAllocation.Schwab.Read")
    monkeypatch.setenv("SCHWAB_TRADING_REQUIRED_ROLES", "AssetAllocation.Schwab.Trade,AssetAllocation.Admin")
    monkeypatch.setenv("SCHWAB_CLIENT_ID", "client-id")
    monkeypatch.setenv("SCHWAB_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SCHWAB_ACCESS_TOKEN", "access-token")
    monkeypatch.setenv("SCHWAB_REFRESH_TOKEN", "refresh-token")

    settings = ServiceSettings.from_env()

    assert settings.schwab.enabled is True
    assert settings.schwab.trading_enabled is True
    assert settings.schwab.callback_url == "https://api.example.com/api/providers/schwab/connect/callback"
    assert settings.schwab.timeout_seconds == pytest.approx(45.0)
    assert settings.schwab.required_roles == ["AssetAllocation.Schwab.Read"]
    assert settings.schwab.trading_required_roles == [
        "AssetAllocation.Schwab.Trade",
        "AssetAllocation.Admin",
    ]
    assert settings.schwab.client_id == "client-id"
    assert settings.schwab.client_secret == "client-secret"
    assert not hasattr(settings.schwab, "access_token")
    assert not hasattr(settings.schwab, "refresh_token")


def test_alpaca_settings_allow_unconfigured_environments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPACA_PAPER_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_PAPER_SECRET_KEY", raising=False)
    monkeypatch.delenv("ALPACA_LIVE_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_LIVE_SECRET_KEY", raising=False)

    settings = ServiceSettings.from_env()

    assert settings.alpaca.paper_configured is False
    assert settings.alpaca.live_configured is False


def test_alpaca_settings_require_paper_credentials_together(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPACA_PAPER_API_KEY_ID", "paper-key")
    monkeypatch.delenv("ALPACA_PAPER_SECRET_KEY", raising=False)

    with pytest.raises(ValueError, match="ALPACA_PAPER_API_KEY_ID and ALPACA_PAPER_SECRET_KEY are required together."):
        ServiceSettings.from_env()


def test_alpaca_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPACA_TIMEOUT_SECONDS", "12")
    monkeypatch.setenv("ALPACA_MAX_RETRIES", "3")
    monkeypatch.setenv("ALPACA_BACKOFF_BASE_SECONDS", "0.5")
    monkeypatch.setenv("ALPACA_REQUIRED_ROLES", "AssetAllocation.Alpaca.Read")
    monkeypatch.setenv("ALPACA_TRADING_REQUIRED_ROLES", "AssetAllocation.Alpaca.Trade,AssetAllocation.Admin")
    monkeypatch.setenv("ALPACA_PAPER_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_PAPER_SECRET_KEY", "paper-secret")
    monkeypatch.setenv("ALPACA_PAPER_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("ALPACA_LIVE_API_KEY_ID", "live-key")
    monkeypatch.setenv("ALPACA_LIVE_SECRET_KEY", "live-secret")
    monkeypatch.setenv("ALPACA_LIVE_TRADING_BASE_URL", "https://api.alpaca.markets")

    settings = ServiceSettings.from_env()

    assert settings.alpaca.timeout_seconds == pytest.approx(12.0)
    assert settings.alpaca.max_retries == 3
    assert settings.alpaca.backoff_base_seconds == pytest.approx(0.5)
    assert settings.alpaca.required_roles == ["AssetAllocation.Alpaca.Read"]
    assert settings.alpaca.trading_required_roles == [
        "AssetAllocation.Alpaca.Trade",
        "AssetAllocation.Admin",
    ]
    assert settings.alpaca.paper_configured is True
    assert settings.alpaca.live_configured is True
    assert settings.alpaca.paper_trading_base_url == "https://paper-api.alpaca.markets"
    assert settings.alpaca.live_trading_base_url == "https://api.alpaca.markets"


def test_kalshi_settings_allow_unconfigured_environments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KALSHI_LIVE_API_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_LIVE_PRIVATE_KEY_PEM", raising=False)

    settings = ServiceSettings.from_env()

    assert settings.kalshi.demo_configured is False
    assert settings.kalshi.live_configured is False


def test_kalshi_trading_requires_kalshi_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_TRADING_ENABLED", "true")
    monkeypatch.delenv("KALSHI_ENABLED", raising=False)

    with pytest.raises(ValueError, match="KALSHI_TRADING_ENABLED requires KALSHI_ENABLED=true."):
        ServiceSettings.from_env()


def test_kalshi_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TRADING_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TIMEOUT_SECONDS", "20")
    monkeypatch.setenv("KALSHI_READ_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("KALSHI_READ_RETRY_BASE_DELAY_SECONDS", "1.5")
    monkeypatch.setenv("KALSHI_REQUIRED_ROLES", "AssetAllocation.Kalshi.Read")
    monkeypatch.setenv("KALSHI_TRADING_REQUIRED_ROLES", "AssetAllocation.Kalshi.Trade,AssetAllocation.Admin")
    monkeypatch.setenv("KALSHI_LIVE_API_KEY_ID", "live-key")
    monkeypatch.setenv("KALSHI_LIVE_PRIVATE_KEY_PEM", "-----BEGIN PRIVATE KEY-----\\nlive\\n-----END PRIVATE KEY-----")
    monkeypatch.setenv("KALSHI_LIVE_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")

    settings = ServiceSettings.from_env()

    assert settings.kalshi.enabled is True
    assert settings.kalshi.trading_enabled is True
    assert settings.kalshi.timeout_seconds == pytest.approx(20.0)
    assert settings.kalshi.read_retry_attempts == 3
    assert settings.kalshi.read_retry_base_delay_seconds == pytest.approx(1.5)
    assert settings.kalshi.required_roles == ["AssetAllocation.Kalshi.Read"]
    assert settings.kalshi.trading_required_roles == [
        "AssetAllocation.Kalshi.Trade",
        "AssetAllocation.Admin",
    ]
    assert settings.kalshi.demo_configured is False
    assert settings.kalshi.live_configured is True
    assert settings.kalshi.live_base_url == "https://api.elections.kalshi.com/trade-api/v2"


def test_api_public_base_url_accepts_origin_without_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")

    settings = ServiceSettings.from_env()

    assert settings.api_public_base_url == "https://api.example.com"


def test_api_public_base_url_rejects_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com/root")

    with pytest.raises(ValueError, match="API_PUBLIC_BASE_URL must be an absolute http\\(s\\) origin"):
        ServiceSettings.from_env()


def test_ui_auth_provider_password_requires_cookie_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_break_glass(monkeypatch)
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "bearer")

    with pytest.raises(ValueError, match="UI_AUTH_PROVIDER=password requires API_AUTH_SESSION_MODE=cookie."):
        ServiceSettings.from_env()


def test_provider_callback_urls_derive_from_public_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")

    settings = ServiceSettings.from_env()

    assert settings.api_root_prefix == "/asset-allocation"
    assert settings.get_provider_callback_path("etrade") == "/asset-allocation/api/providers/etrade/connect/callback"
    assert settings.get_provider_callback_url("etrade") == (
        "https://api.example.com/asset-allocation/api/providers/etrade/connect/callback"
    )
    assert settings.get_provider_callback_url("schwab") == (
        "https://api.example.com/asset-allocation/api/providers/schwab/connect/callback"
    )


def test_etrade_callback_url_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("ETRADE_CALLBACK_URL", "https://override.example.com/etrade/callback")

    settings = ServiceSettings.from_env()

    assert settings.get_provider_callback_url("etrade") == "https://override.example.com/etrade/callback"


def test_schwab_placeholder_callback_url_falls_back_to_derived_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("SCHWAB_APP_CALLBACK_URL", "https://api.example.com/")

    settings = ServiceSettings.from_env()

    assert settings.get_provider_callback_url("schwab") == "https://api.example.com/api/providers/schwab/connect/callback"


def test_notification_settings_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("NOTIFICATIONS_READ_REQUIRED_ROLES", "AssetAllocation.Notifications.Read,AssetAllocation.Admin")
    monkeypatch.setenv("NOTIFICATIONS_WRITE_REQUIRED_ROLES", "AssetAllocation.Notifications.Write")
    monkeypatch.setenv("NOTIFICATIONS_DELIVERY_PROVIDER", "log")
    monkeypatch.setenv("NOTIFICATIONS_APP_BASE_URL", "https://app.example.com")
    monkeypatch.setenv("NOTIFICATIONS_ACTION_PATH_TEMPLATE", "/trade-approvals/{token}")
    monkeypatch.setenv("NOTIFICATIONS_DEFAULT_TRADE_APPROVAL_TTL_SECONDS", "600")
    monkeypatch.setenv("NOTIFICATIONS_TOKEN_HASH_SECRET", "hash-secret")

    settings = ServiceSettings.from_env()

    assert settings.notifications.read_required_roles == [
        "AssetAllocation.Notifications.Read",
        "AssetAllocation.Admin",
    ]
    assert settings.notifications.write_required_roles == ["AssetAllocation.Notifications.Write"]
    assert settings.notifications.delivery_provider == "log"
    assert settings.notifications.app_base_url == "https://app.example.com"
    assert settings.notifications.action_path_template == "/trade-approvals/{token}"
    assert settings.notifications.default_trade_approval_ttl_seconds == 600
    assert settings.notifications.token_hash_secret == "hash-secret"


def test_notification_settings_default_app_base_url_uses_api_public_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")
    monkeypatch.delenv("NOTIFICATIONS_APP_BASE_URL", raising=False)

    settings = ServiceSettings.from_env()

    assert settings.notifications.app_base_url == "https://api.example.com"


def test_notification_settings_require_token_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFICATIONS_ACTION_PATH_TEMPLATE", "/trade-approvals/static")

    with pytest.raises(ValueError, match="NOTIFICATIONS_ACTION_PATH_TEMPLATE must include"):
        ServiceSettings.from_env()


def test_notification_acs_provider_requires_connection_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFICATIONS_DELIVERY_PROVIDER", "acs")
    monkeypatch.delenv("NOTIFICATIONS_ACS_CONNECTION_STRING", raising=False)

    with pytest.raises(ValueError, match="NOTIFICATIONS_ACS_CONNECTION_STRING is required"):
        ServiceSettings.from_env()
