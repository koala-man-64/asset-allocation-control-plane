from __future__ import annotations

from dataclasses import dataclass


_DEFAULT_AUTHORIZE_URL = "https://us.etrade.com/e/t/etws/authorize"
_DEFAULT_AUTH_BASE_URL = "https://api.etrade.com"
_DEFAULT_LIVE_API_BASE_URL = "https://api.etrade.com"
_DEFAULT_SANDBOX_API_BASE_URL = "https://apisb.etrade.com"


@dataclass(frozen=True)
class ETradeEnvironmentConfig:
    environment: str
    consumer_key: str | None
    consumer_secret: str | None
    api_base_url: str
    auth_base_url: str = _DEFAULT_AUTH_BASE_URL
    authorize_url: str = _DEFAULT_AUTHORIZE_URL

    @property
    def request_token_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/oauth/request_token"

    @property
    def access_token_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/oauth/access_token"

    @property
    def renew_access_token_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/oauth/renew_access_token"

    @property
    def revoke_access_token_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/oauth/revoke_access_token"

    @property
    def is_configured(self) -> bool:
        return bool((self.consumer_key or "").strip() and (self.consumer_secret or "").strip())


@dataclass(frozen=True)
class ETradeConfig:
    sandbox: ETradeEnvironmentConfig
    live: ETradeEnvironmentConfig
    timeout_seconds: float = 15.0
    read_retry_attempts: int = 2
    read_retry_base_delay_seconds: float = 1.0

    @staticmethod
    def default() -> "ETradeConfig":
        return ETradeConfig(
            sandbox=ETradeEnvironmentConfig(
                environment="sandbox",
                consumer_key=None,
                consumer_secret=None,
                api_base_url=_DEFAULT_SANDBOX_API_BASE_URL,
            ),
            live=ETradeEnvironmentConfig(
                environment="live",
                consumer_key=None,
                consumer_secret=None,
                api_base_url=_DEFAULT_LIVE_API_BASE_URL,
            ),
        )

    def for_environment(self, environment: str) -> ETradeEnvironmentConfig:
        normalized = str(environment or "").strip().lower()
        if normalized == "sandbox":
            return self.sandbox
        if normalized == "live":
            return self.live
        raise ValueError(f"Unsupported E*TRADE environment={environment!r}.")
