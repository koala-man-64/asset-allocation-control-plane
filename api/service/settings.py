from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, List, Literal, Optional
from urllib.parse import urlparse, urlunparse

AuthMode = Literal["anonymous", "oidc"]
_FIXED_UI_API_BASE_URL = "/api"
_LOCAL_RUNTIME_MARKER_ENV_VARS = (
    "CONTAINER_APP_ENV_DNS_SUFFIX",
    "CONTAINER_APP_JOB_EXECUTION_NAME",
    "CONTAINER_APP_REPLICA_NAME",
    "KUBERNETES_SERVICE_HOST",
)
_AI_RELAY_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}


def _split_csv(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _get_optional_str(name: str) -> Optional[str]:
    raw = os.environ.get(name)
    value = raw.strip() if raw else ""
    return value or None


def _get_optional_bool(name: str, *, default: bool = False) -> bool:
    raw = _get_optional_str(name)
    if raw is None:
        return bool(default)
    value = raw.lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


def _get_optional_int(
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = _get_optional_str(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}.")
    return value


def _get_optional_float(
    name: str,
    *,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = _get_optional_str(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number.") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}.")
    return value


def _is_local_runtime() -> bool:
    return not any((os.environ.get(key) or "").strip() for key in _LOCAL_RUNTIME_MARKER_ENV_VARS)


def _validate_absolute_http_url(value: str, *, env_name: str) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"http", "https"} or not host:
        raise ValueError(f"{env_name} must be an absolute http(s) URL.")
    if parsed.scheme != "https" and host not in {"localhost", "127.0.0.1"}:
        raise ValueError(f"{env_name} must use https unless targeting localhost.")
    return value


def _validate_ui_redirect_uri(value: str) -> str:
    return _validate_absolute_http_url(value, env_name="UI_OIDC_REDIRECT_URI")


def _derive_ui_post_logout_redirect_uri(redirect_uri: str | None) -> str | None:
    if not redirect_uri:
        return None

    parsed = urlparse(redirect_uri)
    if not parsed.scheme or not parsed.netloc:
        return None

    return urlunparse(parsed._replace(path="/auth/logout-complete", params="", query="", fragment=""))


@dataclass(frozen=True)
class AiRelaySettings:
    enabled: bool = False
    api_key: Optional[str] = None
    model: str = "gpt-5.4-mini"
    reasoning_effort: str = "low"
    timeout_seconds: float = 120.0
    max_prompt_chars: int = 40_000
    max_files: int = 4
    max_file_bytes: int = 5 * 1024 * 1024
    max_total_file_bytes: int = 20 * 1024 * 1024
    max_output_tokens: int = 4_000
    required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.AiRelay.Use"])

    @staticmethod
    def from_env() -> "AiRelaySettings":
        reasoning_effort = (_get_optional_str("AI_RELAY_REASONING_EFFORT") or "low").lower()
        if reasoning_effort not in _AI_RELAY_REASONING_EFFORTS:
            raise ValueError(
                "AI_RELAY_REASONING_EFFORT must be one of: "
                + ", ".join(sorted(_AI_RELAY_REASONING_EFFORTS))
                + "."
            )

        required_roles = _split_csv(_get_optional_str("AI_RELAY_REQUIRED_ROLES")) or ["AssetAllocation.AiRelay.Use"]
        settings = AiRelaySettings(
            enabled=_get_optional_bool("AI_RELAY_ENABLED", default=False),
            api_key=_get_optional_str("AI_RELAY_API_KEY"),
            model=_get_optional_str("AI_RELAY_MODEL") or "gpt-5.4-mini",
            reasoning_effort=reasoning_effort,
            timeout_seconds=_get_optional_float(
                "AI_RELAY_TIMEOUT_SECONDS",
                default=120.0,
                minimum=1.0,
                maximum=900.0,
            ),
            max_prompt_chars=_get_optional_int(
                "AI_RELAY_MAX_PROMPT_CHARS",
                default=40_000,
                minimum=1,
                maximum=200_000,
            ),
            max_files=_get_optional_int(
                "AI_RELAY_MAX_FILES",
                default=4,
                minimum=0,
                maximum=16,
            ),
            max_file_bytes=_get_optional_int(
                "AI_RELAY_MAX_FILE_BYTES",
                default=5 * 1024 * 1024,
                minimum=1,
                maximum=50 * 1024 * 1024,
            ),
            max_total_file_bytes=_get_optional_int(
                "AI_RELAY_MAX_TOTAL_FILE_BYTES",
                default=20 * 1024 * 1024,
                minimum=1,
                maximum=50 * 1024 * 1024,
            ),
            max_output_tokens=_get_optional_int(
                "AI_RELAY_MAX_OUTPUT_TOKENS",
                default=4_000,
                minimum=1,
                maximum=32_000,
            ),
            required_roles=required_roles,
        )
        if settings.max_total_file_bytes < settings.max_file_bytes:
            raise ValueError("AI_RELAY_MAX_TOTAL_FILE_BYTES must be greater than or equal to AI_RELAY_MAX_FILE_BYTES.")
        return settings


@dataclass(frozen=True)
class QuiverSettings:
    enabled: bool = False
    api_key: Optional[str] = None
    base_url: str = "https://api.quiverquant.com"
    timeout_seconds: float = 30.0
    rate_limit_per_min: int = 30
    max_concurrency: int = 2
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Quiver.Read"])

    @staticmethod
    def from_env() -> "QuiverSettings":
        settings = QuiverSettings(
            enabled=_get_optional_bool("QUIVER_ENABLED", default=False),
            api_key=_get_optional_str("QUIVER_API_KEY"),
            base_url=_get_optional_str("QUIVER_BASE_URL") or "https://api.quiverquant.com",
            timeout_seconds=_get_optional_float(
                "QUIVER_TIMEOUT_SECONDS",
                default=30.0,
                minimum=1.0,
                maximum=300.0,
            ),
            rate_limit_per_min=_get_optional_int(
                "QUIVER_RATE_LIMIT_PER_MIN",
                default=30,
                minimum=1,
                maximum=1_000,
            ),
            max_concurrency=_get_optional_int(
                "QUIVER_MAX_CONCURRENCY",
                default=2,
                minimum=1,
                maximum=32,
            ),
            max_retries=_get_optional_int(
                "QUIVER_MAX_RETRIES",
                default=3,
                minimum=0,
                maximum=10,
            ),
            backoff_base_seconds=_get_optional_float(
                "QUIVER_BACKOFF_BASE_SECONDS",
                default=1.0,
                minimum=0.0,
                maximum=30.0,
            ),
            required_roles=_split_csv(_get_optional_str("QUIVER_REQUIRED_ROLES")) or ["AssetAllocation.Quiver.Read"],
        )
        if settings.enabled and not settings.api_key:
            raise ValueError("QUIVER_API_KEY is required when QUIVER_ENABLED=true.")
        return QuiverSettings(
            enabled=settings.enabled,
            api_key=settings.api_key,
            base_url=_validate_absolute_http_url(settings.base_url, env_name="QUIVER_BASE_URL"),
            timeout_seconds=settings.timeout_seconds,
            rate_limit_per_min=settings.rate_limit_per_min,
            max_concurrency=settings.max_concurrency,
            max_retries=settings.max_retries,
            backoff_base_seconds=settings.backoff_base_seconds,
            required_roles=settings.required_roles,
        )


@dataclass(frozen=True)
class SymbolEnrichmentSettings:
    enabled: bool = False
    model: str = "gpt-5.4-mini"
    confidence_min: float = 0.7
    max_symbols_per_run: int = 500
    allowed_jobs: list[str] = field(default_factory=list)

    @staticmethod
    def from_env() -> "SymbolEnrichmentSettings":
        settings = SymbolEnrichmentSettings(
            enabled=_get_optional_bool("SYMBOL_ENRICHMENT_ENABLED", default=False),
            model=_get_optional_str("SYMBOL_ENRICHMENT_MODEL") or "gpt-5.4-mini",
            confidence_min=_get_optional_float(
                "SYMBOL_ENRICHMENT_CONFIDENCE_MIN",
                default=0.7,
                minimum=0.0,
                maximum=1.0,
            ),
            max_symbols_per_run=_get_optional_int(
                "SYMBOL_ENRICHMENT_MAX_SYMBOLS_PER_RUN",
                default=500,
                minimum=1,
                maximum=50_000,
            ),
            allowed_jobs=_split_csv(_get_optional_str("SYMBOL_ENRICHMENT_ALLOWED_JOBS")),
        )
        if settings.enabled and not settings.allowed_jobs:
            raise ValueError("SYMBOL_ENRICHMENT_ALLOWED_JOBS is required when SYMBOL_ENRICHMENT_ENABLED=true.")
        return settings


@dataclass(frozen=True)
class ETradeSettings:
    enabled: bool = False
    trading_enabled: bool = False
    callback_url: Optional[str] = None
    timeout_seconds: float = 15.0
    read_retry_attempts: int = 2
    read_retry_base_delay_seconds: float = 1.0
    pending_auth_ttl_seconds: int = 300
    preview_ttl_seconds: int = 180
    idle_renew_seconds: int = 7200
    session_expiry_guard_seconds: int = 300
    required_roles: list[str] = field(default_factory=list)
    trading_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.ETrade.Trade"])
    sandbox_consumer_key: Optional[str] = None
    sandbox_consumer_secret: Optional[str] = None
    live_consumer_key: Optional[str] = None
    live_consumer_secret: Optional[str] = None

    @staticmethod
    def from_env() -> "ETradeSettings":
        callback_url = _get_optional_str("ETRADE_CALLBACK_URL")
        if callback_url:
            callback_url = _validate_absolute_http_url(callback_url, env_name="ETRADE_CALLBACK_URL")

        settings = ETradeSettings(
            enabled=_get_optional_bool("ETRADE_ENABLED", default=False),
            trading_enabled=_get_optional_bool("ETRADE_TRADING_ENABLED", default=False),
            callback_url=callback_url,
            timeout_seconds=_get_optional_float(
                "ETRADE_TIMEOUT_SECONDS",
                default=15.0,
                minimum=1.0,
                maximum=300.0,
            ),
            read_retry_attempts=_get_optional_int(
                "ETRADE_READ_RETRY_ATTEMPTS",
                default=2,
                minimum=1,
                maximum=5,
            ),
            read_retry_base_delay_seconds=_get_optional_float(
                "ETRADE_READ_RETRY_BASE_DELAY_SECONDS",
                default=1.0,
                minimum=0.0,
                maximum=30.0,
            ),
            pending_auth_ttl_seconds=_get_optional_int(
                "ETRADE_PENDING_AUTH_TTL_SECONDS",
                default=300,
                minimum=60,
                maximum=900,
            ),
            preview_ttl_seconds=_get_optional_int(
                "ETRADE_PREVIEW_TTL_SECONDS",
                default=180,
                minimum=60,
                maximum=300,
            ),
            idle_renew_seconds=_get_optional_int(
                "ETRADE_IDLE_RENEW_SECONDS",
                default=7200,
                minimum=3600,
                maximum=7200,
            ),
            session_expiry_guard_seconds=_get_optional_int(
                "ETRADE_SESSION_EXPIRY_GUARD_SECONDS",
                default=300,
                minimum=60,
                maximum=3600,
            ),
            required_roles=_split_csv(_get_optional_str("ETRADE_REQUIRED_ROLES")),
            trading_required_roles=_split_csv(_get_optional_str("ETRADE_TRADING_REQUIRED_ROLES"))
            or ["AssetAllocation.ETrade.Trade"],
            sandbox_consumer_key=_get_optional_str("ETRADE_SANDBOX_CONSUMER_KEY"),
            sandbox_consumer_secret=_get_optional_str("ETRADE_SANDBOX_CONSUMER_SECRET"),
            live_consumer_key=_get_optional_str("ETRADE_LIVE_CONSUMER_KEY"),
            live_consumer_secret=_get_optional_str("ETRADE_LIVE_CONSUMER_SECRET"),
        )
        if settings.trading_enabled and not settings.enabled:
            raise ValueError("ETRADE_TRADING_ENABLED requires ETRADE_ENABLED=true.")
        return settings


@dataclass(frozen=True)
class IntradayMonitorSettings:
    enabled: bool = False
    allowed_jobs: list[str] = field(default_factory=list)
    operator_required_roles: list[str] = field(default_factory=list)
    jobs_required_roles: list[str] = field(default_factory=list)

    @staticmethod
    def from_env() -> "IntradayMonitorSettings":
        settings = IntradayMonitorSettings(
            enabled=_get_optional_bool("INTRADAY_MONITOR_ENABLED", default=False),
            allowed_jobs=_split_csv(_get_optional_str("INTRADAY_MONITOR_ALLOWED_JOBS")),
            operator_required_roles=_split_csv(_get_optional_str("INTRADAY_MONITOR_OPERATOR_REQUIRED_ROLES")),
            jobs_required_roles=_split_csv(_get_optional_str("INTRADAY_MONITOR_JOBS_REQUIRED_ROLES")),
        )
        if settings.enabled and not settings.allowed_jobs:
            raise ValueError("INTRADAY_MONITOR_ALLOWED_JOBS is required when INTRADAY_MONITOR_ENABLED=true.")
        return settings


@dataclass(frozen=True)
class ServiceSettings:
    oidc_auth_enabled: bool
    anonymous_local_auth_enabled: bool
    oidc_issuer: Optional[str]
    oidc_audience: List[str]
    oidc_jwks_url: Optional[str]
    oidc_required_scopes: List[str]
    oidc_required_roles: List[str]
    postgres_dsn: Optional[str]
    browser_oidc_enabled: bool
    ui_oidc_config: dict[str, Any]
    ai_relay: AiRelaySettings = field(default_factory=AiRelaySettings)
    quiver: QuiverSettings = field(default_factory=QuiverSettings)
    etrade: ETradeSettings = field(default_factory=ETradeSettings)
    symbol_enrichment: SymbolEnrichmentSettings = field(default_factory=SymbolEnrichmentSettings)
    intraday_monitor: IntradayMonitorSettings = field(default_factory=IntradayMonitorSettings)

    @property
    def auth_required(self) -> bool:
        return not self.anonymous_local_auth_enabled

    @property
    def auth_summary(self) -> str:
        return "oidc" if self.oidc_auth_enabled else "anonymous-local"

    @staticmethod
    def from_env() -> "ServiceSettings":
        oidc_issuer = _get_optional_str("API_OIDC_ISSUER")
        oidc_audience = _split_csv(_get_optional_str("API_OIDC_AUDIENCE"))
        oidc_jwks_url = _get_optional_str("API_OIDC_JWKS_URL")
        oidc_required_scopes = _split_csv(_get_optional_str("API_OIDC_REQUIRED_SCOPES"))
        oidc_required_roles = _split_csv(_get_optional_str("API_OIDC_REQUIRED_ROLES"))

        oidc_inputs_present = bool(
            oidc_issuer
            or oidc_audience
            or oidc_jwks_url
            or oidc_required_scopes
            or oidc_required_roles
        )
        if oidc_inputs_present and not oidc_issuer:
            raise ValueError("API_OIDC_ISSUER is required when API OIDC auth is configured.")
        if oidc_inputs_present and not oidc_audience:
            raise ValueError("API_OIDC_AUDIENCE is required when API OIDC auth is configured.")
        oidc_auth_enabled = bool(oidc_issuer and oidc_audience)

        configured_ui_authority = _get_optional_str("UI_OIDC_AUTHORITY")
        ui_authority = configured_ui_authority or oidc_issuer
        ui_client_id = _get_optional_str("UI_OIDC_CLIENT_ID")
        ui_scopes = _get_optional_str("UI_OIDC_SCOPES")
        ui_redirect_uri = _get_optional_str("UI_OIDC_REDIRECT_URI")
        browser_oidc_inputs_present = bool(
            configured_ui_authority or ui_client_id or ui_scopes or ui_redirect_uri
        )
        if browser_oidc_inputs_present and not (ui_authority and ui_client_id):
            raise ValueError("UI_OIDC_AUTHORITY and UI_OIDC_CLIENT_ID are required together.")
        browser_oidc_enabled = bool(ui_authority and ui_client_id)
        if browser_oidc_enabled and not oidc_auth_enabled:
            raise ValueError("Browser OIDC requires API OIDC auth to be configured.")
        if browser_oidc_enabled and not ui_redirect_uri:
            raise ValueError("UI_OIDC_REDIRECT_URI is required when browser OIDC is configured.")
        if ui_redirect_uri:
            ui_redirect_uri = _validate_ui_redirect_uri(ui_redirect_uri)

        anonymous_local_auth_enabled = False
        if not oidc_auth_enabled:
            if _is_local_runtime():
                anonymous_local_auth_enabled = True
            else:
                raise ValueError("Deployed runtime requires API OIDC configuration.")

        postgres_dsn = _get_optional_str("POSTGRES_DSN")
        ai_relay = AiRelaySettings.from_env()
        quiver = QuiverSettings.from_env()
        etrade = ETradeSettings.from_env()
        symbol_enrichment = SymbolEnrichmentSettings.from_env()
        intraday_monitor = IntradayMonitorSettings.from_env()
        ui_oidc_config = {
            "authority": ui_authority,
            "clientId": ui_client_id,
            "scope": ui_scopes,
            "redirectUri": ui_redirect_uri,
            "postLogoutRedirectUri": _derive_ui_post_logout_redirect_uri(ui_redirect_uri),
            "apiBaseUrl": _FIXED_UI_API_BASE_URL,
        }

        return ServiceSettings(
            oidc_auth_enabled=oidc_auth_enabled,
            anonymous_local_auth_enabled=anonymous_local_auth_enabled,
            oidc_issuer=oidc_issuer,
            oidc_audience=oidc_audience,
            oidc_jwks_url=oidc_jwks_url,
            oidc_required_scopes=oidc_required_scopes,
            oidc_required_roles=oidc_required_roles,
            postgres_dsn=postgres_dsn,
            browser_oidc_enabled=browser_oidc_enabled,
            ui_oidc_config=ui_oidc_config,
            ai_relay=ai_relay,
            quiver=quiver,
            etrade=etrade,
            symbol_enrichment=symbol_enrichment,
            intraday_monitor=intraday_monitor,
        )
