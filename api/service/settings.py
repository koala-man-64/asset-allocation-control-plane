from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Literal, Optional
from urllib.parse import urlparse, urlunparse

from api.service.password_auth import validate_password_hash_format

AuthMode = Literal["anonymous", "oidc", "password"]
AuthSessionMode = Literal["bearer", "cookie"]
UiAuthProvider = Literal["disabled", "oidc", "password"]
ProviderName = Literal["etrade", "schwab"]
_FIXED_UI_API_BASE_URL = "/api"
_LOCAL_RUNTIME_MARKER_ENV_VARS = (
    "CONTAINER_APP_ENV_DNS_SUFFIX",
    "CONTAINER_APP_JOB_EXECUTION_NAME",
    "CONTAINER_APP_REPLICA_NAME",
    "KUBERNETES_SERVICE_HOST",
)
_AI_RELAY_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}
_PROVIDER_CALLBACK_BASE_PATHS: dict[ProviderName, str] = {
    "etrade": "/api/providers/etrade/connect/callback",
    "schwab": "/api/providers/schwab/connect/callback",
}


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


def _get_auth_session_mode() -> AuthSessionMode:
    raw = (_get_optional_str("API_AUTH_SESSION_MODE") or "bearer").lower()
    if raw not in {"bearer", "cookie"}:
        raise ValueError("API_AUTH_SESSION_MODE must be either 'bearer' or 'cookie'.")
    return raw  # type: ignore[return-value]


def _get_ui_auth_provider() -> UiAuthProvider | None:
    raw = _get_optional_str("UI_AUTH_PROVIDER")
    if raw is None:
        return None
    normalized = raw.lower()
    if normalized not in {"disabled", "oidc", "password"}:
        raise ValueError("UI_AUTH_PROVIDER must be one of: disabled, oidc, password.")
    return normalized  # type: ignore[return-value]


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


def _normalize_root_prefix(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


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


def _validate_ui_post_logout_redirect_uri(value: str) -> str:
    return _validate_absolute_http_url(value, env_name="UI_OIDC_POST_LOGOUT_REDIRECT_URI")


def _validate_absolute_http_origin(value: str, *, env_name: str) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"http", "https"} or not host:
        raise ValueError(f"{env_name} must be an absolute http(s) origin without path, query, or fragment.")
    if parsed.scheme != "https" and host not in {"localhost", "127.0.0.1"}:
        raise ValueError(f"{env_name} must use https unless targeting localhost.")

    normalized_path = "/" if not (parsed.path or "").strip() else parsed.path
    if normalized_path != "/" or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(f"{env_name} must be an absolute http(s) origin without path, query, or fragment.")

    return urlunparse(parsed._replace(path="", params="", query="", fragment="")).rstrip("/")


def _normalize_path(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw or raw == "/":
        return "/"
    return "/" + raw.strip("/")


def _build_provider_callback_path(provider: ProviderName, *, api_root_prefix: str) -> str:
    return f"{api_root_prefix}{_PROVIDER_CALLBACK_BASE_PATHS[provider]}" if api_root_prefix else _PROVIDER_CALLBACK_BASE_PATHS[provider]


def _build_provider_callback_url(
    provider: ProviderName,
    *,
    api_root_prefix: str,
    api_public_base_url: str | None,
) -> str | None:
    if not api_public_base_url:
        return None
    return f"{api_public_base_url}{_build_provider_callback_path(provider, api_root_prefix=api_root_prefix)}"


def _is_schwab_callback_placeholder(value: str, *, api_root_prefix: str) -> bool:
    parsed = urlparse(value)
    if parsed.params or parsed.query or parsed.fragment:
        return False

    normalized_path = _normalize_path(parsed.path)
    placeholder_paths = {"/"}
    if api_root_prefix:
        placeholder_paths.add(api_root_prefix)
        placeholder_paths.add(f"{api_root_prefix}/api")
    else:
        placeholder_paths.add("/api")
    return normalized_path in placeholder_paths


def _resolve_provider_callback_url(
    provider: ProviderName,
    *,
    api_root_prefix: str,
    api_public_base_url: str | None,
    override_url: str | None,
    override_env_name: str,
    allow_placeholder_override: bool = False,
) -> str | None:
    if override_url:
        validated_override = _validate_absolute_http_url(override_url, env_name=override_env_name)
        if not (allow_placeholder_override and _is_schwab_callback_placeholder(validated_override, api_root_prefix=api_root_prefix)):
            return validated_override
    return _build_provider_callback_url(
        provider,
        api_root_prefix=api_root_prefix,
        api_public_base_url=api_public_base_url,
    )


def _derive_ui_post_logout_redirect_uri(redirect_uri: str | None) -> str | None:
    if not redirect_uri:
        return None

    parsed = urlparse(redirect_uri)
    if not parsed.scheme or not parsed.netloc:
        return None

    return urlunparse(parsed._replace(path="/auth/logout-complete", params="", query="", fragment=""))


def _validate_cidr_list(values: list[str], *, env_name: str) -> list[str]:
    normalized: list[str] = []
    for item in values:
        try:
            network = ipaddress.ip_network(item, strict=False)
        except ValueError as exc:
            raise ValueError(f"{env_name} contains an invalid CIDR: {item}.") from exc
        normalized.append(str(network))
    return normalized


def _parse_iso8601_timestamp(value: str, *, env_name: str) -> int:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{env_name} must be an ISO-8601 timestamp.")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be an ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{env_name} must include a timezone offset.")
    return int(parsed.astimezone(timezone.utc).timestamp())


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
    def from_env(
        *,
        api_root_prefix: str = "",
        api_public_base_url: str | None = None,
    ) -> "ETradeSettings":
        callback_url = _resolve_provider_callback_url(
            "etrade",
            api_root_prefix=api_root_prefix,
            api_public_base_url=api_public_base_url,
            override_url=_get_optional_str("ETRADE_CALLBACK_URL"),
            override_env_name="ETRADE_CALLBACK_URL",
        )

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
class SchwabSettings:
    enabled: bool = False
    trading_enabled: bool = False
    callback_url: Optional[str] = None
    timeout_seconds: float = 30.0
    required_roles: list[str] = field(default_factory=list)
    trading_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Schwab.Trade"])
    client_id: Optional[str] = None
    client_secret: Optional[str] = None

    @staticmethod
    def from_env(
        *,
        api_root_prefix: str = "",
        api_public_base_url: str | None = None,
    ) -> "SchwabSettings":
        callback_url = _resolve_provider_callback_url(
            "schwab",
            api_root_prefix=api_root_prefix,
            api_public_base_url=api_public_base_url,
            override_url=_get_optional_str("SCHWAB_APP_CALLBACK_URL"),
            override_env_name="SCHWAB_APP_CALLBACK_URL",
            allow_placeholder_override=True,
        )

        settings = SchwabSettings(
            enabled=_get_optional_bool("SCHWAB_ENABLED", default=False),
            trading_enabled=_get_optional_bool("SCHWAB_TRADING_ENABLED", default=False),
            callback_url=callback_url,
            timeout_seconds=_get_optional_float(
                "SCHWAB_TIMEOUT_SECONDS",
                default=30.0,
                minimum=1.0,
                maximum=300.0,
            ),
            required_roles=_split_csv(_get_optional_str("SCHWAB_REQUIRED_ROLES")),
            trading_required_roles=_split_csv(_get_optional_str("SCHWAB_TRADING_REQUIRED_ROLES"))
            or ["AssetAllocation.Schwab.Trade"],
            client_id=_get_optional_str("SCHWAB_CLIENT_ID"),
            client_secret=_get_optional_str("SCHWAB_CLIENT_SECRET"),
        )
        if settings.trading_enabled and not settings.enabled:
            raise ValueError("SCHWAB_TRADING_ENABLED requires SCHWAB_ENABLED=true.")
        if bool(settings.client_id) != bool(settings.client_secret):
            raise ValueError("SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET are required together.")
        return settings


@dataclass(frozen=True)
class PasswordAuthSettings:
    enabled: bool = False
    verifier: Optional[str] = None
    session_subject: str = "break-glass-operator"
    session_display_name: str = "Break Glass Operator"
    session_username: str = "break-glass"
    session_roles: list[str] = field(default_factory=list)
    allowed_cidrs: list[str] = field(default_factory=list)
    expires_at_epoch: int | None = None
    rate_limit_window_seconds: int = 300
    rate_limit_max_attempts_per_ip: int = 10
    rate_limit_max_attempts_global: int = 200

    @staticmethod
    def from_env() -> "PasswordAuthSettings":
        verifier = _get_optional_str("UI_SHARED_PASSWORD_HASH")
        break_glass_enabled = _get_optional_bool("UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED", default=False)
        session_roles = _split_csv(_get_optional_str("UI_BREAK_GLASS_PASSWORD_ROLES"))
        allowed_cidrs = _validate_cidr_list(
            _split_csv(_get_optional_str("UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS")),
            env_name="UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS",
        )
        expires_at_raw = _get_optional_str("UI_BREAK_GLASS_PASSWORD_EXPIRES_AT")
        expires_at_epoch = (
            _parse_iso8601_timestamp(expires_at_raw, env_name="UI_BREAK_GLASS_PASSWORD_EXPIRES_AT")
            if expires_at_raw
            else None
        )
        settings = PasswordAuthSettings(
            enabled=bool(verifier) and break_glass_enabled,
            verifier=verifier,
            session_subject=_get_optional_str("UI_SHARED_PASSWORD_SUBJECT") or "break-glass-operator",
            session_display_name=_get_optional_str("UI_SHARED_PASSWORD_DISPLAY_NAME") or "Break Glass Operator",
            session_username=_get_optional_str("UI_SHARED_PASSWORD_USERNAME") or "break-glass",
            session_roles=session_roles,
            allowed_cidrs=allowed_cidrs,
            expires_at_epoch=expires_at_epoch,
            rate_limit_window_seconds=_get_optional_int(
                "UI_PASSWORD_RATE_LIMIT_WINDOW_SECONDS",
                default=300,
                minimum=1,
                maximum=86_400,
            ),
            rate_limit_max_attempts_per_ip=_get_optional_int(
                "UI_PASSWORD_RATE_LIMIT_MAX_ATTEMPTS_PER_IP",
                default=10,
                minimum=1,
                maximum=1_000,
            ),
            rate_limit_max_attempts_global=_get_optional_int(
                "UI_PASSWORD_RATE_LIMIT_MAX_ATTEMPTS_GLOBAL",
                default=200,
                minimum=1,
                maximum=10_000,
            ),
        )
        if verifier:
            validate_password_hash_format(settings.verifier)
        if break_glass_enabled and not verifier:
            raise ValueError("UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED requires UI_SHARED_PASSWORD_HASH.")
        if break_glass_enabled and not settings.session_roles:
            raise ValueError("UI_BREAK_GLASS_PASSWORD_ROLES is required when UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED=true.")
        if break_glass_enabled and not settings.allowed_cidrs:
            raise ValueError(
                "UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS is required when UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED=true."
            )
        if break_glass_enabled and settings.expires_at_epoch is None:
            raise ValueError("UI_BREAK_GLASS_PASSWORD_EXPIRES_AT is required when UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED=true.")
        if settings.expires_at_epoch is not None and settings.expires_at_epoch <= int(datetime.now(timezone.utc).timestamp()):
            raise ValueError("UI_BREAK_GLASS_PASSWORD_EXPIRES_AT must be in the future.")
        return settings


@dataclass(frozen=True)
class AlpacaSettings:
    timeout_seconds: float = 10.0
    max_retries: int = 2
    backoff_base_seconds: float = 0.25
    required_roles: list[str] = field(default_factory=list)
    trading_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Alpaca.Trade"])
    paper_api_key_id: Optional[str] = None
    paper_secret_key: Optional[str] = None
    paper_trading_base_url: str = "https://paper-api.alpaca.markets"
    live_api_key_id: Optional[str] = None
    live_secret_key: Optional[str] = None
    live_trading_base_url: str = "https://api.alpaca.markets"

    @property
    def paper_configured(self) -> bool:
        return bool(self.paper_api_key_id and self.paper_secret_key)

    @property
    def live_configured(self) -> bool:
        return bool(self.live_api_key_id and self.live_secret_key)

    @staticmethod
    def from_env() -> "AlpacaSettings":
        settings = AlpacaSettings(
            timeout_seconds=_get_optional_float(
                "ALPACA_TIMEOUT_SECONDS",
                default=10.0,
                minimum=1.0,
                maximum=300.0,
            ),
            max_retries=_get_optional_int(
                "ALPACA_MAX_RETRIES",
                default=2,
                minimum=0,
                maximum=10,
            ),
            backoff_base_seconds=_get_optional_float(
                "ALPACA_BACKOFF_BASE_SECONDS",
                default=0.25,
                minimum=0.0,
                maximum=30.0,
            ),
            required_roles=_split_csv(_get_optional_str("ALPACA_REQUIRED_ROLES")),
            trading_required_roles=_split_csv(_get_optional_str("ALPACA_TRADING_REQUIRED_ROLES"))
            or ["AssetAllocation.Alpaca.Trade"],
            paper_api_key_id=_get_optional_str("ALPACA_PAPER_API_KEY_ID"),
            paper_secret_key=_get_optional_str("ALPACA_PAPER_SECRET_KEY"),
            paper_trading_base_url=_get_optional_str("ALPACA_PAPER_TRADING_BASE_URL")
            or "https://paper-api.alpaca.markets",
            live_api_key_id=_get_optional_str("ALPACA_LIVE_API_KEY_ID"),
            live_secret_key=_get_optional_str("ALPACA_LIVE_SECRET_KEY"),
            live_trading_base_url=_get_optional_str("ALPACA_LIVE_TRADING_BASE_URL")
            or "https://api.alpaca.markets",
        )

        if bool(settings.paper_api_key_id) != bool(settings.paper_secret_key):
            raise ValueError("ALPACA_PAPER_API_KEY_ID and ALPACA_PAPER_SECRET_KEY are required together.")
        if bool(settings.live_api_key_id) != bool(settings.live_secret_key):
            raise ValueError("ALPACA_LIVE_API_KEY_ID and ALPACA_LIVE_SECRET_KEY are required together.")

        return AlpacaSettings(
            timeout_seconds=settings.timeout_seconds,
            max_retries=settings.max_retries,
            backoff_base_seconds=settings.backoff_base_seconds,
            required_roles=settings.required_roles,
            trading_required_roles=settings.trading_required_roles,
            paper_api_key_id=settings.paper_api_key_id,
            paper_secret_key=settings.paper_secret_key,
            paper_trading_base_url=_validate_absolute_http_url(
                settings.paper_trading_base_url,
                env_name="ALPACA_PAPER_TRADING_BASE_URL",
            ),
            live_api_key_id=settings.live_api_key_id,
            live_secret_key=settings.live_secret_key,
            live_trading_base_url=_validate_absolute_http_url(
                settings.live_trading_base_url,
                env_name="ALPACA_LIVE_TRADING_BASE_URL",
            ),
        )


@dataclass(frozen=True)
class KalshiSettings:
    enabled: bool = False
    trading_enabled: bool = False
    timeout_seconds: float = 15.0
    read_retry_attempts: int = 2
    read_retry_base_delay_seconds: float = 1.0
    required_roles: list[str] = field(default_factory=list)
    trading_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Kalshi.Trade"])
    live_api_key_id: Optional[str] = None
    live_private_key_pem: Optional[str] = None
    live_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"

    @property
    def demo_configured(self) -> bool:
        return False

    @property
    def live_configured(self) -> bool:
        return bool(self.live_api_key_id and self.live_private_key_pem)

    @staticmethod
    def from_env() -> "KalshiSettings":
        settings = KalshiSettings(
            enabled=_get_optional_bool("KALSHI_ENABLED", default=False),
            trading_enabled=_get_optional_bool("KALSHI_TRADING_ENABLED", default=False),
            timeout_seconds=_get_optional_float(
                "KALSHI_TIMEOUT_SECONDS",
                default=15.0,
                minimum=1.0,
                maximum=300.0,
            ),
            read_retry_attempts=_get_optional_int(
                "KALSHI_READ_RETRY_ATTEMPTS",
                default=2,
                minimum=0,
                maximum=10,
            ),
            read_retry_base_delay_seconds=_get_optional_float(
                "KALSHI_READ_RETRY_BASE_DELAY_SECONDS",
                default=1.0,
                minimum=0.0,
                maximum=30.0,
            ),
            required_roles=_split_csv(_get_optional_str("KALSHI_REQUIRED_ROLES")),
            trading_required_roles=_split_csv(_get_optional_str("KALSHI_TRADING_REQUIRED_ROLES"))
            or ["AssetAllocation.Kalshi.Trade"],
            live_api_key_id=_get_optional_str("KALSHI_LIVE_API_KEY_ID"),
            live_private_key_pem=_get_optional_str("KALSHI_LIVE_PRIVATE_KEY_PEM"),
            live_base_url=_get_optional_str("KALSHI_LIVE_BASE_URL")
            or "https://api.elections.kalshi.com/trade-api/v2",
        )

        if settings.trading_enabled and not settings.enabled:
            raise ValueError("KALSHI_TRADING_ENABLED requires KALSHI_ENABLED=true.")
        if bool(settings.live_api_key_id) != bool(settings.live_private_key_pem):
            raise ValueError("KALSHI_LIVE_API_KEY_ID and KALSHI_LIVE_PRIVATE_KEY_PEM are required together.")

        return KalshiSettings(
            enabled=settings.enabled,
            trading_enabled=settings.trading_enabled,
            timeout_seconds=settings.timeout_seconds,
            read_retry_attempts=settings.read_retry_attempts,
            read_retry_base_delay_seconds=settings.read_retry_base_delay_seconds,
            required_roles=settings.required_roles,
            trading_required_roles=settings.trading_required_roles,
            live_api_key_id=settings.live_api_key_id,
            live_private_key_pem=settings.live_private_key_pem,
            live_base_url=_validate_absolute_http_url(settings.live_base_url, env_name="KALSHI_LIVE_BASE_URL"),
        )


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
class DataDiscoverySettings:
    required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.DataDiscovery.Read"])
    write_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.DataDiscovery.Write"])
    visible_schemas: list[str] = field(default_factory=lambda: ["core", "gold", "platinum"])
    sample_max_limit: int = 25
    cache_ttl_seconds: float = 30.0

    @staticmethod
    def from_env() -> "DataDiscoverySettings":
        visible_schemas = _split_csv(_get_optional_str("DATA_DISCOVERY_VISIBLE_SCHEMAS")) or [
            "core",
            "gold",
            "platinum",
        ]
        normalized_visible_schemas = list(
            dict.fromkeys(str(schema or "").strip().lower() for schema in visible_schemas if str(schema or "").strip())
        )
        if not normalized_visible_schemas:
            raise ValueError("DATA_DISCOVERY_VISIBLE_SCHEMAS must include at least one schema.")

        return DataDiscoverySettings(
            required_roles=_split_csv(_get_optional_str("DATA_DISCOVERY_REQUIRED_ROLES"))
            or ["AssetAllocation.DataDiscovery.Read"],
            write_required_roles=_split_csv(_get_optional_str("DATA_DISCOVERY_WRITE_REQUIRED_ROLES"))
            or ["AssetAllocation.DataDiscovery.Write"],
            visible_schemas=normalized_visible_schemas,
            sample_max_limit=_get_optional_int(
                "DATA_DISCOVERY_SAMPLE_MAX_LIMIT",
                default=25,
                minimum=1,
                maximum=250,
            ),
            cache_ttl_seconds=_get_optional_float(
                "DATA_DISCOVERY_CACHE_TTL_SECONDS",
                default=30.0,
                minimum=0.0,
                maximum=3600.0,
            ),
        )


@dataclass(frozen=True)
class SystemAccessSettings:
    read_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.System.Read"])
    logs_read_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.System.Logs.Read"])
    operate_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.System.Operate"])
    runtime_config_write_required_roles: list[str] = field(
        default_factory=lambda: ["AssetAllocation.RuntimeConfig.Write"]
    )
    job_operate_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Jobs.Operate"])
    purge_write_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Purge.Write"])

    @staticmethod
    def from_env() -> "SystemAccessSettings":
        return SystemAccessSettings(
            read_required_roles=_split_csv(_get_optional_str("SYSTEM_READ_REQUIRED_ROLES"))
            or ["AssetAllocation.System.Read"],
            logs_read_required_roles=_split_csv(_get_optional_str("SYSTEM_LOGS_READ_REQUIRED_ROLES"))
            or ["AssetAllocation.System.Logs.Read"],
            operate_required_roles=_split_csv(_get_optional_str("SYSTEM_OPERATE_REQUIRED_ROLES"))
            or ["AssetAllocation.System.Operate"],
            runtime_config_write_required_roles=_split_csv(
                _get_optional_str("RUNTIME_CONFIG_WRITE_REQUIRED_ROLES")
            )
            or ["AssetAllocation.RuntimeConfig.Write"],
            job_operate_required_roles=_split_csv(_get_optional_str("JOB_OPERATE_REQUIRED_ROLES"))
            or ["AssetAllocation.Jobs.Operate"],
            purge_write_required_roles=_split_csv(_get_optional_str("PURGE_WRITE_REQUIRED_ROLES"))
            or ["AssetAllocation.Purge.Write"],
        )


@dataclass(frozen=True)
class BrokerAccountPolicySettings:
    read_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.AccountPolicy.Read"])
    write_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.AccountPolicy.Write"])
    trade_confirmation_release_required_roles: list[str] = field(
        default_factory=lambda: ["AssetAllocation.TradeConfirmation.Release"]
    )

    @staticmethod
    def from_env() -> "BrokerAccountPolicySettings":
        return BrokerAccountPolicySettings(
            read_required_roles=_split_csv(_get_optional_str("BROKER_ACCOUNT_POLICY_READ_REQUIRED_ROLES"))
            or ["AssetAllocation.AccountPolicy.Read"],
            write_required_roles=_split_csv(_get_optional_str("BROKER_ACCOUNT_POLICY_WRITE_REQUIRED_ROLES"))
            or ["AssetAllocation.AccountPolicy.Write"],
            trade_confirmation_release_required_roles=_split_csv(
                _get_optional_str("TRADE_CONFIRMATION_RELEASE_REQUIRED_ROLES")
            )
            or ["AssetAllocation.TradeConfirmation.Release"],
        )


@dataclass(frozen=True)
class TradeDeskSettings:
    read_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.TradeDesk.Read"])
    preview_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.TradeDesk.Preview"])
    place_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.TradeDesk.Place"])
    cancel_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.TradeDesk.Cancel"])
    live_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.TradeDesk.Live"])
    global_kill_switch: bool = False
    live_kill_switch: bool = True
    paper_execution_enabled: bool = False
    sandbox_execution_enabled: bool = False
    live_execution_enabled: bool = False
    simulated_execution_enabled: bool = False
    provider_kill_switches: list[str] = field(default_factory=list)
    account_kill_switches: list[str] = field(default_factory=list)
    account_allowlist: list[str] = field(default_factory=list)
    live_account_allowlist: list[str] = field(default_factory=list)
    max_order_notional: float = 50_000.0
    data_max_age_seconds: int = 300

    @staticmethod
    def from_env() -> "TradeDeskSettings":
        return TradeDeskSettings(
            read_required_roles=_split_csv(_get_optional_str("TRADE_DESK_READ_REQUIRED_ROLES"))
            or ["AssetAllocation.TradeDesk.Read"],
            preview_required_roles=_split_csv(_get_optional_str("TRADE_DESK_PREVIEW_REQUIRED_ROLES"))
            or ["AssetAllocation.TradeDesk.Preview"],
            place_required_roles=_split_csv(_get_optional_str("TRADE_DESK_PLACE_REQUIRED_ROLES"))
            or ["AssetAllocation.TradeDesk.Place"],
            cancel_required_roles=_split_csv(_get_optional_str("TRADE_DESK_CANCEL_REQUIRED_ROLES"))
            or ["AssetAllocation.TradeDesk.Cancel"],
            live_required_roles=_split_csv(_get_optional_str("TRADE_DESK_LIVE_REQUIRED_ROLES"))
            or ["AssetAllocation.TradeDesk.Live"],
            global_kill_switch=_get_optional_bool("TRADE_DESK_GLOBAL_KILL_SWITCH", default=False),
            live_kill_switch=_get_optional_bool("TRADE_DESK_LIVE_KILL_SWITCH", default=True),
            paper_execution_enabled=_get_optional_bool("TRADE_DESK_PAPER_EXECUTION_ENABLED", default=False),
            sandbox_execution_enabled=_get_optional_bool("TRADE_DESK_SANDBOX_EXECUTION_ENABLED", default=False),
            live_execution_enabled=_get_optional_bool("TRADE_DESK_LIVE_EXECUTION_ENABLED", default=False),
            simulated_execution_enabled=_get_optional_bool("TRADE_DESK_SIMULATED_EXECUTION_ENABLED", default=False),
            provider_kill_switches=[
                provider.strip().lower()
                for provider in _split_csv(_get_optional_str("TRADE_DESK_PROVIDER_KILL_SWITCHES"))
                if provider.strip()
            ],
            account_kill_switches=_split_csv(_get_optional_str("TRADE_DESK_ACCOUNT_KILL_SWITCHES")),
            account_allowlist=_split_csv(_get_optional_str("TRADE_DESK_ACCOUNT_ALLOWLIST")),
            live_account_allowlist=_split_csv(_get_optional_str("TRADE_DESK_LIVE_ACCOUNT_ALLOWLIST")),
            max_order_notional=_get_optional_float(
                "TRADE_DESK_MAX_ORDER_NOTIONAL",
                default=50_000.0,
                minimum=1.0,
                maximum=1_000_000_000.0,
            ),
            data_max_age_seconds=_get_optional_int(
                "TRADE_DESK_DATA_MAX_AGE_SECONDS",
                default=300,
                minimum=1,
                maximum=86_400,
            ),
        )


@dataclass(frozen=True)
class NotificationSettings:
    read_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Notifications.Read"])
    write_required_roles: list[str] = field(default_factory=lambda: ["AssetAllocation.Notifications.Write"])
    delivery_provider: Literal["log", "acs"] = "log"
    acs_connection_string: Optional[str] = None
    acs_email_sender: Optional[str] = None
    acs_sms_from: Optional[str] = None
    app_base_url: Optional[str] = None
    action_path_template: str = "/notifications/actions/{token}"
    default_trade_approval_ttl_seconds: int = 300
    token_hash_secret: Optional[str] = None

    @staticmethod
    def from_env(*, api_public_base_url: str | None) -> "NotificationSettings":
        delivery_provider = (_get_optional_str("NOTIFICATIONS_DELIVERY_PROVIDER") or "log").lower()
        if delivery_provider not in {"log", "acs"}:
            raise ValueError("NOTIFICATIONS_DELIVERY_PROVIDER must be either 'log' or 'acs'.")

        app_base_url = _get_optional_str("NOTIFICATIONS_APP_BASE_URL") or api_public_base_url
        if app_base_url:
            app_base_url = _validate_absolute_http_origin(app_base_url, env_name="NOTIFICATIONS_APP_BASE_URL")

        action_path_template = _normalize_path(
            _get_optional_str("NOTIFICATIONS_ACTION_PATH_TEMPLATE") or "/notifications/actions/{token}"
        )
        if "{token}" not in action_path_template:
            raise ValueError("NOTIFICATIONS_ACTION_PATH_TEMPLATE must include '{token}'.")

        settings = NotificationSettings(
            read_required_roles=_split_csv(_get_optional_str("NOTIFICATIONS_READ_REQUIRED_ROLES"))
            or ["AssetAllocation.Notifications.Read"],
            write_required_roles=_split_csv(_get_optional_str("NOTIFICATIONS_WRITE_REQUIRED_ROLES"))
            or ["AssetAllocation.Notifications.Write"],
            delivery_provider=delivery_provider,  # type: ignore[arg-type]
            acs_connection_string=_get_optional_str("NOTIFICATIONS_ACS_CONNECTION_STRING"),
            acs_email_sender=_get_optional_str("NOTIFICATIONS_ACS_EMAIL_SENDER"),
            acs_sms_from=_get_optional_str("NOTIFICATIONS_ACS_SMS_FROM"),
            app_base_url=app_base_url,
            action_path_template=action_path_template,
            default_trade_approval_ttl_seconds=_get_optional_int(
                "NOTIFICATIONS_DEFAULT_TRADE_APPROVAL_TTL_SECONDS",
                default=300,
                minimum=30,
                maximum=86_400,
            ),
            token_hash_secret=_get_optional_str("NOTIFICATIONS_TOKEN_HASH_SECRET"),
        )
        if settings.delivery_provider == "acs" and not settings.acs_connection_string:
            raise ValueError("NOTIFICATIONS_ACS_CONNECTION_STRING is required when NOTIFICATIONS_DELIVERY_PROVIDER=acs.")
        return settings


@dataclass(frozen=True)
class ServiceSettings:
    api_root_prefix: str
    api_public_base_url: Optional[str]
    oidc_auth_enabled: bool
    anonymous_local_auth_enabled: bool
    oidc_issuer: Optional[str]
    oidc_audience: List[str]
    oidc_jwks_url: Optional[str]
    oidc_required_scopes: List[str]
    oidc_required_roles: List[str]
    postgres_dsn: Optional[str]
    browser_oidc_enabled: bool = False
    ui_oidc_config: dict[str, Any] = field(default_factory=dict)
    ui_auth_provider: UiAuthProvider = "disabled"
    auth_session_mode: AuthSessionMode = "bearer"
    auth_session_idle_ttl_seconds: int = 1_800
    auth_session_absolute_ttl_seconds: int = 28_800
    auth_session_version: str = "1"
    auth_session_secret_keys: list[str] = field(default_factory=list)
    auth_session_cookie_secure: bool = True
    auth_session_cookie_name: str = "__Host-aa_session"
    auth_session_csrf_cookie_name: str = "__Host-aa_csrf"
    password_auth: PasswordAuthSettings = field(default_factory=PasswordAuthSettings)
    ai_relay: AiRelaySettings = field(default_factory=AiRelaySettings)
    quiver: QuiverSettings = field(default_factory=QuiverSettings)
    etrade: ETradeSettings = field(default_factory=ETradeSettings)
    alpaca: AlpacaSettings = field(default_factory=AlpacaSettings)
    kalshi: KalshiSettings = field(default_factory=KalshiSettings)
    schwab: SchwabSettings = field(default_factory=SchwabSettings)
    schwab_callback_url: Optional[str] = None
    symbol_enrichment: SymbolEnrichmentSettings = field(default_factory=SymbolEnrichmentSettings)
    intraday_monitor: IntradayMonitorSettings = field(default_factory=IntradayMonitorSettings)
    data_discovery: DataDiscoverySettings = field(default_factory=DataDiscoverySettings)
    system_access: SystemAccessSettings = field(default_factory=SystemAccessSettings)
    broker_account_policy: BrokerAccountPolicySettings = field(default_factory=BrokerAccountPolicySettings)
    trade_desk: TradeDeskSettings = field(default_factory=TradeDeskSettings)
    notifications: NotificationSettings = field(default_factory=NotificationSettings)

    @property
    def auth_required(self) -> bool:
        return not self.anonymous_local_auth_enabled

    @property
    def auth_summary(self) -> str:
        modes: list[str] = []
        if self.oidc_auth_enabled:
            modes.append("oidc")
        if self.password_auth.enabled:
            modes.append("password")
        if self.anonymous_local_auth_enabled:
            modes.append("anonymous-local")
        return "+".join(modes) if modes else "disabled"

    @property
    def cookie_auth_sessions_enabled(self) -> bool:
        return self.auth_session_mode == "cookie"

    def get_provider_callback_path(self, provider: ProviderName) -> str:
        return _build_provider_callback_path(provider, api_root_prefix=self.api_root_prefix)

    def get_provider_callback_url(self, provider: ProviderName) -> str | None:
        if provider == "etrade":
            return self.etrade.callback_url
        return self.schwab.callback_url

    @staticmethod
    def from_env() -> "ServiceSettings":
        api_root_prefix = _normalize_root_prefix(_get_optional_str("API_ROOT_PREFIX"))
        api_public_base_url = _get_optional_str("API_PUBLIC_BASE_URL")
        if api_public_base_url:
            api_public_base_url = _validate_absolute_http_origin(
                api_public_base_url,
                env_name="API_PUBLIC_BASE_URL",
            )

        oidc_issuer = _get_optional_str("API_OIDC_ISSUER")
        oidc_audience = _split_csv(_get_optional_str("API_OIDC_AUDIENCE"))
        oidc_jwks_url = _get_optional_str("API_OIDC_JWKS_URL")
        oidc_required_scopes = _split_csv(_get_optional_str("API_OIDC_REQUIRED_SCOPES"))
        oidc_required_roles = _split_csv(_get_optional_str("API_OIDC_REQUIRED_ROLES"))
        auth_session_mode = _get_auth_session_mode()
        auth_session_idle_ttl_seconds = _get_optional_int(
            "API_AUTH_SESSION_IDLE_TTL_SECONDS",
            default=1_800,
            minimum=60,
            maximum=31_536_000,
        )
        auth_session_absolute_ttl_seconds = _get_optional_int(
            "API_AUTH_SESSION_ABSOLUTE_TTL_SECONDS",
            default=28_800,
            minimum=60,
            maximum=31_536_000,
        )
        if auth_session_absolute_ttl_seconds < auth_session_idle_ttl_seconds:
            raise ValueError(
                "API_AUTH_SESSION_ABSOLUTE_TTL_SECONDS must be greater than or equal to "
                "API_AUTH_SESSION_IDLE_TTL_SECONDS."
            )
        auth_session_secret_keys = _split_csv(_get_optional_str("API_AUTH_SESSION_SECRET_KEYS"))
        auth_session_version = _get_optional_str("API_AUTH_SESSION_VERSION") or "1"

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
        password_auth = PasswordAuthSettings.from_env()
        if auth_session_mode == "cookie" and not auth_session_secret_keys:
            raise ValueError("API_AUTH_SESSION_SECRET_KEYS is required when API_AUTH_SESSION_MODE=cookie.")

        configured_ui_authority = _get_optional_str("UI_OIDC_AUTHORITY")
        ui_authority = configured_ui_authority or oidc_issuer
        ui_client_id = _get_optional_str("UI_OIDC_CLIENT_ID")
        ui_scopes = _get_optional_str("UI_OIDC_SCOPES")
        ui_redirect_uri = _get_optional_str("UI_OIDC_REDIRECT_URI")
        ui_post_logout_redirect_uri = _get_optional_str("UI_OIDC_POST_LOGOUT_REDIRECT_URI")
        browser_oidc_inputs_present = bool(
            configured_ui_authority or ui_client_id or ui_scopes or ui_redirect_uri or ui_post_logout_redirect_uri
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
        if ui_post_logout_redirect_uri:
            ui_post_logout_redirect_uri = _validate_ui_post_logout_redirect_uri(ui_post_logout_redirect_uri)

        configured_ui_auth_provider = _get_ui_auth_provider()
        if configured_ui_auth_provider is not None:
            ui_auth_provider = configured_ui_auth_provider
        elif browser_oidc_enabled:
            ui_auth_provider = "oidc"
        else:
            ui_auth_provider = "disabled"

        if ui_auth_provider == "oidc" and not browser_oidc_enabled:
            raise ValueError("UI_AUTH_PROVIDER=oidc requires browser OIDC configuration.")
        if ui_auth_provider == "oidc" and auth_session_mode != "cookie":
            raise ValueError("UI_AUTH_PROVIDER=oidc requires API_AUTH_SESSION_MODE=cookie.")
        if ui_auth_provider == "password" and not password_auth.enabled:
            raise ValueError("UI_AUTH_PROVIDER=password requires explicitly enabled break-glass password auth.")
        if ui_auth_provider == "password" and auth_session_mode != "cookie":
            raise ValueError("UI_AUTH_PROVIDER=password requires API_AUTH_SESSION_MODE=cookie.")
        if password_auth.enabled and ui_auth_provider != "password":
            raise ValueError("UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED requires UI_AUTH_PROVIDER=password.")

        anonymous_local_auth_enabled = False
        if not oidc_auth_enabled and not password_auth.enabled:
            if _is_local_runtime():
                anonymous_local_auth_enabled = True
            else:
                raise ValueError(
                    "Deployed runtime requires API OIDC configuration or explicitly enabled break-glass password auth."
                )
        if auth_session_mode == "cookie" and not (oidc_auth_enabled or password_auth.enabled):
            raise ValueError("Cookie auth sessions require API OIDC auth or explicitly enabled break-glass password auth.")

        postgres_dsn = _get_optional_str("POSTGRES_DSN")
        ai_relay = AiRelaySettings.from_env()
        quiver = QuiverSettings.from_env()
        alpaca = AlpacaSettings.from_env()
        kalshi = KalshiSettings.from_env()
        etrade = ETradeSettings.from_env(
            api_root_prefix=api_root_prefix,
            api_public_base_url=api_public_base_url,
        )
        schwab = SchwabSettings.from_env(
            api_root_prefix=api_root_prefix,
            api_public_base_url=api_public_base_url,
        )
        symbol_enrichment = SymbolEnrichmentSettings.from_env()
        intraday_monitor = IntradayMonitorSettings.from_env()
        data_discovery = DataDiscoverySettings.from_env()
        system_access = SystemAccessSettings.from_env()
        broker_account_policy = BrokerAccountPolicySettings.from_env()
        trade_desk = TradeDeskSettings.from_env()
        notifications = NotificationSettings.from_env(api_public_base_url=api_public_base_url)
        ui_oidc_config = {
            "authority": ui_authority,
            "clientId": ui_client_id,
            "scope": ui_scopes,
            "redirectUri": ui_redirect_uri,
            "postLogoutRedirectUri": ui_post_logout_redirect_uri or _derive_ui_post_logout_redirect_uri(ui_redirect_uri),
            "apiBaseUrl": _FIXED_UI_API_BASE_URL,
            "authSessionMode": auth_session_mode,
            "authProvider": ui_auth_provider,
        }
        local_runtime = _is_local_runtime()

        return ServiceSettings(
            api_root_prefix=api_root_prefix,
            api_public_base_url=api_public_base_url,
            oidc_auth_enabled=oidc_auth_enabled,
            anonymous_local_auth_enabled=anonymous_local_auth_enabled,
            oidc_issuer=oidc_issuer,
            oidc_audience=oidc_audience,
            oidc_jwks_url=oidc_jwks_url,
            oidc_required_scopes=oidc_required_scopes,
            oidc_required_roles=oidc_required_roles,
            postgres_dsn=postgres_dsn,
            ui_auth_provider=ui_auth_provider,
            browser_oidc_enabled=browser_oidc_enabled,
            ui_oidc_config=ui_oidc_config,
            auth_session_mode=auth_session_mode,
            auth_session_idle_ttl_seconds=auth_session_idle_ttl_seconds,
            auth_session_absolute_ttl_seconds=auth_session_absolute_ttl_seconds,
            auth_session_version=auth_session_version,
            auth_session_secret_keys=auth_session_secret_keys,
            auth_session_cookie_secure=not local_runtime,
            auth_session_cookie_name="aa_session_dev" if local_runtime else "__Host-aa_session",
            auth_session_csrf_cookie_name="aa_csrf_dev" if local_runtime else "__Host-aa_csrf",
            password_auth=password_auth,
            ai_relay=ai_relay,
            quiver=quiver,
            etrade=etrade,
            alpaca=alpaca,
            kalshi=kalshi,
            schwab=schwab,
            schwab_callback_url=schwab.callback_url,
            symbol_enrichment=symbol_enrichment,
            intraday_monitor=intraday_monitor,
            data_discovery=data_discovery,
            system_access=system_access,
            broker_account_policy=broker_account_policy,
            trade_desk=trade_desk,
            notifications=notifications,
        )
