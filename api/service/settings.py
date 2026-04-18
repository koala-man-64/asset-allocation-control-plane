from __future__ import annotations

import os
from dataclasses import dataclass
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


def _split_csv(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _get_optional_str(name: str) -> Optional[str]:
    raw = os.environ.get(name)
    value = raw.strip() if raw else ""
    return value or None


def _is_local_runtime() -> bool:
    return not any((os.environ.get(key) or "").strip() for key in _LOCAL_RUNTIME_MARKER_ENV_VARS)


def _validate_ui_redirect_uri(value: str) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"http", "https"} or not host:
        raise ValueError("UI_OIDC_REDIRECT_URI must be an absolute http(s) URL.")
    if parsed.scheme != "https" and host not in {"localhost", "127.0.0.1"}:
        raise ValueError("UI_OIDC_REDIRECT_URI must use https unless targeting localhost.")
    return value


def _derive_ui_post_logout_redirect_uri(redirect_uri: str | None) -> str | None:
    if not redirect_uri:
        return None

    parsed = urlparse(redirect_uri)
    if not parsed.scheme or not parsed.netloc:
        return None

    return urlunparse(parsed._replace(path="/auth/logout-complete", params="", query="", fragment=""))


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
        )
