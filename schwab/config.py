"""Configuration for Schwab Trader API access."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional


SCHWAB_AUTH_BASE_URL = "https://api.schwabapi.com"
SCHWAB_TRADER_BASE_URL = "https://api.schwabapi.com/trader/v1"
SCHWAB_TIMEOUT_SECONDS = 30.0


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_or_default(value: object, default: float) -> float:
    text = _strip_or_none(value)
    if text is None:
        return default
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError("SCHWAB_TIMEOUT_SECONDS must be a number.") from exc
    if parsed <= 0:
        raise ValueError("SCHWAB_TIMEOUT_SECONDS must be greater than zero.")
    return parsed


@dataclass(frozen=True)
class SchwabConfig:
    """Runtime configuration for the Schwab Trader API client.

    The user-provided Schwab PDFs describe the credentials as belonging to a
    Dev Portal App subscribed to "Trader API - Individual". The client ID and
    client secret are generated when that App is approved and registered.
    """

    client_id: str
    client_secret: str
    app_callback_url: str = ""
    access_token: str = ""
    refresh_token: str = ""
    auth_base_url: str = SCHWAB_AUTH_BASE_URL
    trader_base_url: str = SCHWAB_TRADER_BASE_URL
    timeout_seconds: float = SCHWAB_TIMEOUT_SECONDS

    @staticmethod
    def from_mapping(
        values: Mapping[str, object],
        *,
        require_client_credentials: bool = True,
    ) -> "SchwabConfig":
        client_id = _strip_or_none(values.get("SCHWAB_CLIENT_ID"))
        client_secret = _strip_or_none(values.get("SCHWAB_CLIENT_SECRET"))

        if require_client_credentials and not client_id:
            raise ValueError("SCHWAB_CLIENT_ID is required.")
        if require_client_credentials and not client_secret:
            raise ValueError("SCHWAB_CLIENT_SECRET is required.")

        return SchwabConfig(
            client_id=str(client_id or ""),
            client_secret=str(client_secret or ""),
            app_callback_url=str(_strip_or_none(values.get("SCHWAB_APP_CALLBACK_URL")) or ""),
            timeout_seconds=_float_or_default(values.get("SCHWAB_TIMEOUT_SECONDS"), SCHWAB_TIMEOUT_SECONDS),
        )

    @staticmethod
    def from_env(*, require_client_credentials: bool = True) -> "SchwabConfig":
        return SchwabConfig.from_mapping(
            os.environ,
            require_client_credentials=require_client_credentials,
        )

    def get_authorization_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/v1/oauth/authorize"

    def get_token_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/v1/oauth/token"
