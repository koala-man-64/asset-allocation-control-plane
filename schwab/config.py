"""Configuration for Schwab Trader API access."""

from __future__ import annotations

import os
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
    def from_env(*, require_client_credentials: bool = True) -> "SchwabConfig":
        client_id = _strip_or_none(os.environ.get("SCHWAB_CLIENT_ID"))
        client_secret = _strip_or_none(os.environ.get("SCHWAB_CLIENT_SECRET"))

        if require_client_credentials and not client_id:
            raise ValueError("SCHWAB_CLIENT_ID is required.")
        if require_client_credentials and not client_secret:
            raise ValueError("SCHWAB_CLIENT_SECRET is required.")

        return SchwabConfig(
            client_id=str(client_id or ""),
            client_secret=str(client_secret or ""),
            app_callback_url=str(_strip_or_none(os.environ.get("SCHWAB_APP_CALLBACK_URL")) or ""),
            access_token=str(_strip_or_none(os.environ.get("SCHWAB_ACCESS_TOKEN")) or ""),
            refresh_token=str(_strip_or_none(os.environ.get("SCHWAB_REFRESH_TOKEN")) or ""),
        )

    def get_authorization_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/v1/oauth/authorize"

    def get_token_url(self) -> str:
        return f"{self.auth_base_url.rstrip('/')}/v1/oauth/token"
