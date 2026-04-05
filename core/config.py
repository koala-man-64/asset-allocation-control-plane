from __future__ import annotations

import os
import json
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def parse_debug_symbols(value: object) -> list[str]:
    if value is None:
        return []

    def normalize_symbol_token(raw_token: object) -> str | None:
        token = str(raw_token).strip()
        if not token:
            return None
        return token.upper()

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []

        # Accept either JSON array syntax or a comma-separated string.
        if raw.startswith("["):
            try:
                decoded = json.loads(raw)
            except Exception:
                decoded = None
            else:
                if isinstance(decoded, list):
                    symbols: list[str] = []
                    for item in decoded:
                        token = normalize_symbol_token(item)
                        if token:
                            symbols.append(token)
                    return symbols

        symbols = []
        for item in raw.split(","):
            token = normalize_symbol_token(item)
            if token:
                symbols.append(token)
        return symbols

    if isinstance(value, list):
        symbols = []
        for item in value:
            token = normalize_symbol_token(item)
            if token:
                symbols.append(token)
        return symbols

    token = normalize_symbol_token(value)
    return [token] if token else []


if not _is_truthy(os.environ.get("DISABLE_DOTENV")):
    load_dotenv(override=False)


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required.")
    return value.strip()


def require_env_bool(name: str) -> bool:
    raw = require_env(name).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {raw!r}")


class AppSettings(BaseSettings):
    # NOTE: We disable env value decoding so list-like settings (e.g. DEBUG_SYMBOLS)
    # can be provided as a simple comma-separated string (AAPL,MSFT) without requiring
    # JSON array syntax. This also avoids failures when the env var is present but empty.
    model_config = SettingsConfigDict(extra="ignore", enable_decoding=False)

    # Basic runtime metadata.
    domain: str = "asset_allocation"
    env: str = "dev"

    # Logging (used by core.logging_config).
    log_level: str = "INFO"

    # Azure Storage auth (at least one required).
    AZURE_STORAGE_ACCOUNT_NAME: Optional[str] = None
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None

    # Azure container routing.
    #
    # Defaults match the deployment manifests. Override via env vars when running
    # against a different storage account/container topology.
    AZURE_FOLDER_MARKET: str = "market-data"
    AZURE_FOLDER_FINANCE: str = "finance-data"
    AZURE_FOLDER_EARNINGS: str = "earnings-data"
    AZURE_FOLDER_TARGETS: str = "price-target-data"
    AZURE_CONTAINER_COMMON: str = "common"
    AZURE_CONTAINER_BRONZE: str = "bronze"
    AZURE_CONTAINER_SILVER: str = "silver"
    AZURE_CONTAINER_GOLD: str = "gold"
    AZURE_CONTAINER_PLATINUM: Optional[str] = "platinum"
    
    # API Configuration
    API_PORT: int = os.environ.get("API_PORT", 9000)

    # Alpha Vantage (market + fundamentals data source).
    ALPHA_VANTAGE_API_KEY: Optional[str] = None
    ALPHA_VANTAGE_RATE_LIMIT_PER_MIN: int = 300
    ALPHA_VANTAGE_TIMEOUT_SECONDS: float = 15.0
    ALPHA_VANTAGE_MAX_WORKERS: int = 32
    ALPHA_VANTAGE_EARNINGS_FRESH_DAYS: int = 7
    ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON: str = "12month"
    ALPHA_VANTAGE_FINANCE_FRESH_DAYS: int = 28

    # Massive (market + fundamentals replacement at Bronze market/finance layers).
    MASSIVE_API_KEY: Optional[str] = None
    MASSIVE_BASE_URL: str = "https://api.massive.com"
    MASSIVE_TIMEOUT_SECONDS: float = 30.0
    MASSIVE_MAX_WORKERS: int = 32
    MASSIVE_TICKERS_PAGE_LIMIT: int = 1000
    MASSIVE_FINANCE_FRESH_DAYS: int = 28

    # Bronze/Silver/Gold storage layout strategy (alpha26-only).
    BRONZE_LAYOUT_MODE: str = "alpha26"
    BRONZE_ALPHA26_FORCE_REBUILD: bool = True
    BRONZE_ALPHA26_CODEC: str = "snappy"
    SILVER_LAYOUT_MODE: str = "alpha26"
    SILVER_ALPHA26_FORCE_REBUILD: bool = True
    GOLD_LAYOUT_MODE: str = "alpha26"

    # Comma-separated list for debug runs (e.g., "AAPL,MSFT"). Empty disables filtering.
    # Note: ETL jobs may override this from Postgres at startup.
    DEBUG_SYMBOLS: list[str] = Field(default_factory=list)

    @field_validator("DEBUG_SYMBOLS", mode="before")
    @classmethod
    def _parse_debug_symbols(cls, value):
        return parse_debug_symbols(value)

    @field_validator("BRONZE_LAYOUT_MODE", "SILVER_LAYOUT_MODE", "GOLD_LAYOUT_MODE", mode="before")
    @classmethod
    def _validate_alpha26_layout_mode(cls, value: object) -> str:
        mode = str(value or "").strip().lower()
        if mode != "alpha26":
            raise ValueError("Only alpha26 storage layout mode is supported.")
        return mode

    @model_validator(mode="after")
    def _validate_storage_auth(self):
        # Storage credentials are only required for workflows that actually read/write to Azure.
        # Allow FastAPI/local tooling to start without Azure configured.
        if _is_truthy(os.environ.get("ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE")):
            if not self.AZURE_STORAGE_ACCOUNT_NAME and not self.AZURE_STORAGE_CONNECTION_STRING:
                raise ValueError("AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING is required.")
        return self


settings = AppSettings()

def _apply_settings(new_settings: AppSettings) -> None:
    # These module-level constants are used widely across ETL jobs.
    # Keeping them in one place lets us "reload" settings after applying DB-backed env overrides.
    global settings
    global AZURE_STORAGE_ACCOUNT_NAME
    global AZURE_STORAGE_CONNECTION_STRING
    global AZURE_FOLDER_MARKET
    global AZURE_FOLDER_FINANCE
    global AZURE_FOLDER_EARNINGS
    global AZURE_FOLDER_TARGETS
    global AZURE_CONTAINER_COMMON
    global AZURE_CONTAINER_BRONZE
    global AZURE_CONTAINER_SILVER
    global AZURE_CONTAINER_GOLD
    global AZURE_CONTAINER_PLATINUM
    global ALPHA_VANTAGE_API_KEY
    global ALPHA_VANTAGE_RATE_LIMIT_PER_MIN
    global ALPHA_VANTAGE_TIMEOUT_SECONDS
    global ALPHA_VANTAGE_MAX_WORKERS
    global ALPHA_VANTAGE_EARNINGS_FRESH_DAYS
    global ALPHA_VANTAGE_FINANCE_FRESH_DAYS
    global MASSIVE_API_KEY
    global MASSIVE_BASE_URL
    global MASSIVE_TIMEOUT_SECONDS
    global MASSIVE_MAX_WORKERS
    global MASSIVE_TICKERS_PAGE_LIMIT
    global MASSIVE_FINANCE_FRESH_DAYS
    global BRONZE_LAYOUT_MODE
    global BRONZE_ALPHA26_FORCE_REBUILD
    global BRONZE_ALPHA26_CODEC
    global SILVER_LAYOUT_MODE
    global SILVER_ALPHA26_FORCE_REBUILD
    global GOLD_LAYOUT_MODE
    global DEBUG_SYMBOLS

    settings = new_settings

    AZURE_STORAGE_ACCOUNT_NAME = settings.AZURE_STORAGE_ACCOUNT_NAME
    AZURE_STORAGE_CONNECTION_STRING = settings.AZURE_STORAGE_CONNECTION_STRING

    AZURE_FOLDER_MARKET = settings.AZURE_FOLDER_MARKET
    AZURE_FOLDER_FINANCE = settings.AZURE_FOLDER_FINANCE
    AZURE_FOLDER_EARNINGS = settings.AZURE_FOLDER_EARNINGS
    AZURE_FOLDER_TARGETS = settings.AZURE_FOLDER_TARGETS
    AZURE_CONTAINER_COMMON = settings.AZURE_CONTAINER_COMMON
    AZURE_CONTAINER_BRONZE = settings.AZURE_CONTAINER_BRONZE
    AZURE_CONTAINER_SILVER = settings.AZURE_CONTAINER_SILVER
    AZURE_CONTAINER_GOLD = settings.AZURE_CONTAINER_GOLD
    AZURE_CONTAINER_PLATINUM = settings.AZURE_CONTAINER_PLATINUM

    ALPHA_VANTAGE_API_KEY = settings.ALPHA_VANTAGE_API_KEY
    ALPHA_VANTAGE_RATE_LIMIT_PER_MIN = settings.ALPHA_VANTAGE_RATE_LIMIT_PER_MIN
    ALPHA_VANTAGE_TIMEOUT_SECONDS = settings.ALPHA_VANTAGE_TIMEOUT_SECONDS
    ALPHA_VANTAGE_MAX_WORKERS = settings.ALPHA_VANTAGE_MAX_WORKERS
    ALPHA_VANTAGE_EARNINGS_FRESH_DAYS = settings.ALPHA_VANTAGE_EARNINGS_FRESH_DAYS
    ALPHA_VANTAGE_FINANCE_FRESH_DAYS = settings.ALPHA_VANTAGE_FINANCE_FRESH_DAYS

    MASSIVE_API_KEY = settings.MASSIVE_API_KEY
    MASSIVE_BASE_URL = settings.MASSIVE_BASE_URL
    MASSIVE_TIMEOUT_SECONDS = settings.MASSIVE_TIMEOUT_SECONDS
    MASSIVE_MAX_WORKERS = settings.MASSIVE_MAX_WORKERS
    MASSIVE_TICKERS_PAGE_LIMIT = settings.MASSIVE_TICKERS_PAGE_LIMIT
    MASSIVE_FINANCE_FRESH_DAYS = settings.MASSIVE_FINANCE_FRESH_DAYS

    BRONZE_LAYOUT_MODE = settings.BRONZE_LAYOUT_MODE
    BRONZE_ALPHA26_FORCE_REBUILD = settings.BRONZE_ALPHA26_FORCE_REBUILD
    BRONZE_ALPHA26_CODEC = settings.BRONZE_ALPHA26_CODEC
    SILVER_LAYOUT_MODE = settings.SILVER_LAYOUT_MODE
    SILVER_ALPHA26_FORCE_REBUILD = settings.SILVER_ALPHA26_FORCE_REBUILD
    GOLD_LAYOUT_MODE = settings.GOLD_LAYOUT_MODE

    DEBUG_SYMBOLS = settings.DEBUG_SYMBOLS


def reload_settings() -> AppSettings:
    """
    Re-read `AppSettings` from the current environment and update module-level constants.

    This is intentionally lightweight (no dotenv reload) and is used after applying
    DB-backed runtime-config overrides so downstream code sees the latest values.
    """
    new_settings = AppSettings()
    _apply_settings(new_settings)
    return new_settings


_apply_settings(settings)

EARNINGS_DATA_PREFIX: str = "earnings-data"

TICKERS_TO_ADD = [
    {
        "Symbol": "SPY",
        "Description": "S&P 500 Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "DIA",
        "Description": "Dow Jones Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "QQQ",
        "Description": "Nasdaq Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "^VIX",
        "Description": "Volatility Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "^VIX3M",
        "Description": "3-Month Volatility Index",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "UST",
        "Description": "US Treasury ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "IWC",
        "Description": "Micro Cap ETF",
        "Sector": "Market Analysis",
        "Industry": "Market Cap",
    },
    {
        "Symbol": "VB",
        "Description": "Small Cap ETF",
        "Sector": "Market Analysis",
        "Industry": "Market Cap",
    },
]
