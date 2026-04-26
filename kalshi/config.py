from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional


def normalize_private_key_pem(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if "\\n" in normalized and "\n" not in normalized:
        normalized = normalized.replace("\\n", "\n")
    return normalized


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: float = 15.0
    read_retry_attempts: int = 2
    read_retry_base_s: float = 1.0


@dataclass(frozen=True)
class KalshiEnvironmentConfig:
    environment: Literal["demo", "live"]
    api_key_id: Optional[str] = None
    private_key_pem: Optional[str] = None
    base_url: Optional[str] = None
    http: HttpConfig = field(default_factory=HttpConfig)

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key_id and self.private_key_pem)

    def get_api_key_id(self) -> str | None:
        value = str(self.api_key_id or "").strip()
        return value or None

    def get_private_key_pem(self) -> str | None:
        return normalize_private_key_pem(self.private_key_pem)

    def get_base_url(self) -> str:
        if self.base_url:
            return str(self.base_url).rstrip("/")
        if self.environment == "demo":
            return "https://demo-api.kalshi.co/trade-api/v2"
        return "https://api.elections.kalshi.com/trade-api/v2"
