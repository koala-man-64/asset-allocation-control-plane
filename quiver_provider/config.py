from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_float(name: str, default: float) -> float:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be a number.") from exc


def _env_int(name: str, default: int) -> int:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be an integer.") from exc


@dataclass(frozen=True)
class QuiverConfig:
    api_key: str
    base_url: str = "https://api.quiverquant.com"
    timeout_seconds: float = 30.0
    rate_limit_per_min: int = 30
    max_concurrency: int = 2
    max_retries: int = 3
    backoff_base_seconds: float = 1.0

    @staticmethod
    def from_env(*, require_api_key: bool = True) -> "QuiverConfig":
        api_key = _strip_or_none(os.environ.get("QUIVER_API_KEY"))
        if require_api_key and not api_key:
            raise ValueError("QUIVER_API_KEY is required.")

        return QuiverConfig(
            api_key=str(api_key or ""),
            base_url=_strip_or_none(os.environ.get("QUIVER_BASE_URL")) or "https://api.quiverquant.com",
            timeout_seconds=_env_float("QUIVER_TIMEOUT_SECONDS", 30.0),
            rate_limit_per_min=max(1, _env_int("QUIVER_RATE_LIMIT_PER_MIN", 30)),
            max_concurrency=max(1, _env_int("QUIVER_MAX_CONCURRENCY", 2)),
            max_retries=max(0, _env_int("QUIVER_MAX_RETRIES", 3)),
            backoff_base_seconds=max(0.0, _env_float("QUIVER_BACKOFF_BASE_SECONDS", 1.0)),
        )
