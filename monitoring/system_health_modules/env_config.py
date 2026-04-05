from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


def _require_env(name: str) -> str:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    return raw.strip()


def _env_or_default(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw.strip() if raw and raw.strip() else default


def _parse_bool(value: str) -> bool:
    text = value.strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _require_bool(name: str) -> bool:
    return _parse_bool(_require_env(name))


def _require_int(name: str, *, min_value: int = 1, max_value: int = 365 * 24 * 3600) -> int:
    raw = _require_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _require_float(name: str, *, min_value: float = 0.1, max_value: float = 120.0) -> float:
    raw = _require_env(name)
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _env_int_or_default(
    name: str,
    default: int,
    *,
    min_value: int = 1,
    max_value: int = 365 * 24 * 3600,
) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _env_float_or_default(
    name: str,
    default: float,
    *,
    min_value: float = 0.1,
    max_value: float = 120.0,
) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _is_truthy(raw: Optional[str]) -> bool:
    value = (raw or "").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


def _is_test_mode() -> bool:
    if _is_truthy(os.environ.get("SYSTEM_HEALTH_RUN_IN_TEST")):
        return False
    if "PYTEST_CURRENT_TEST" in os.environ:
        return True
    return _is_truthy(os.environ.get("TEST_MODE"))


def _split_csv(raw: Optional[str]) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _env_has_value(name: str) -> bool:
    raw = os.environ.get(name)
    return bool(raw and raw.strip())


@dataclass(frozen=True)
class FreshnessPolicy:
    max_age_seconds: int
    source: str


@dataclass(frozen=True)
class MarkerProbeConfig:
    enabled: bool
    container: str
    prefix: str


@dataclass(frozen=True)
class JobScheduleMetadata:
    trigger_type: str
    cron_expression: str


@dataclass(frozen=True)
class BronzeSymbolJumpThreshold:
    warn_factor: float
    error_factor: float
    min_previous_symbols: int
    min_current_symbols: int
