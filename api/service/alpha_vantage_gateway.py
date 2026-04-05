from __future__ import annotations

import hashlib
import logging
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Literal, Optional

from alpha_vantage import (
    AlphaVantageClient,
    AlphaVantageConfig,
    AlphaVantageError,
)

logger = logging.getLogger("asset-allocation.api.alpha_vantage")

EarningsCalendarHorizon = Literal["3month", "6month", "12month"]


class AlphaVantageNotConfiguredError(RuntimeError):
    pass


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    base_url: str
    rate_limit_per_min: int
    timeout_seconds: float
    max_workers: int
    max_retries: int
    backoff_base_seconds: float
    rate_wait_timeout_seconds: float | None
    throttle_cooldown_seconds: float


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_int(name: str, default: int) -> int:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception:
        logger.warning("Invalid int for %s=%r; using default=%s", name, raw, default)
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except Exception:
        logger.warning("Invalid float for %s=%r; using default=%s", name, raw, default)
        return float(default)


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


_CALLER_JOB: ContextVar[str] = ContextVar("alpha_vantage_caller_job", default="api")
_CALLER_EXECUTION: ContextVar[str] = ContextVar("alpha_vantage_caller_execution", default="")


def _normalize_caller_component(value: object, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    if len(text) > 128:
        return text[:128]
    return text


def normalize_earnings_calendar_horizon(value: object, *, default: EarningsCalendarHorizon = "12month") -> EarningsCalendarHorizon:
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"3month", "6month", "12month"}:
        return text  # type: ignore[return-value]
    raise ValueError(f"Invalid earnings calendar horizon={value!r}; expected 3month, 6month, or 12month.")


def _get_current_caller_job() -> str:
    return _normalize_caller_component(_CALLER_JOB.get(), default="api")


def get_current_caller_context() -> tuple[str, str]:
    return (
        _normalize_caller_component(_CALLER_JOB.get(), default="api"),
        _normalize_caller_component(_CALLER_EXECUTION.get(), default=""),
    )


@contextmanager
def alpha_vantage_caller_context(
    *, caller_job: Optional[str], caller_execution: Optional[str] = None
) -> Iterator[None]:
    job_token = _CALLER_JOB.set(_normalize_caller_component(caller_job, default="api"))
    execution_token = _CALLER_EXECUTION.set(_normalize_caller_component(caller_execution, default=""))
    try:
        yield
    finally:
        _CALLER_JOB.reset(job_token)
        _CALLER_EXECUTION.reset(execution_token)


class AlphaVantageGateway:
    """
    Process-local gateway for Alpha Vantage calls.

    Responsibilities:
      - Construct and hold a shared AlphaVantageClient (rate limiting is process-local).
      - Recreate the client if allowlisted env tuning changes (rate limit/timeout/etc).
      - Provide a constrained surface area used by API routes.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._client: AlphaVantageClient | None = None
        self._snapshot: _ClientSnapshot | None = None

    def _build_snapshot(self) -> tuple[_ClientSnapshot, AlphaVantageConfig]:
        api_key = _strip_or_none(os.environ.get("ALPHA_VANTAGE_API_KEY"))
        if not api_key:
            raise AlphaVantageNotConfiguredError("ALPHA_VANTAGE_API_KEY is not configured for the API service.")

        base_url = _strip_or_none(os.environ.get("ALPHA_VANTAGE_BASE_URL")) or "https://www.alphavantage.co"
        rate_wait_timeout_seconds = _env_float("ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS", 600.0)
        if rate_wait_timeout_seconds <= 0:
            rate_wait_timeout_seconds = None
        throttle_cooldown_seconds = _env_float("ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS", 60.0)
        if throttle_cooldown_seconds < 60.0:
            logger.warning(
                "ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS=%s is too low; enforcing 60 seconds.",
                throttle_cooldown_seconds,
            )
            throttle_cooldown_seconds = 60.0

        cfg = AlphaVantageConfig(
            api_key=api_key,
            base_url=base_url,
            rate_limit_per_min=_env_int("ALPHA_VANTAGE_RATE_LIMIT_PER_MIN", 300),
            timeout=_env_float("ALPHA_VANTAGE_TIMEOUT_SECONDS", 15.0),
            max_workers=_env_int("ALPHA_VANTAGE_MAX_WORKERS", 32),
            max_retries=_env_int("ALPHA_VANTAGE_MAX_RETRIES", 5),
            backoff_base_seconds=_env_float("ALPHA_VANTAGE_BACKOFF_BASE_SECONDS", 0.5),
            rate_wait_timeout_seconds=rate_wait_timeout_seconds,
            throttle_cooldown_seconds=throttle_cooldown_seconds,
        )

        snapshot = _ClientSnapshot(
            api_key_hash=_hash_secret(api_key),
            base_url=str(cfg.base_url),
            rate_limit_per_min=int(cfg.rate_limit_per_min),
            timeout_seconds=float(cfg.timeout),
            max_workers=int(cfg.max_workers),
            max_retries=int(cfg.max_retries),
            backoff_base_seconds=float(cfg.backoff_base_seconds),
            rate_wait_timeout_seconds=(
                float(cfg.rate_wait_timeout_seconds) if cfg.rate_wait_timeout_seconds is not None else None
            ),
            throttle_cooldown_seconds=float(cfg.throttle_cooldown_seconds),
        )
        return snapshot, cfg

    def get_client(self) -> AlphaVantageClient:
        snapshot, cfg = self._build_snapshot()
        with self._lock:
            if self._client is None or self._snapshot != snapshot:
                old = self._client
                self._client = AlphaVantageClient(cfg, caller_provider=_get_current_caller_job)
                self._snapshot = snapshot
                if old is not None:
                    try:
                        old.close()
                    except Exception:
                        pass
            return self._client

    def close(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
            self._snapshot = None
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def get_listing_status_csv(self, *, state: Optional[str] = "active", date: Optional[str] = None) -> str:
        return str(self.get_client().get_listing_status(state=state, date=date))

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
    ) -> str:
        return str(
            self.get_client().get_daily_time_series(symbol, outputsize=outputsize, adjusted=adjusted, datatype="csv")
        )

    def get_earnings(self, *, symbol: str) -> dict[str, Any]:
        payload = self.get_client().fetch("EARNINGS", symbol)
        if not isinstance(payload, dict):
            raise AlphaVantageError("Unexpected Alpha Vantage earnings response type.", code="invalid_payload")
        return payload

    def get_earnings_calendar_csv(
        self,
        *,
        symbol: Optional[str] = None,
        horizon: EarningsCalendarHorizon = "12month",
    ) -> str:
        normalized_horizon = normalize_earnings_calendar_horizon(horizon)
        return str(self.get_client().get_earnings_calendar(symbol=symbol, horizon=normalized_horizon))
