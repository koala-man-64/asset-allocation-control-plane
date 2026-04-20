from __future__ import annotations

import hashlib
import logging
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from api.service.settings import QuiverSettings
from quiver_provider import QuiverClient, QuiverConfig, QuiverError, QuiverNotConfiguredError


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    base_url: str
    timeout_seconds: float
    rate_limit_per_min: int
    max_concurrency: int
    max_retries: int
    backoff_base_seconds: float


logger = logging.getLogger("asset-allocation.api.quiver")


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_caller_component(value: object, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    if len(text) > 128:
        return text[:128]
    return text


def _config_from_settings(settings: QuiverSettings) -> QuiverConfig:
    api_key = _strip_or_none(settings.api_key)
    if not api_key:
        raise QuiverNotConfiguredError("QUIVER_API_KEY is not configured for the API service.")

    return QuiverConfig(
        api_key=api_key,
        base_url=str(settings.base_url).rstrip("/"),
        timeout_seconds=float(settings.timeout_seconds),
        rate_limit_per_min=int(settings.rate_limit_per_min),
        max_concurrency=int(settings.max_concurrency),
        max_retries=int(settings.max_retries),
        backoff_base_seconds=float(settings.backoff_base_seconds),
    )


def _snapshot_from_config(config: QuiverConfig) -> _ClientSnapshot:
    return _ClientSnapshot(
        api_key_hash=_hash_secret(str(config.api_key)),
        base_url=str(config.base_url).rstrip("/"),
        timeout_seconds=float(config.timeout_seconds),
        rate_limit_per_min=int(config.rate_limit_per_min),
        max_concurrency=int(config.max_concurrency),
        max_retries=int(config.max_retries),
        backoff_base_seconds=float(config.backoff_base_seconds),
    )


def _summarize_exception(exc: Exception) -> str:
    detail = getattr(exc, "detail", None)
    payload = getattr(exc, "payload", None)
    message = _strip_or_none(detail) or _strip_or_none(exc) or type(exc).__name__
    if payload:
        message = f"{message} payload={payload}"
    if len(message) > 240:
        return f"{message[:237]}..."
    return message


def _status_for_exception(exc: Exception) -> str:
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        return str(status_code)
    if isinstance(exc, QuiverNotConfiguredError):
        return "503"
    return type(exc).__name__


_CALLER_JOB: ContextVar[str] = ContextVar("quiver_caller_job", default="api")
_CALLER_EXECUTION: ContextVar[str] = ContextVar("quiver_caller_execution", default="")


def get_current_caller_context() -> tuple[str, str]:
    return (
        _normalize_caller_component(_CALLER_JOB.get(), default="api"),
        _normalize_caller_component(_CALLER_EXECUTION.get(), default=""),
    )


@contextmanager
def quiver_caller_context(*, caller_job: Optional[str], caller_execution: Optional[str] = None) -> Iterator[None]:
    job_token = _CALLER_JOB.set(_normalize_caller_component(caller_job, default="api"))
    execution_token = _CALLER_EXECUTION.set(_normalize_caller_component(caller_execution, default=""))
    try:
        yield
    finally:
        _CALLER_JOB.reset(job_token)
        _CALLER_EXECUTION.reset(execution_token)


class QuiverGateway:
    def __init__(self, settings: QuiverSettings | None = None) -> None:
        self._lock = threading.RLock()
        self._client: QuiverClient | None = None
        self._snapshot: _ClientSnapshot | None = None
        self._settings = settings

    def _build_snapshot(self) -> tuple[_ClientSnapshot, QuiverConfig]:
        config = _config_from_settings(self._settings) if self._settings is not None else QuiverConfig.from_env(require_api_key=True)
        snapshot = _snapshot_from_config(config)
        return snapshot, config

    def get_client(self) -> QuiverClient:
        snapshot, config = self._build_snapshot()
        with self._lock:
            if self._client is None or self._snapshot != snapshot:
                old = self._client
                self._client = QuiverClient(config)
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

    def _get(self, path: str, *, params: Optional[dict[str, Any]] = None) -> Any:
        caller_job, caller_execution = get_current_caller_context()
        started_at = time.perf_counter()
        try:
            payload = self.get_client().get_json(path, params=params)
        except Exception as exc:
            logger.warning(
                "Quiver provider failure caller_job=%s caller_execution=%s path=%s status=%s latency_ms=%s error=%s",
                caller_job,
                caller_execution or "n/a",
                path,
                _status_for_exception(exc),
                int((time.perf_counter() - started_at) * 1000),
                _summarize_exception(exc),
                exc_info=not isinstance(exc, QuiverError),
            )
            raise

        logger.info(
            "Quiver provider success caller_job=%s caller_execution=%s path=%s status=success latency_ms=%s payload_type=%s",
            caller_job,
            caller_execution or "n/a",
            path,
            int((time.perf_counter() - started_at) * 1000),
            type(payload).__name__,
        )
        return payload

    def get_live_congress_trading(self, *, normalized: bool | None = None, representative: str | None = None) -> Any:
        return self._get(
            "/beta/live/congresstrading",
            params={"normalized": normalized, "representative": representative},
        )

    def get_historical_congress_trading(self, *, ticker: str, analyst: str | None = None) -> Any:
        return self._get(f"/beta/historical/congresstrading/{ticker}", params={"analyst": analyst})

    def get_live_senate_trading(self, *, name: str | None = None, options: bool | None = None) -> Any:
        return self._get("/beta/live/senatetrading", params={"name": name, "options": options})

    def get_historical_senate_trading(self, *, ticker: str) -> Any:
        return self._get(f"/beta/historical/senatetrading/{ticker}")

    def get_live_house_trading(self, *, name: str | None = None, options: bool | None = None) -> Any:
        return self._get("/beta/live/housetrading", params={"name": name, "options": options})

    def get_historical_house_trading(self, *, ticker: str) -> Any:
        return self._get(f"/beta/historical/housetrading/{ticker}")

    def get_live_gov_contracts(self) -> Any:
        return self._get("/beta/live/govcontracts")

    def get_historical_gov_contracts(self, *, ticker: str) -> Any:
        return self._get(f"/beta/historical/govcontracts/{ticker}")

    def get_live_gov_contracts_all(
        self,
        *,
        date: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._get("/beta/live/govcontractsall", params={"date": date, "page": page, "page_size": page_size})

    def get_historical_gov_contracts_all(self, *, ticker: str) -> Any:
        return self._get(f"/beta/historical/govcontractsall/{ticker}")

    def get_live_insiders(
        self,
        *,
        ticker: str | None = None,
        date: str | None = None,
        uploaded: str | None = None,
        limit_codes: bool | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._get(
            "/beta/live/insiders",
            params={
                "ticker": ticker,
                "date": date,
                "uploaded": uploaded,
                "limit_codes": limit_codes,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_live_sec13f(
        self,
        *,
        ticker: str | None = None,
        owner: str | None = None,
        date: str | None = None,
        period: str | None = None,
        today: bool | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._get(
            "/beta/live/sec13f",
            params={
                "ticker": ticker,
                "owner": owner,
                "date": date,
                "period": period,
                "today": today,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_live_sec13f_changes(
        self,
        *,
        ticker: str | None = None,
        owner: str | None = None,
        date: str | None = None,
        period: str | None = None,
        today: bool | None = None,
        most_recent: bool | None = None,
        show_new_funds: bool | None = None,
        mobile: bool | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._get(
            "/beta/live/sec13fchanges",
            params={
                "ticker": ticker,
                "owner": owner,
                "date": date,
                "period": period,
                "today": today,
                "most_recent": most_recent,
                "show_new_funds": show_new_funds,
                "mobile": mobile,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_live_lobbying(
        self,
        *,
        all_records: bool | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._get(
            "/beta/live/lobbying",
            params={
                "all": all_records,
                "date_from": date_from,
                "date_to": date_to,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_historical_lobbying(
        self,
        *,
        ticker: str,
        page: int | None = None,
        page_size: int | None = None,
        query: str | None = None,
        query_ticker: str | None = None,
    ) -> Any:
        return self._get(
            f"/beta/historical/lobbying/{ticker}",
            params={"page": page, "page_size": page_size, "query": query, "queryTicker": query_ticker},
        )

    def get_live_etf_holdings(self, *, etf: str | None = None, ticker: str | None = None) -> Any:
        return self._get("/beta/live/etfholdings", params={"etf": etf, "ticker": ticker})

    def get_live_congress_holdings(self) -> Any:
        return self._get("/beta/live/congressholdings")
