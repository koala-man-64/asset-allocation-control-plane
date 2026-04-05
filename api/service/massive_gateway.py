from __future__ import annotations

import csv
import collections
import hashlib
import io
import logging
import os
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterator, Literal, Optional

from core.market_history_contract import (
    MARKET_HISTORY_START_DATE,
    MARKET_HISTORY_STATUS_NO_HISTORY,
    MARKET_HISTORY_STATUS_OK,
)
from core.massive_provider import get_complete_ticker_list as get_complete_reference_ticker_list
from massive_provider import MassiveClient, MassiveConfig
from massive_provider.errors import (
    MassiveAuthError,
    MassiveError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
)
from massive_provider.utils import ms_to_iso_date

logger = logging.getLogger("asset-allocation.api.massive")
_FULL_HISTORY_START_DATE = "1970-01-01"
_CANONICAL_TO_PROVIDER_SYMBOL = {
    "^VIX": "I:VIX",
    "^VIX3M": "I:VIX3M",
}
_PROVIDER_TO_CANONICAL_SYMBOL = {
    provider: canonical for canonical, provider in _CANONICAL_TO_PROVIDER_SYMBOL.items()
}

FinanceReport = Literal["balance_sheet", "cash_flow", "income_statement", "valuation"]
_FINANCE_TRACE_ENABLED = (os.environ.get("MASSIVE_FINANCE_TRACE_ENABLED") or "").strip().lower() in {
    "1",
    "true",
    "t",
    "yes",
    "y",
    "on",
}
try:
    _FINANCE_TRACE_SUCCESS_LIMIT = max(0, int(str(os.environ.get("MASSIVE_FINANCE_TRACE_SUCCESS_LIMIT") or "40").strip()))
except Exception:
    _FINANCE_TRACE_SUCCESS_LIMIT = 40
try:
    _FINANCE_TRACE_ANOMALY_LIMIT = max(1, int(str(os.environ.get("MASSIVE_FINANCE_TRACE_ANOMALY_LIMIT") or "200").strip()))
except Exception:
    _FINANCE_TRACE_ANOMALY_LIMIT = 200
_FINANCE_TRACE_COUNTERS: collections.Counter[str] = collections.Counter()
_FINANCE_TRACE_LOCK = threading.Lock()


class MassiveNotConfiguredError(RuntimeError):
    pass


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    base_url: str
    timeout_seconds: float


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _normalize_caller_component(value: object, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    if len(text) > 128:
        return text[:128]
    return text


def _to_provider_symbol(symbol: object) -> str:
    canonical = str(symbol or "").strip().upper()
    if not canonical:
        return ""
    return _CANONICAL_TO_PROVIDER_SYMBOL.get(canonical, canonical)


def _to_canonical_symbol(symbol: object) -> str:
    provider = str(symbol or "").strip().upper()
    if not provider:
        return ""
    return _PROVIDER_TO_CANONICAL_SYMBOL.get(provider, provider)


_CALLER_JOB: ContextVar[str] = ContextVar("massive_caller_job", default="api")
_CALLER_EXECUTION: ContextVar[str] = ContextVar("massive_caller_execution", default="")


def get_current_caller_context() -> tuple[str, str]:
    return (
        _normalize_caller_component(_CALLER_JOB.get(), default="api"),
        _normalize_caller_component(_CALLER_EXECUTION.get(), default=""),
    )


@contextmanager
def massive_caller_context(
    *, caller_job: Optional[str], caller_execution: Optional[str] = None
) -> Iterator[None]:
    job_token = _CALLER_JOB.set(_normalize_caller_component(caller_job, default="api"))
    execution_token = _CALLER_EXECUTION.set(_normalize_caller_component(caller_execution, default=""))
    try:
        yield
    finally:
        _CALLER_JOB.reset(job_token)
        _CALLER_EXECUTION.reset(execution_token)


def _truncate_trace_text(value: object, *, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _emit_bounded_finance_log(category: str, message: str, *, warning: bool, limit: Optional[int] = None) -> None:
    max_logs = _FINANCE_TRACE_ANOMALY_LIMIT if limit is None else max(0, int(limit))
    with _FINANCE_TRACE_LOCK:
        seen = int(_FINANCE_TRACE_COUNTERS.get(category, 0))
        if seen >= max_logs:
            return
        current = seen + 1
        _FINANCE_TRACE_COUNTERS[category] = current
    prefix = f"[finance-api:{category}#{current}] "
    if warning:
        logger.warning("%s%s", prefix, message)
    else:
        logger.info("%s%s", prefix, message)
    if current == max_logs:
        logger.info("[finance-api:%s] further logs suppressed after %s entries.", category, max_logs)


def _summarize_finance_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return f"payload_type={type(payload).__name__}"
    parts: list[str] = []
    status = payload.get("status")
    if status is not None:
        parts.append(f"provider_status={_truncate_trace_text(status, limit=32)}")
    request_id = payload.get("request_id")
    if request_id:
        parts.append(f"request_id={_truncate_trace_text(request_id, limit=48)}")
    results = payload.get("results")
    if isinstance(results, list):
        parts.append(f"results_len={len(results)}")
        if results and isinstance(results[0], dict):
            first = results[0]
            first_date = first.get("period_end") or first.get("date") or first.get("as_of")
            if first_date:
                parts.append(f"first_date={_truncate_trace_text(first_date, limit=32)}")
    else:
        parts.append(f"results_type={type(results).__name__ if results is not None else 'None'}")
    parts.append(f"next_url={'present' if payload.get('next_url') else 'none'}")
    if payload.get("error"):
        parts.append(f"error={_truncate_trace_text(payload.get('error'))}")
    return " ".join(parts) or "payload_summary=empty"


def _summarize_massive_exception(exc: BaseException) -> str:
    parts = [f"type={type(exc).__name__}"]
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        parts.append(f"status={status_code}")
    detail = getattr(exc, "detail", None)
    if detail:
        parts.append(f"detail={_truncate_trace_text(detail)}")
    elif str(exc).strip():
        parts.append(f"message={_truncate_trace_text(str(exc))}")
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = payload.get("path")
        if path:
            parts.append(f"path={_truncate_trace_text(path, limit=96)}")
        payload_detail = payload.get("detail")
        if payload_detail and payload_detail != detail:
            parts.append(f"payload_detail={_truncate_trace_text(payload_detail)}")
    return " ".join(parts)


def _coerce_number(payload: dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _extract_iso_date(payload: dict[str, Any]) -> Optional[str]:
    for key in (
        "date",
        "Date",
        "session",
        "day",
        "start",
        "start_date",
        "timestamp",
        "t",
        "time",
        "window_start",
    ):
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue

        if isinstance(value, (int, float)):
            try:
                iv = int(value)
                if abs(iv) > 10_000_000_000:
                    return ms_to_iso_date(iv)
                dt = datetime.fromtimestamp(iv, tz=timezone.utc)
                return dt.date().isoformat()
            except Exception:
                continue

        raw = str(value).strip()
        if not raw:
            continue
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            pass
        try:
            return date.fromisoformat(raw[:10]).isoformat()
        except Exception:
            continue
    return None


def _normalize_key(value: object) -> str:
    return "".join(ch for ch in str(value or "").strip().lower() if ch.isalnum())


def _extract_payload_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return [row for row in results if isinstance(row, dict)]
        if isinstance(results, dict):
            return [results]
        return [payload]
    return []


def _extract_row_date(payload: dict[str, Any]) -> Optional[str]:
    normalized = {_normalize_key(key): value for key, value in payload.items()}
    for key in (
        "date",
        "settlement_date",
        "settlementdate",
        "effective_date",
        "effectivedate",
        "as_of",
        "asof",
        "session",
        "day",
        "start",
        "start_date",
        "startdate",
        "timestamp",
        "t",
        "time",
        "window_start",
        "windowstart",
    ):
        raw = normalized.get(_normalize_key(key))
        if raw is None:
            continue
        if isinstance(raw, (int, float)):
            try:
                ivalue = int(raw)
                if abs(ivalue) > 10_000_000_000:
                    return ms_to_iso_date(ivalue)
                return datetime.fromtimestamp(ivalue, tz=timezone.utc).date().isoformat()
            except Exception:
                continue
        text = str(raw).strip()
        if not text:
            continue
        try:
            out = datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            try:
                out = date.fromisoformat(text[:10]).isoformat()
            except Exception:
                continue
        if out:
            return out
    return None


def _extract_first_numeric(payload: dict[str, Any], keys: tuple[str, ...]) -> Optional[float]:
    normalized = {_normalize_key(key): value for key, value in payload.items()}
    for key in keys:
        raw = normalized.get(_normalize_key(key))
        if raw is None:
            continue
        try:
            return float(raw)
        except Exception:
            continue
    return None


def _clamp_market_history_start(from_date: Optional[str], *, to_date: Optional[str]) -> tuple[str, str]:
    start = from_date or MARKET_HISTORY_START_DATE
    start = max(str(start), MARKET_HISTORY_START_DATE)
    end = to_date or _utc_today_iso()
    return start, str(end)


class MassiveGateway:
    """
    Process-local gateway for Massive provider calls.

    Responsibilities:
      - Construct and hold a shared MassiveClient.
      - Recreate the client if Massive env tuning changes.
      - Provide a constrained surface area used by API routes.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._client: MassiveClient | None = None
        self._snapshot: _ClientSnapshot | None = None

    def _build_snapshot(self) -> tuple[_ClientSnapshot, MassiveConfig]:
        try:
            cfg = MassiveConfig.from_env(require_api_key=True)
        except Exception as exc:
            raise MassiveNotConfiguredError("MASSIVE_API_KEY is not configured for the API service.") from exc

        snapshot = _ClientSnapshot(
            api_key_hash=_hash_secret(str(cfg.api_key)),
            base_url=str(cfg.base_url).rstrip("/"),
            timeout_seconds=float(cfg.timeout_seconds),
        )
        return snapshot, cfg

    def get_client(self) -> MassiveClient:
        snapshot, cfg = self._build_snapshot()
        with self._lock:
            if self._client is None or self._snapshot != snapshot:
                old = self._client
                self._client = MassiveClient(cfg)
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

    def _normalize_ohlcv_rows(self, bars: list[dict[str, Any]]) -> list[dict[str, float | str]]:
        rows: list[dict[str, float | str]] = []
        for bar in bars:
            if not isinstance(bar, dict):
                continue

            as_of = _extract_iso_date(bar)
            open_ = _coerce_number(bar, "o", "open", "Open")
            high = _coerce_number(bar, "h", "high", "High")
            low = _coerce_number(bar, "l", "low", "Low")
            close = _coerce_number(bar, "c", "close", "Close")
            volume = _coerce_number(bar, "v", "volume", "Volume")

            if not as_of:
                continue
            if open_ is None or high is None or low is None or close is None:
                continue

            rows.append(
                {
                    "Date": as_of,
                    "Open": float(open_),
                    "High": float(high),
                    "Low": float(low),
                    "Close": float(close),
                    "Volume": float(volume or 0.0),
                }
            )

        rows.sort(key=lambda row: str(row["Date"]))
        return rows

    def _to_csv(self, rows: list[dict[str, float | str]]) -> str:
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])
        for row in rows:
            writer.writerow(
                [
                    row["Date"],
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"],
                ]
            )
        return out.getvalue()

    def _normalize_daily_summary_row(self, payload: Any, *, fallback_date: str) -> Optional[dict[str, float | str]]:
        if not isinstance(payload, dict):
            return None

        as_of = _extract_iso_date(payload) or str(fallback_date)
        open_ = _coerce_number(payload, "open", "o", "Open")
        high = _coerce_number(payload, "high", "h", "High")
        low = _coerce_number(payload, "low", "l", "Low")
        close = _coerce_number(payload, "close", "c", "Close")
        volume = _coerce_number(payload, "volume", "v", "Volume")

        if not as_of:
            return None
        if open_ is None or high is None or low is None or close is None:
            return None

        return {
            "Date": str(as_of),
            "Open": float(open_),
            "High": float(high),
            "Low": float(low),
            "Close": float(close),
            "Volume": float(volume or 0.0),
        }

    def _load_daily_time_series_rows(
        self,
        *,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        adjusted: bool = True,
    ) -> list[dict[str, float | str]]:
        sym = str(symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol is required.")
        provider_symbol = _to_provider_symbol(sym)

        end_date = to_date or _utc_today_iso()
        start_date = from_date or _FULL_HISTORY_START_DATE

        if str(start_date) == str(end_date):
            try:
                summary_payload = self.get_client().get_daily_ticker_summary(
                    ticker=provider_symbol,
                    date=str(start_date),
                    adjusted=bool(adjusted),
                )
                row = self._normalize_daily_summary_row(summary_payload, fallback_date=str(start_date))
                if row is not None:
                    return [row]
            except MassiveNotFoundError:
                # Fallback to aggs endpoint below so callers still get a CSV response.
                pass

        bars = self.get_client().list_ohlcv(
            ticker=provider_symbol,
            multiplier=1,
            timespan="day",
            from_=str(start_date),
            to=str(end_date),
            adjusted=bool(adjusted),
            sort="asc",
            limit=50000,
            pagination=True,
        )
        return self._normalize_ohlcv_rows([b for b in bars if isinstance(b, dict)])

    def _build_metric_values_by_date(
        self,
        payload: Any,
        *,
        value_keys: tuple[str, ...],
        fallback_date: str,
        min_date: str,
        max_date: str,
    ) -> dict[str, float]:
        values_by_date: dict[str, float] = {}
        for row in _extract_payload_rows(payload):
            value = _extract_first_numeric(row, value_keys)
            as_of = _extract_row_date(row)
            if value is None or as_of is None:
                continue
            if as_of < min_date or as_of > max_date:
                continue
            values_by_date[as_of] = float(value)

        if not values_by_date and isinstance(payload, dict):
            top_level_value = _extract_first_numeric(payload, value_keys)
            if top_level_value is not None:
                values_by_date[fallback_date] = float(top_level_value)
        return values_by_date

    def _merge_market_metrics(
        self,
        daily_rows: list[dict[str, float | str]],
        *,
        short_interest_payload: Any,
        short_volume_payload: Any,
    ) -> list[dict[str, float | str | None]]:
        if not daily_rows:
            return []

        out_rows = [dict(row) for row in daily_rows]
        min_date = str(out_rows[0]["Date"])
        max_date = str(out_rows[-1]["Date"])
        metric_specs = (
            (
                "ShortInterest",
                short_interest_payload,
                (
                    "short_interest",
                    "shortinterest",
                    "shortinterestshares",
                    "short_interest_shares",
                    "sharesshort",
                    "value",
                ),
            ),
            (
                "ShortVolume",
                short_volume_payload,
                (
                    "short_volume",
                    "shortvolume",
                    "shortvolumeshares",
                    "short_volume_shares",
                    "volumeshort",
                    "value",
                ),
            ),
        )

        for column_name, payload, value_keys in metric_specs:
            values_by_date = self._build_metric_values_by_date(
                payload,
                value_keys=value_keys,
                fallback_date=max_date,
                min_date=min_date,
                max_date=max_date,
            )
            last_value: float | None = None
            for row in out_rows:
                row_date = str(row["Date"])
                if row_date in values_by_date:
                    last_value = values_by_date[row_date]
                row[column_name] = last_value

        return out_rows

    def _market_history_payload(
        self,
        *,
        symbol: str,
        rows: list[dict[str, float | str | None]],
        status: str,
    ) -> dict[str, Any]:
        payload_rows: list[dict[str, Any]] = []
        for row in rows:
            payload_rows.append(
                {
                    "date": row.get("Date"),
                    "open": row.get("Open"),
                    "high": row.get("High"),
                    "low": row.get("Low"),
                    "close": row.get("Close"),
                    "volume": row.get("Volume"),
                    "short_interest": row.get("ShortInterest"),
                    "short_volume": row.get("ShortVolume"),
                }
            )
        return {
            "symbol": str(symbol or "").strip().upper(),
            "status": status,
            "rows": payload_rows,
        }

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        adjusted: bool = True,
    ) -> str:
        rows = self._load_daily_time_series_rows(
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
            adjusted=adjusted,
        )
        return self._to_csv(rows)

    def get_market_history(
        self,
        *,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> dict[str, Any]:
        sym = str(symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol is required.")

        start_date, end_date = _clamp_market_history_start(from_date, to_date=to_date)
        if end_date < MARKET_HISTORY_START_DATE or start_date > end_date:
            return self._market_history_payload(
                symbol=sym,
                rows=[],
                status=MARKET_HISTORY_STATUS_NO_HISTORY,
            )

        daily_rows = self._load_daily_time_series_rows(
            symbol=sym,
            from_date=start_date,
            to_date=end_date,
            adjusted=True,
        )
        if not daily_rows:
            return self._market_history_payload(
                symbol=sym,
                rows=[],
                status=MARKET_HISTORY_STATUS_NO_HISTORY,
            )

        try:
            short_interest_payload = self.get_short_interest(
                symbol=sym,
                settlement_date_gte=start_date,
                settlement_date_lte=end_date,
            )
        except MassiveNotFoundError:
            short_interest_payload = {}

        try:
            short_volume_payload = self.get_short_volume(
                symbol=sym,
                date_gte=start_date,
                date_lte=end_date,
            )
        except MassiveNotFoundError:
            short_volume_payload = {}

        merged_rows = self._merge_market_metrics(
            daily_rows,
            short_interest_payload=short_interest_payload,
            short_volume_payload=short_volume_payload,
        )
        return self._market_history_payload(
            symbol=sym,
            rows=merged_rows,
            status=MARKET_HISTORY_STATUS_OK,
        )

    def get_short_interest(
        self,
        *,
        symbol: str,
        settlement_date_gte: Optional[str] = None,
        settlement_date_lte: Optional[str] = None,
    ) -> Any:
        params = {"sort": "settlement_date.asc", "limit": 50000}
        if settlement_date_gte:
            params["settlement_date.gte"] = settlement_date_gte
        if settlement_date_lte:
            params["settlement_date.lte"] = settlement_date_lte
        return self.get_client().get_short_interest(
            ticker=_to_provider_symbol(symbol),
            params=params,
            pagination=True,
        )

    def get_short_volume(
        self,
        *,
        symbol: str,
        date_gte: Optional[str] = None,
        date_lte: Optional[str] = None,
    ) -> Any:
        params = {"sort": "date.asc", "limit": 50000}
        if date_gte:
            params["date.gte"] = date_gte
        if date_lte:
            params["date.lte"] = date_lte
        return self.get_client().get_short_volume(
            ticker=_to_provider_symbol(symbol),
            params=params,
            pagination=True,
        )

    def get_float(self, *, symbol: str, as_of: Optional[str] = None) -> Any:
        # Massive's current float endpoint does not document as-of/date filters.
        # Keep the parameter for compatibility but do not forward query filters.
        return self.get_client().get_float(
            ticker=_to_provider_symbol(symbol),
            params={"sort": "effective_date.asc", "limit": 5000},
            pagination=True,
        )

    def get_finance_report(
        self,
        *,
        symbol: str,
        report: FinanceReport,
        timeframe: Optional[str] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        pagination: bool = True,
    ) -> Any:
        caller_job, caller_execution = get_current_caller_context()
        by_report = {
            "balance_sheet": self.get_client().get_balance_sheet,
            "cash_flow": self.get_client().get_cash_flow_statement,
            "income_statement": self.get_client().get_income_statement,
            "valuation": self.get_client().get_ratios,
        }
        handler = by_report.get(str(report))
        if handler is None:
            raise ValueError(f"Unknown finance report={report!r}")
        params: dict[str, Any] = {}
        if timeframe:
            params["timeframe"] = str(timeframe).strip().lower()
        if sort:
            params["sort"] = str(sort).strip()
        if limit is not None:
            params["limit"] = int(limit)
        try:
            payload = handler(
                ticker=_to_provider_symbol(symbol),
                params=params or None,
                pagination=bool(pagination),
            )
        except BaseException as exc:
            _emit_bounded_finance_log(
                "provider_error",
                f"Massive finance provider error caller_job={caller_job} caller_execution={caller_execution or 'n/a'} "
                f"symbol={symbol} report={report} timeframe={timeframe or 'n/a'} sort={sort or 'n/a'} "
                f"limit={limit if limit is not None else 'n/a'} pagination={bool(pagination)} "
                f"{_summarize_massive_exception(exc)}",
                warning=True,
            )
            raise

        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list) and results:
                if _FINANCE_TRACE_ENABLED:
                    _emit_bounded_finance_log(
                        "provider_success",
                        f"Massive finance provider success caller_job={caller_job} "
                        f"caller_execution={caller_execution or 'n/a'} symbol={symbol} report={report} "
                        f"timeframe={timeframe or 'n/a'} sort={sort or 'n/a'} "
                        f"limit={limit if limit is not None else 'n/a'} pagination={bool(pagination)} "
                        f"{_summarize_finance_payload(payload)}",
                        warning=False,
                        limit=_FINANCE_TRACE_SUCCESS_LIMIT,
                    )
            else:
                _emit_bounded_finance_log(
                    "provider_anomaly",
                    f"Massive finance provider anomaly caller_job={caller_job} "
                    f"caller_execution={caller_execution or 'n/a'} symbol={symbol} report={report} "
                    f"timeframe={timeframe or 'n/a'} sort={sort or 'n/a'} "
                    f"limit={limit if limit is not None else 'n/a'} pagination={bool(pagination)} "
                    f"{_summarize_finance_payload(payload)}",
                    warning=True,
                )
        else:
            _emit_bounded_finance_log(
                "provider_anomaly",
                f"Massive finance provider anomaly caller_job={caller_job} "
                f"caller_execution={caller_execution or 'n/a'} symbol={symbol} report={report} "
                f"timeframe={timeframe or 'n/a'} sort={sort or 'n/a'} "
                f"limit={limit if limit is not None else 'n/a'} pagination={bool(pagination)} "
                f"payload_type={type(payload).__name__}",
                warning=True,
            )
        return payload

    def get_tickers(
        self,
        *,
        market: str = "stocks",
        locale: Optional[str] = "us",
        active: bool = True,
    ) -> list[dict[str, Any]]:
        _, cfg = self._build_snapshot()
        df = get_complete_reference_ticker_list(
            api_key=str(cfg.api_key),
            base_url=str(cfg.base_url),
            timeout_seconds=float(cfg.timeout_seconds),
            market=str(market or "stocks").strip() or "stocks",
            locale=_strip_or_none(locale),
            active=bool(active),
        )
        if df is None or df.empty:
            return []
        records = df.to_dict(orient="records")
        out: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            item = dict(record)
            ticker = _to_canonical_symbol(item.get("Symbol") or item.get("symbol") or item.get("ticker"))
            if ticker:
                item["Symbol"] = ticker
            active_value = item.get("Active")
            if active_value is not None:
                item["Active"] = bool(active_value)
            out.append(item)
        return out

    def get_unified_snapshot(
        self,
        *,
        symbols: list[str],
        asset_type: str = "stocks",
    ) -> Any:
        normalized = [str(symbol or "").strip().upper() for symbol in symbols]
        normalized = [symbol for symbol in normalized if symbol]
        if not normalized:
            raise ValueError("symbols is required.")
        provider_symbols = [_to_provider_symbol(symbol) for symbol in normalized]
        payload = self.get_client().get_unified_snapshot(
            tickers=provider_symbols,
            asset_type=asset_type,
            limit=250,
        )
        if not isinstance(payload, dict):
            return payload
        results = payload.get("results")
        if not isinstance(results, list):
            return payload

        translated_results: list[Any] = []
        for row in results:
            if not isinstance(row, dict):
                translated_results.append(row)
                continue
            translated = dict(row)
            ticker = _to_canonical_symbol(translated.get("ticker"))
            if ticker:
                translated["ticker"] = ticker
            symbol_value = _to_canonical_symbol(translated.get("symbol"))
            if symbol_value:
                translated["symbol"] = symbol_value
            translated_results.append(translated)
        translated_payload = dict(payload)
        translated_payload["results"] = translated_results
        return translated_payload
 

__all__ = [
    "FinanceReport",
    "MassiveGateway",
    "MassiveNotConfiguredError",
    "MassiveError",
    "MassiveAuthError",
    "MassiveNotFoundError",
    "MassiveRateLimitError",
    "MassiveServerError",
    "massive_caller_context",
    "get_current_caller_context",
]
