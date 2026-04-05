from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from massive_provider.config import MassiveConfig
from massive_provider.errors import (
    MassiveAuthError,
    MassiveError,
    MassiveNotConfiguredError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
)
from massive_provider.utils import filter_none, to_jsonable

logger = logging.getLogger(__name__)

_ENDPOINT_OPEN_CLOSE_TEMPLATE = "/v1/open-close/{ticker}/{date}"
_ENDPOINT_AGGS_RANGE_TEMPLATE = "/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_}/{to}"
_ENDPOINT_SHORT_INTEREST = "/stocks/v1/short-interest"
_ENDPOINT_SHORT_VOLUME = "/stocks/v1/short-volume"
_ENDPOINT_FLOAT_DEFAULT = "/stocks/vX/float"
_ENDPOINT_FINANCIALS_INCOME = "/stocks/financials/v1/income-statements"
_ENDPOINT_FINANCIALS_CASH_FLOW = "/stocks/financials/v1/cash-flow-statements"
_ENDPOINT_FINANCIALS_BALANCE_SHEET = "/stocks/financials/v1/balance-sheets"
_ENDPOINT_FINANCIALS_RATIOS = "/stocks/financials/v1/ratios"
_ENDPOINT_UNIFIED_SNAPSHOT = "/v3/snapshot"
_MAX_SNAPSHOT_TICKERS_PER_REQUEST = 250
_DEFAULT_SNAPSHOT_LIMIT = 250


@dataclass(frozen=True)
class MassiveHTTPResponse:
    """Normalized Massive REST response payload.

    Massive endpoints are not fully uniform across products and versions.
    This container provides a consistent shape for downstream code.
    """

    status_code: int
    url: str
    payload: Any


class MassiveClient:
    """Project-specific façade over Massive REST + optional official SDK.

    This client supports:
      * OHLCV bars (via ``/v2/aggs`` or ``RESTClient.list_aggs``)
      * Fundamentals: short interest, short volume, float
      * Financial statements: income statement, cash flow, balance sheet
      * Ratios

    The official Massive SDK (``pip install massive``) is *optional* here.
    If installed and enabled, it is used for high-volume OHLCV pagination.
    """

    def __init__(
        self,
        config: MassiveConfig,
        *,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if not config.api_key:
            raise MassiveNotConfiguredError("MASSIVE_API_KEY is not configured.")

        self.config = config
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(
            timeout=httpx.Timeout(config.timeout_seconds),
            base_url=str(config.base_url).rstrip("/"),
            headers={"Authorization": f"Bearer {config.api_key}"},
            trust_env=False,
        )

    def close(self) -> None:
        if self._owns_http:
            try:
                self._http.close()
            except Exception:
                pass

    def __enter__(self) -> "MassiveClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------
    # Low-level HTTP
    # ------------------------------

    def _extract_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            text = (response.text or "").strip()
            return text or response.reason_phrase
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()
            # Some Massive endpoints return {"error": "..."}
            err = payload.get("error")
            if isinstance(err, str) and err.strip():
                return err.strip()
            return json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return response.reason_phrase

    def _request_json(self, path_or_url: str, *, params: Optional[dict[str, Any]] = None) -> MassiveHTTPResponse:
        """GET JSON from Massive.

        ``path_or_url`` can be a relative API path ("/v2/...") or an absolute
        next_url returned by Massive.
        """

        url = str(path_or_url)
        try:
            request_kwargs: dict[str, Any] = {}
            if params is not None:
                # On absolute next_url pagination calls, passing params={} can
                # strip existing query params under httpx 0.28+.
                request_kwargs["params"] = params
            resp = self._http.get(url, **request_kwargs)
        except httpx.TimeoutException as exc:
            raise MassiveError(f"Massive timeout calling {path_or_url}", payload={"path": path_or_url}) from exc
        except Exception as exc:
            raise MassiveError(
                f"Massive call failed: {type(exc).__name__}: {exc}",
                payload={"path": path_or_url},
            ) from exc

        if resp.status_code < 400:
            try:
                payload = resp.json()
            except Exception:
                payload = resp.text
            return MassiveHTTPResponse(status_code=int(resp.status_code), url=str(resp.url), payload=payload)

        detail = self._extract_detail(resp)
        payload = {"path": path_or_url, "status_code": int(resp.status_code), "detail": detail}

        if resp.status_code in {401, 403}:
            raise MassiveAuthError("Massive auth failed.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 404:
            raise MassiveNotFoundError(detail or "Not found.", status_code=resp.status_code, detail=detail, payload=payload)
        if resp.status_code == 429:
            raise MassiveRateLimitError(detail or "Rate limited.", status_code=resp.status_code, detail=detail, payload=payload)
        if 500 <= resp.status_code <= 599:
            raise MassiveServerError(detail or "Massive server error.", status_code=resp.status_code, detail=detail, payload=payload)

        raise MassiveError(
            f"Massive error (status={resp.status_code}).",
            status_code=resp.status_code,
            detail=detail,
            payload=payload,
        )

    # ------------------------------
    # OHLCV
    # ------------------------------

    def get_daily_ticker_summary(self, *, ticker: str, date: str, adjusted: bool = True) -> Any:
        """Single-day open/close summary.

        REST endpoint: ``GET /v1/open-close/{stocksTicker}/{date}``.
        """
        sym = str(ticker or "").strip().upper()
        day = str(date or "").strip()
        if not sym:
            raise ValueError("ticker is required")
        if not day:
            raise ValueError("date is required")
        path = _ENDPOINT_OPEN_CLOSE_TEMPLATE.format(ticker=sym, date=day)
        params = {"adjusted": "true" if adjusted else "false"}
        return self._request_json(path, params=params).payload

    def list_ohlcv(
        self,
        *,
        ticker: str,
        multiplier: int = 1,
        timespan: str = "day",
        from_: str,
        to: str,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 50000,
        pagination: bool = True,
    ) -> list[dict[str, Any]]:
        """Return OHLCV bars for a ticker.

        When the official SDK is installed, this method uses ``RESTClient.list_aggs``.
        Otherwise it calls ``/v2/aggs`` directly.
        """

        sym = str(ticker or "").strip().upper()
        if not sym:
            raise ValueError("ticker is required")

        path = _ENDPOINT_AGGS_RANGE_TEMPLATE.format(
            ticker=sym,
            multiplier=int(multiplier),
            timespan=str(timespan),
            from_=str(from_),
            to=str(to),
        )
        params = {
            "adjusted": "true" if adjusted else "false",
            "sort": str(sort),
            "limit": int(limit),
        }

        bars: list[dict[str, Any]] = []
        next_url: Optional[str] = None

        resp = self._request_json(path, params=params)
        payload = resp.payload
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                bars.extend([to_jsonable(r) for r in results])
            next_url = payload.get("next_url") if pagination else None
        else:
            raise MassiveError("Unexpected Massive aggs response.", payload={"url": resp.url})

        while pagination and next_url:
            resp = self._request_json(str(next_url))
            payload = resp.payload
            if not isinstance(payload, dict):
                break
            results = payload.get("results")
            if isinstance(results, list):
                bars.extend([to_jsonable(r) for r in results])
            next_url = payload.get("next_url")

        return bars

    # ------------------------------
    # Fundamentals & Financials
    # ------------------------------

    def _request_paginated_results(
        self,
        endpoint: str,
        *,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        resp = self._request_json(endpoint, params=params)
        payload = resp.payload
        if not pagination:
            return payload

        if not isinstance(payload, dict):
            return payload

        initial_results = payload.get("results")
        if not isinstance(initial_results, list):
            return payload

        merged_results = [to_jsonable(item) for item in initial_results]
        next_url = payload.get("next_url")

        while next_url:
            next_resp = self._request_json(str(next_url))
            next_payload = next_resp.payload
            if not isinstance(next_payload, dict):
                break
            next_results = next_payload.get("results")
            if not isinstance(next_results, list):
                break
            merged_results.extend([to_jsonable(item) for item in next_results])
            next_url = next_payload.get("next_url")

        merged_payload = dict(payload)
        merged_payload["results"] = merged_results
        merged_payload["next_url"] = None
        return merged_payload

    def get_short_interest(
        self,
        *,
        ticker: str,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Short interest.

        REST endpoint: ``GET /stocks/v1/short-interest``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_paginated_results(
            _ENDPOINT_SHORT_INTEREST,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def get_short_volume(
        self,
        *,
        ticker: str,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Short volume.

        REST endpoint: ``GET /stocks/v1/short-volume``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_paginated_results(
            _ENDPOINT_SHORT_VOLUME,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def get_float(
        self,
        *,
        ticker: str,
        as_of: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Company float (experimental).

        REST endpoint: ``GET /stocks/vX/float``.
        """

        q: dict[str, Any] = {"ticker": str(ticker).strip().upper(), "as_of": as_of}
        if params:
            q.update(params)
        endpoint = str(getattr(self.config, "float_endpoint", _ENDPOINT_FLOAT_DEFAULT) or _ENDPOINT_FLOAT_DEFAULT).strip()
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return self._request_paginated_results(
            endpoint,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def get_income_statement(
        self,
        *,
        ticker: str,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Income statements.

        REST endpoint: ``GET /stocks/financials/v1/income-statements``.
        """

        q = {"tickers": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_paginated_results(
            _ENDPOINT_FINANCIALS_INCOME,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def get_cash_flow_statement(
        self,
        *,
        ticker: str,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Cash-flow statements.

        REST endpoint: ``GET /stocks/financials/v1/cash-flow-statements``.
        """

        q = {"tickers": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_paginated_results(
            _ENDPOINT_FINANCIALS_CASH_FLOW,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def get_balance_sheet(
        self,
        *,
        ticker: str,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Balance sheets.

        REST endpoint: ``GET /stocks/financials/v1/balance-sheets``.
        """

        q = {"tickers": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_paginated_results(
            _ENDPOINT_FINANCIALS_BALANCE_SHEET,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def get_ratios(
        self,
        *,
        ticker: str,
        params: Optional[dict[str, Any]] = None,
        pagination: bool = True,
    ) -> Any:
        """Financial ratios.

        REST endpoint: ``GET /stocks/financials/v1/ratios``.
        """

        q = {"ticker": str(ticker).strip().upper()}
        if params:
            q.update(params)
        return self._request_paginated_results(
            _ENDPOINT_FINANCIALS_RATIOS,
            params=filter_none(q),
            pagination=bool(pagination),
        )

    def _normalize_tickers(self, tickers: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in tickers:
            ticker = str(raw or "").strip().upper()
            if not ticker:
                continue
            if ticker in seen:
                continue
            seen.add(ticker)
            normalized.append(ticker)
        return normalized

    def _iter_ticker_chunks(self, tickers: list[str], chunk_size: int) -> list[list[str]]:
        out: list[list[str]] = []
        size = max(1, int(chunk_size))
        for start in range(0, len(tickers), size):
            out.append(tickers[start : start + size])
        return out

    def get_unified_snapshot(
        self,
        *,
        tickers: list[str],
        asset_type: str = "stocks",
        limit: int = _DEFAULT_SNAPSHOT_LIMIT,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Unified multi-ticker snapshot via ``GET /v3/snapshot``.

        Massive supports up to 250 tickers per request (`ticker.any_of`). This
        method transparently chunks larger requests and merges all rows.
        """

        normalized_tickers = self._normalize_tickers(tickers)
        if not normalized_tickers:
            raise ValueError("tickers is required")

        requested_limit = max(1, min(int(limit), _MAX_SNAPSHOT_TICKERS_PER_REQUEST))

        merged_results: list[Any] = []
        merged_payload: Optional[dict[str, Any]] = None
        request_count = 0

        for chunk in self._iter_ticker_chunks(normalized_tickers, _MAX_SNAPSHOT_TICKERS_PER_REQUEST):
            q: dict[str, Any] = dict(params or {})
            # Massive rejects combining ticker filters with type on /v3/snapshot.
            q.pop("type", None)
            q.update(
                {
                    "ticker.any_of": ",".join(chunk),
                    "limit": min(requested_limit, max(1, len(chunk))),
                }
            )
            payload = self._request_paginated_results(
                _ENDPOINT_UNIFIED_SNAPSHOT,
                params=filter_none(q),
                pagination=True,
            )
            request_count += 1

            if not isinstance(payload, dict):
                raise MassiveError(
                    "Unexpected Massive snapshot response.",
                    payload={"endpoint": _ENDPOINT_UNIFIED_SNAPSHOT, "response_type": type(payload).__name__},
                )

            if merged_payload is None:
                merged_payload = dict(payload)

            rows = payload.get("results")
            if isinstance(rows, list):
                merged_results.extend([to_jsonable(row) for row in rows])

        out = dict(merged_payload or {})
        out["results"] = merged_results
        out["next_url"] = None
        out["request_count"] = request_count
        out["symbols_requested"] = normalized_tickers
        return out
