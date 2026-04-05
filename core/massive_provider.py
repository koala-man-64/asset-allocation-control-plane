from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://api.massive.com"
_DEFAULT_TIMEOUT_SECONDS = 30.0
_DEFAULT_PAGE_LIMIT = 1000
_MAX_PAGE_LIMIT = 1000


class MassiveProviderError(RuntimeError):
    """Raised when Massive ticker retrieval fails."""


@dataclass(frozen=True)
class MassiveProviderConfig:
    api_key: str
    base_url: str = _DEFAULT_BASE_URL
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    page_limit: int = _DEFAULT_PAGE_LIMIT


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_positive_float(value: object, *, default: float) -> float:
    if value is None:
        return float(default)
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    if parsed <= 0:
        return float(default)
    return float(parsed)


def _to_page_limit(value: object, *, default: int) -> int:
    if value is None:
        return int(default)
    try:
        parsed = int(value)
    except Exception:
        return int(default)
    if parsed <= 0:
        return int(default)
    return int(min(parsed, _MAX_PAGE_LIMIT))


def _to_optional_bool(value: object) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return None


def _bool_to_query(value: bool) -> str:
    return "true" if value else "false"


def _clean_text(value: object) -> Optional[str]:
    text = _strip_or_none(value)
    if text is None:
        return None
    return text


def _with_api_key(url: str, api_key: str) -> str:
    parsed = urlparse(url)
    query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if not query_pairs.get("apiKey"):
        query_pairs["apiKey"] = api_key
    return urlunparse(parsed._replace(query=urlencode(query_pairs, doseq=True)))


class MassiveProvider:
    """
    Lightweight client for Massive REST `/v3/reference/tickers`.

    It follows every `next_url` page until exhaustion to return the complete ticker set
    for the selected query filter.
    """

    def __init__(
        self,
        config: MassiveProviderConfig,
        *,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = MassiveProviderConfig(
            api_key=str(config.api_key).strip(),
            base_url=(str(config.base_url).strip() or _DEFAULT_BASE_URL).rstrip("/"),
            timeout_seconds=_to_positive_float(config.timeout_seconds, default=_DEFAULT_TIMEOUT_SECONDS),
            page_limit=_to_page_limit(config.page_limit, default=_DEFAULT_PAGE_LIMIT),
        )
        self._owns_session = session is None
        self._session = session or requests.Session()

    @classmethod
    def from_env(cls, *, session: Optional[requests.Session] = None) -> "MassiveProvider":
        api_key = _strip_or_none(os.environ.get("MASSIVE_API_KEY"))
        if not api_key:
            raise MassiveProviderError("MASSIVE_API_KEY is required.")

        base_url = _strip_or_none(os.environ.get("MASSIVE_BASE_URL")) or _DEFAULT_BASE_URL
        timeout_seconds = _to_positive_float(
            _strip_or_none(os.environ.get("MASSIVE_TIMEOUT_SECONDS")),
            default=_DEFAULT_TIMEOUT_SECONDS,
        )
        page_limit = _to_page_limit(
            _strip_or_none(os.environ.get("MASSIVE_TICKERS_PAGE_LIMIT")),
            default=_DEFAULT_PAGE_LIMIT,
        )

        return cls(
            MassiveProviderConfig(
                api_key=api_key,
                base_url=base_url,
                timeout_seconds=timeout_seconds,
                page_limit=page_limit,
            ),
            session=session,
        )

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "MassiveProvider":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _request_json(
        self,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        try:
            response = self._session.get(
                url,
                params=params or None,
                timeout=float(self.config.timeout_seconds),
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise MassiveProviderError(
                f"Massive request failed for url={url!r}: {type(exc).__name__}: {exc}"
            ) from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise MassiveProviderError(f"Massive response was not valid JSON for url={url!r}.") from exc

        if not isinstance(payload, dict):
            raise MassiveProviderError(
                f"Massive response was not a JSON object for url={url!r} (type={type(payload).__name__})."
            )
        return payload

    def _normalize_next_url(self, next_url: object) -> Optional[str]:
        raw = _strip_or_none(next_url)
        if not raw:
            return None

        absolute = urljoin(f"{self.config.base_url}/", raw)
        parsed = urlparse(absolute)
        if not parsed.scheme or not parsed.netloc:
            raise MassiveProviderError(f"Invalid next_url returned by Massive: {raw!r}")
        return _with_api_key(absolute, self.config.api_key)

    def list_tickers(
        self,
        *,
        market: str = "stocks",
        locale: Optional[str] = "us",
        active: bool = True,
        ticker_type: Optional[str] = None,
        include_otc: Optional[bool] = None,
        sort: str = "ticker",
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        """
        Retrieve all ticker rows for the selected Massive query, paging until complete.
        """
        endpoint = f"{self.config.base_url}/v3/reference/tickers"
        params: dict[str, Any] = {
            "market": market,
            "active": _bool_to_query(bool(active)),
            "limit": int(self.config.page_limit),
            "sort": sort,
            "order": order,
            "apiKey": self.config.api_key,
        }
        if locale:
            params["locale"] = locale
        if ticker_type:
            params["type"] = ticker_type
        include_otc_bool = _to_optional_bool(include_otc)
        if include_otc_bool is not None:
            params["include_otc"] = _bool_to_query(include_otc_bool)

        out: list[dict[str, Any]] = []
        url: Optional[str] = endpoint
        next_params: Optional[dict[str, Any]] = params
        seen_urls: set[str] = set()

        while url:
            if url in seen_urls:
                raise MassiveProviderError(f"Massive pagination loop detected at url={url!r}.")
            seen_urls.add(url)

            payload = self._request_json(url, params=next_params)
            next_params = None

            results = payload.get("results")
            if results is None:
                results = []
            if not isinstance(results, list):
                raise MassiveProviderError(
                    f"Massive response field `results` was not a list for url={url!r}."
                )

            for item in results:
                if isinstance(item, dict):
                    out.append(item)

            url = self._normalize_next_url(payload.get("next_url"))

        return out


_NORMALIZED_COLUMNS = [
    "Symbol",
    "Name",
    "Exchange",
    "AssetType",
    "Locale",
    "Market",
    "CurrencyName",
    "Active",
    "source_massive",
]


def tickers_to_dataframe(records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        symbol_raw = _clean_text(item.get("ticker"))
        if not symbol_raw:
            continue
        symbol = symbol_raw.upper()

        active_value = _to_optional_bool(item.get("active"))
        if active_value is None and item.get("active") is not None:
            active_value = bool(item.get("active"))

        rows.append(
            {
                "Symbol": symbol,
                "Name": _clean_text(item.get("name")),
                "Exchange": _clean_text(item.get("primary_exchange")),
                "AssetType": _clean_text(item.get("type")),
                "Locale": _clean_text(item.get("locale")),
                "Market": _clean_text(item.get("market")),
                "CurrencyName": _clean_text(item.get("currency_name")),
                "Active": active_value,
                "source_massive": True,
            }
        )

    if not rows:
        return pd.DataFrame(columns=_NORMALIZED_COLUMNS)

    df = pd.DataFrame(rows, columns=_NORMALIZED_COLUMNS)
    df = df.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    return df


def get_complete_ticker_list(
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_seconds: Optional[float] = None,
    page_limit: Optional[int] = None,
    market: str = "stocks",
    locale: Optional[str] = "us",
    active: bool = True,
    ticker_type: Optional[str] = None,
    include_otc: Optional[bool] = None,
    sort: str = "ticker",
    order: str = "asc",
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """
    Convenience entrypoint: fetch every ticker page and return a normalized DataFrame.
    """
    resolved_key = _strip_or_none(api_key) or _strip_or_none(os.environ.get("MASSIVE_API_KEY"))
    if not resolved_key:
        raise MassiveProviderError("MASSIVE_API_KEY is required to fetch tickers from Massive.")

    resolved_base_url = _strip_or_none(base_url) or _strip_or_none(os.environ.get("MASSIVE_BASE_URL")) or _DEFAULT_BASE_URL
    resolved_timeout = _to_positive_float(
        timeout_seconds if timeout_seconds is not None else _strip_or_none(os.environ.get("MASSIVE_TIMEOUT_SECONDS")),
        default=_DEFAULT_TIMEOUT_SECONDS,
    )
    resolved_page_limit = _to_page_limit(
        page_limit if page_limit is not None else _strip_or_none(os.environ.get("MASSIVE_TICKERS_PAGE_LIMIT")),
        default=_DEFAULT_PAGE_LIMIT,
    )

    config = MassiveProviderConfig(
        api_key=resolved_key,
        base_url=resolved_base_url,
        timeout_seconds=resolved_timeout,
        page_limit=resolved_page_limit,
    )
    with MassiveProvider(config, session=session) as provider:
        records = provider.list_tickers(
            market=market,
            locale=locale,
            active=active,
            ticker_type=ticker_type,
            include_otc=include_otc,
            sort=sort,
            order=order,
        )
    return tickers_to_dataframe(records)
