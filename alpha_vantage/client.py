"""
High‑level Alpha Vantage API client.

This module defines the :class:`AlphaVantageClient` class which
exposes methods corresponding to the various functions described in
the official Alpha Vantage documentation【23†L141-L149】【34†L374-L382】.  The
client takes care of appending your API key to each request, waiting
between calls to respect rate limits and optionally fetching multiple
symbols in parallel.

Typical usage looks like this::

    from alpha_vantage import AlphaVantageClient, AlphaVantageConfig

    cfg = AlphaVantageConfig(api_key="YOUR_KEY", rate_limit_per_min=60, max_workers=5)
    av = AlphaVantageClient(cfg)
    # Fetch daily data for a single symbol
    data = av.get_daily_time_series("AAPL", outputsize="full")
    # Convert to DataFrame
    df = av.parse_time_series(data)
    # Fetch multiple symbols concurrently
    requests = [
        {"function": "TIME_SERIES_DAILY", "symbol": "MSFT", "outputsize": "compact"},
        {"function": "TIME_SERIES_DAILY", "symbol": "TSLA", "outputsize": "compact"},
    ]
    results = av.fetch_many(requests)

See the ``api_keys`` page on Alpha Vantage for rate limits and
subscription tiers【7†L49-L53】【6†L2153-L2156】.
"""

from __future__ import annotations

import itertools
import logging
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

import httpx

from .config import AlphaVantageConfig
from .errors import AlphaVantageError, AlphaVantageInvalidSymbolError, AlphaVantageThrottleError
from .rate_limiter import RateLimiter
from .utils import parse_time_series, parse_financial_reports


logger = logging.getLogger(__name__)

_APIKEY_QUERY_RE = re.compile(r"(apikey=)([^&\s]+)", re.IGNORECASE)
_HARD_DAILY_QUOTA_PATTERNS = (
    "standard api rate limit is 25 requests per day",
    "daily rate limit",
    "daily rate limits",
)


class AlphaVantageClient:
    """Client for interacting with the Alpha Vantage REST API.

    Parameters
    ----------
    config : AlphaVantageConfig
        Configuration object holding your API key, rate limit and
        connection settings.
    """

    def __init__(
        self,
        config: AlphaVantageConfig,
        *,
        http_client: Optional[httpx.Client] = None,
        caller_provider: Optional[Callable[[], str]] = None,
    ) -> None:
        self.config = config
        self._rate_limiter = RateLimiter(config.rate_limit_per_min)
        self._caller_provider = caller_provider
        # httpx will attempt to use proxy settings from the environment by
        # default.  In containerized deployments this can result in an
        # ImportError when the optional ``socksio`` dependency is not
        # installed.  Setting ``trust_env=False`` prevents httpx from
        # reading proxy configuration from the environment and avoids
        # that error.
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=config.timeout, trust_env=False)
        self._query_url = config.get_query_url()

        self._request_seq = itertools.count(1)
        self._metrics_lock = threading.Lock()
        self._metrics: Dict[str, Union[int, float]] = {
            "logical_requests": 0,
            "http_calls": 0,
            "success": 0,
            "failed": 0,
            "retries": 0,
            "rate_wait_timeouts": 0,
            "throttle_payloads": 0,
            "invalid_symbols": 0,
            "http_status_errors": 0,
            "network_errors": 0,
            "invalid_json": 0,
            "total_success_ms": 0.0,
            "throttle_cooldown_waits": 0,
        }
        self._summary_interval_seconds = 60.0
        self._started_monotonic = time.monotonic()
        self._last_summary_monotonic = self._started_monotonic
        self._throttle_cooldown_lock = threading.Lock()
        self._throttle_cooldown_until_monotonic = 0.0

        self._log(
            logging.INFO,
            "Alpha Vantage client initialized",
            av_event="client_init",
            av_base_url=str(getattr(config, "base_url", "")),
            av_query_url=str(self._query_url),
            av_timeout_seconds=float(getattr(config, "timeout", 0.0)),
            av_rate_limit_per_min=int(getattr(config, "rate_limit_per_min", 0)),
            av_max_workers=int(getattr(config, "max_workers", 0)),
            av_max_retries=int(getattr(config, "max_retries", 0)),
            av_backoff_base_seconds=float(getattr(config, "backoff_base_seconds", 0.0)),
            av_throttle_cooldown_seconds=float(getattr(config, "throttle_cooldown_seconds", 60.0)),
            av_api_key_set=bool(getattr(config, "api_key", "")),
            av_caller_provider=bool(caller_provider),
        )

    def _resolve_caller(self) -> Optional[str]:
        provider = self._caller_provider
        if provider is None:
            return None
        try:
            resolved = provider()
        except Exception:
            return None
        text = str(resolved or "").strip()
        return text or None

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "AlphaVantageClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _classify_payload_error(payload: Mapping[str, Any]) -> Optional[AlphaVantageError]:
        """
        Alpha Vantage often returns HTTP 200 with an error payload.

        Common patterns:
        - {"Note": "..."} (throttle)
        - {"Information": "..."} (throttle / informational)
        - {"Error Message": "..."} (invalid symbol / bad request)
        """
        note = payload.get("Note") or payload.get("Information")
        if isinstance(note, str) and note.strip():
            return AlphaVantageThrottleError(note.strip(), payload=payload)

        error_message = payload.get("Error Message")
        if isinstance(error_message, str) and error_message.strip():
            text = error_message.strip()
            lowered = text.lower()
            if "invalid api call" in lowered or "invalid symbol" in lowered:
                return AlphaVantageInvalidSymbolError(text, payload=payload)
            return AlphaVantageError(text, code="api_error", payload=payload)

        return None

    @staticmethod
    def _is_hard_daily_quota_throttle(exc: AlphaVantageThrottleError) -> bool:
        message = str(getattr(exc, "message", "") or str(exc) or "").casefold()
        if "premium plan" in message and any(pattern in message for pattern in _HARD_DAILY_QUOTA_PATTERNS):
            return True
        return "standard api rate limit is 25 requests per day" in message

    @staticmethod
    def _try_parse_json(text: str) -> Optional[Dict[str, Any]]:
        raw = (text or "").strip()
        if not raw or not raw.startswith("{"):
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        if isinstance(parsed, dict):
            return parsed
        return None

    def _log(self, level: int, message: str, **context: Any) -> None:
        if not logger.isEnabledFor(level):
            return

        def redact(text: str) -> str:
            out = text
            api_key = str(getattr(self.config, "api_key", "") or "")
            if api_key:
                out = re.sub(re.escape(api_key), "[REDACTED]", out, flags=re.IGNORECASE)
            out = _APIKEY_QUERY_RE.sub(r"\1[REDACTED]", out)
            return out

        def sanitize(value: Any) -> Any:
            if value is None or isinstance(value, (bool, int, float)):
                return value
            if isinstance(value, str):
                return redact(value)
            if isinstance(value, list):
                return [sanitize(v) for v in value]
            if isinstance(value, dict):
                return {k: sanitize(v) for k, v in value.items()}
            return redact(str(value))

        safe_context = {"provider": "alpha_vantage"}
        safe_context.update({k: sanitize(v) for k, v in context.items() if v is not None})
        try:
            logger.log(level, message, extra={"context": safe_context})
        except Exception:
            # Logging must never break ingestion; fall back to a plain message.
            logger.log(level, message)

    def _sanitize_params_for_log(self, params: Mapping[str, Any]) -> Dict[str, Any]:
        safe: Dict[str, Any] = {}
        for key, value in params.items():
            lowered = str(key).lower()
            if lowered in {"apikey", "api_key"} or "key" in lowered or "token" in lowered:
                continue
            if value is None:
                safe[key] = None
                continue
            if isinstance(value, (bool, int, float)):
                safe[key] = value
                continue
            text = str(value)
            if len(text) > 160:
                text = text[:160] + "..."
            safe[key] = text
        return safe

    def _record_metric(self, key: str, inc: Union[int, float] = 1) -> None:
        with self._metrics_lock:
            current = self._metrics.get(key, 0)
            self._metrics[key] = current + inc  # type: ignore[operator]

    def _maybe_log_summary(self) -> None:
        if not logger.isEnabledFor(logging.INFO):
            return
        now = time.monotonic()
        if (now - self._last_summary_monotonic) < self._summary_interval_seconds:
            return
        with self._metrics_lock:
            now = time.monotonic()
            if (now - self._last_summary_monotonic) < self._summary_interval_seconds:
                return
            self._last_summary_monotonic = now
            snapshot = dict(self._metrics)

        success = int(snapshot.get("success") or 0)
        avg_ms = float(snapshot.get("total_success_ms") or 0.0) / float(max(1, success))
        elapsed_s = now - self._started_monotonic

        self._log(
            logging.INFO,
            "Alpha Vantage client progress",
            av_event="client_progress",
            av_elapsed_seconds=round(elapsed_s, 1),
            av_logical_requests=int(snapshot.get("logical_requests") or 0),
            av_http_calls=int(snapshot.get("http_calls") or 0),
            av_success=int(snapshot.get("success") or 0),
            av_failed=int(snapshot.get("failed") or 0),
            av_retries=int(snapshot.get("retries") or 0),
            av_rate_wait_timeouts=int(snapshot.get("rate_wait_timeouts") or 0),
            av_throttle_payloads=int(snapshot.get("throttle_payloads") or 0),
            av_invalid_symbols=int(snapshot.get("invalid_symbols") or 0),
            av_http_status_errors=int(snapshot.get("http_status_errors") or 0),
            av_network_errors=int(snapshot.get("network_errors") or 0),
            av_invalid_json=int(snapshot.get("invalid_json") or 0),
            av_throttle_cooldown_waits=int(snapshot.get("throttle_cooldown_waits") or 0),
            av_avg_success_ms=round(avg_ms, 1),
        )

    def _get_throttle_cooldown_seconds(self) -> float:
        try:
            raw = float(getattr(self.config, "throttle_cooldown_seconds", 60.0))
        except Exception:
            raw = 60.0
        return max(0.0, raw)

    def _arm_throttle_cooldown(
        self,
        *,
        req_id: int,
        function: Optional[str],
        symbol: Optional[str],
        reason: str,
    ) -> None:
        cooldown_seconds = self._get_throttle_cooldown_seconds()
        if cooldown_seconds <= 0:
            return

        now = time.monotonic()
        with self._throttle_cooldown_lock:
            previous_until = self._throttle_cooldown_until_monotonic
            cooldown_until = max(previous_until, now + cooldown_seconds)
            self._throttle_cooldown_until_monotonic = cooldown_until

        if cooldown_until > previous_until:
            self._log(
                logging.WARNING,
                "Alpha Vantage throttle cooldown armed",
                av_event="throttle_cooldown_set",
                av_request_id=req_id,
                av_function=function,
                av_symbol=symbol,
                av_reason=reason,
                av_cooldown_seconds=round(cooldown_seconds, 3),
                av_resume_in_seconds=round(max(0.0, cooldown_until - now), 3),
            )

    def _wait_for_throttle_cooldown(
        self,
        *,
        req_id: int,
        function: Optional[str],
        symbol: Optional[str],
        attempt: int,
        caller: Optional[str],
    ) -> None:
        with self._throttle_cooldown_lock:
            remaining = self._throttle_cooldown_until_monotonic - time.monotonic()
        if remaining <= 0:
            return

        self._record_metric("throttle_cooldown_waits", 1)
        level = logging.WARNING if attempt == 0 else logging.DEBUG
        self._log(
            level,
            "Alpha Vantage throttle cooldown active; waiting before outbound request",
            av_event="throttle_cooldown_wait",
            av_request_id=req_id,
            av_function=function,
            av_symbol=symbol,
            av_attempt=attempt,
            av_caller=caller,
            av_sleep_seconds=round(float(remaining), 3),
        )
        time.sleep(max(0.0, float(remaining)))

    def _sleep_backoff(
        self,
        attempt: int,
        *,
        req_id: int,
        function: Optional[str],
        symbol: Optional[str],
        reason: str,
        status_code: Optional[int] = None,
    ) -> None:
        throttle_reasons = {"throttle_payload", "http_429"}
        if reason in throttle_reasons:
            sleep_seconds = self._get_throttle_cooldown_seconds()
        else:
            base = max(0.0, float(getattr(self.config, "backoff_base_seconds", 0.5)))
            # Exponential backoff with jitter; cap at 60s to avoid runaway sleeps.
            sleep_seconds = min(60.0, base * (2.0**attempt))
            sleep_seconds += random.uniform(0.0, min(1.0, sleep_seconds * 0.2))

        warn_reasons = {"throttle_payload", "http_429"}
        level = logging.WARNING if (attempt == 0 and reason in warn_reasons) else logging.DEBUG
        self._log(
            level,
            "Alpha Vantage backing off before retry",
            av_event="backoff",
            av_request_id=req_id,
            av_function=function,
            av_symbol=symbol,
            av_attempt=attempt,
            av_reason=reason,
            av_status_code=status_code,
            av_sleep_seconds=round(float(sleep_seconds), 3),
        )
        time.sleep(sleep_seconds)

    def _request(self, params: Dict[str, Any], raw: bool = False) -> Union[Dict[str, Any], str]:
        """Perform a GET request to the Alpha Vantage API.

        Parameters
        ----------
        params : dict
            Dictionary of query parameters.  Must contain at least a
            ``"function"`` entry.
        raw : bool, optional
            If ``True``, return the raw response text instead of
            attempting to parse JSON.  Use this for CSV endpoints.

        Returns
        -------
        dict or str
            Parsed JSON object or raw text depending on ``raw``.

        Raises
        ------
        httpx.HTTPStatusError
            If the response indicates an HTTP error.  Alpha Vantage
            returns ``200 OK`` for most errors, in which case the
            message will be contained in the JSON payload.
        """
        max_retries = max(0, int(getattr(self.config, "max_retries", 0)))

        req_id = next(self._request_seq)
        function = str(params.get("function") or "")
        symbol = params.get("symbol")
        safe_params = self._sanitize_params_for_log(params)
        caller = self._resolve_caller()
        rate_wait_timeout_seconds = getattr(self.config, "rate_wait_timeout_seconds", None)

        self._record_metric("logical_requests", 1)
        self._log(
            logging.DEBUG,
            "Alpha Vantage request started",
            av_event="request_start",
            av_request_id=req_id,
            av_function=function,
            av_symbol=symbol,
            av_raw=bool(raw),
            av_max_retries=max_retries,
            av_caller=caller,
            av_rate_wait_timeout_seconds=rate_wait_timeout_seconds,
            av_params=safe_params,
        )

        try:
            for attempt in range(max_retries + 1):
                self._wait_for_throttle_cooldown(
                    req_id=req_id,
                    function=function or None,
                    symbol=str(symbol) if symbol is not None else None,
                    attempt=attempt,
                    caller=caller,
                )

                wait_started = time.monotonic()
                try:
                    self._rate_limiter.wait(caller=caller, timeout_seconds=rate_wait_timeout_seconds)
                except TimeoutError as exc:
                    self._record_metric("rate_wait_timeouts", 1)
                    if attempt < max_retries:
                        self._record_metric("retries", 1)
                        self._sleep_backoff(
                            attempt,
                            req_id=req_id,
                            function=function or None,
                            symbol=str(symbol) if symbol is not None else None,
                            reason="rate_wait_timeout",
                        )
                        continue
                    raise AlphaVantageThrottleError(
                        "Timed out waiting for Alpha Vantage rate-limit capacity.",
                        payload={"caller": caller, "rate_wait_timeout_seconds": rate_wait_timeout_seconds},
                    ) from exc
                rate_wait_ms = (time.monotonic() - wait_started) * 1000.0

                self._wait_for_throttle_cooldown(
                    req_id=req_id,
                    function=function or None,
                    symbol=str(symbol) if symbol is not None else None,
                    attempt=attempt,
                    caller=caller,
                )

                query_params = dict(params)
                query_params["apikey"] = self.config.api_key

                self._record_metric("http_calls", 1)
                attempt_started = time.monotonic()
                self._log(
                    logging.DEBUG,
                    "Alpha Vantage request attempt",
                    av_event="request_attempt",
                    av_request_id=req_id,
                    av_function=function,
                    av_symbol=symbol,
                    av_raw=bool(raw),
                    av_attempt=attempt,
                    av_caller=caller,
                    av_rate_wait_ms=round(rate_wait_ms, 1),
                    av_params=safe_params,
                )

                try:
                    response = self._client.get(self._query_url, params=query_params)
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    self._record_metric("http_status_errors", 1)
                    status = exc.response.status_code
                    # Retry 429/5xx; fail fast on other 4xx.
                    if status == 429 or status >= 500:
                        if status == 429:
                            self._arm_throttle_cooldown(
                                req_id=req_id,
                                function=function or None,
                                symbol=str(symbol) if symbol is not None else None,
                                reason="http_429",
                            )
                        if attempt < max_retries:
                            self._record_metric("retries", 1)
                            self._sleep_backoff(
                                attempt,
                                req_id=req_id,
                                function=function or None,
                                symbol=str(symbol) if symbol is not None else None,
                                reason="http_429" if status == 429 else "http_5xx",
                                status_code=int(status),
                            )
                            continue
                    raise
                except httpx.RequestError:
                    self._record_metric("network_errors", 1)
                    if attempt < max_retries:
                        self._record_metric("retries", 1)
                        self._sleep_backoff(
                            attempt,
                            req_id=req_id,
                            function=function or None,
                            symbol=str(symbol) if symbol is not None else None,
                            reason="network_error",
                        )
                        continue
                    raise

                # Attempt to interpret an error payload even for raw CSV endpoints.
                if raw:
                    text = response.text
                    parsed = self._try_parse_json(text)
                    if parsed is not None:
                        classified = self._classify_payload_error(parsed)
                        if classified is not None:
                            if isinstance(classified, AlphaVantageThrottleError):
                                self._record_metric("throttle_payloads", 1)
                                if self._is_hard_daily_quota_throttle(classified):
                                    note = str(getattr(classified, "message", "") or "")
                                    if len(note) > 200:
                                        note = note[:200] + "..."
                                    self._log(
                                        logging.WARNING,
                                        "Alpha Vantage hard daily quota payload detected (CSV)",
                                        av_event="hard_daily_quota",
                                        av_request_id=req_id,
                                        av_function=function,
                                        av_symbol=str(symbol) if symbol is not None else None,
                                        av_attempt=attempt,
                                        av_caller=caller,
                                        av_note=note,
                                    )
                                    raise classified
                                self._arm_throttle_cooldown(
                                    req_id=req_id,
                                    function=function or None,
                                    symbol=str(symbol) if symbol is not None else None,
                                    reason="throttle_payload",
                                )
                            if isinstance(classified, AlphaVantageThrottleError) and attempt < max_retries:
                                self._record_metric("retries", 1)
                                # Log throttling at warning once per request; subsequent retries are debug noise.
                                level = logging.WARNING if attempt == 0 else logging.DEBUG
                                note = str(getattr(classified, "message", "") or "")
                                if len(note) > 200:
                                    note = note[:200] + "..."
                                self._log(
                                    level,
                                    "Alpha Vantage throttle payload detected (CSV)",
                                    av_event="throttle",
                                    av_request_id=req_id,
                                    av_function=function,
                                    av_symbol=str(symbol) if symbol is not None else None,
                                    av_attempt=attempt,
                                    av_caller=caller,
                                    av_note=note,
                                )
                                self._sleep_backoff(
                                    attempt,
                                    req_id=req_id,
                                    function=function or None,
                                    symbol=str(symbol) if symbol is not None else None,
                                    reason="throttle_payload",
                                )
                                continue
                            raise classified
                    elapsed_ms = (time.monotonic() - attempt_started) * 1000.0
                    self._record_metric("success", 1)
                    self._record_metric("total_success_ms", float(elapsed_ms))
                    self._log(
                        logging.DEBUG,
                        "Alpha Vantage request succeeded (CSV)",
                        av_event="request_success",
                        av_request_id=req_id,
                        av_function=function,
                        av_symbol=str(symbol) if symbol is not None else None,
                        av_raw=True,
                        av_attempt=attempt,
                        av_status_code=int(response.status_code),
                        av_elapsed_ms=round(elapsed_ms, 1),
                        av_response_chars=len(text or ""),
                    )
                    self._maybe_log_summary()
                    return text

                try:
                    payload = response.json()
                except Exception as exc:
                    if attempt < max_retries:
                        self._record_metric("retries", 1)
                        self._record_metric("invalid_json", 1)
                        self._sleep_backoff(
                            attempt,
                            req_id=req_id,
                            function=function or None,
                            symbol=str(symbol) if symbol is not None else None,
                            reason="invalid_json",
                            status_code=int(response.status_code),
                        )
                        continue
                    self._record_metric("invalid_json", 1)
                    snippet = (response.text or "").strip().replace("\n", " ")
                    if len(snippet) > 200:
                        snippet = snippet[:200] + "..."
                    raise AlphaVantageError(
                        "Failed to parse JSON response from Alpha Vantage.",
                        code="invalid_json",
                        payload={"snippet": snippet},
                    ) from exc

                if isinstance(payload, dict):
                    classified = self._classify_payload_error(payload)
                    if classified is not None:
                        if isinstance(classified, AlphaVantageThrottleError):
                            self._record_metric("throttle_payloads", 1)
                            if self._is_hard_daily_quota_throttle(classified):
                                note = str(getattr(classified, "message", "") or "")
                                if len(note) > 200:
                                    note = note[:200] + "..."
                                self._log(
                                    logging.WARNING,
                                    "Alpha Vantage hard daily quota payload detected",
                                    av_event="hard_daily_quota",
                                    av_request_id=req_id,
                                    av_function=function,
                                    av_symbol=str(symbol) if symbol is not None else None,
                                    av_attempt=attempt,
                                    av_caller=caller,
                                    av_note=note,
                                )
                                raise classified
                            self._arm_throttle_cooldown(
                                req_id=req_id,
                                function=function or None,
                                symbol=str(symbol) if symbol is not None else None,
                                reason="throttle_payload",
                            )
                        if isinstance(classified, AlphaVantageThrottleError) and attempt < max_retries:
                            self._record_metric("retries", 1)
                            level = logging.WARNING if attempt == 0 else logging.DEBUG
                            note = str(getattr(classified, "message", "") or "")
                            if len(note) > 200:
                                note = note[:200] + "..."
                            self._log(
                                level,
                                "Alpha Vantage throttle payload detected",
                                av_event="throttle",
                                av_request_id=req_id,
                                av_function=function,
                                av_symbol=str(symbol) if symbol is not None else None,
                                av_attempt=attempt,
                                av_caller=caller,
                                av_note=note,
                            )
                            self._sleep_backoff(
                                attempt,
                                req_id=req_id,
                                function=function or None,
                                symbol=str(symbol) if symbol is not None else None,
                                reason="throttle_payload",
                            )
                            continue
                        if isinstance(classified, AlphaVantageInvalidSymbolError):
                            self._record_metric("invalid_symbols", 1)
                            self._log(
                                logging.INFO,
                                "Alpha Vantage invalid symbol payload",
                                av_event="invalid_symbol",
                                av_request_id=req_id,
                                av_function=function,
                                av_symbol=str(symbol) if symbol is not None else None,
                                av_attempt=attempt,
                                av_message=str(classified),
                            )
                        raise classified

                elapsed_ms = (time.monotonic() - attempt_started) * 1000.0
                self._record_metric("success", 1)
                self._record_metric("total_success_ms", float(elapsed_ms))
                payload_keys = []
                if isinstance(payload, dict):
                    try:
                        payload_keys = list(payload.keys())[:10]
                    except Exception:
                        payload_keys = []

                self._log(
                    logging.DEBUG,
                    "Alpha Vantage request succeeded",
                    av_event="request_success",
                    av_request_id=req_id,
                    av_function=function,
                    av_symbol=str(symbol) if symbol is not None else None,
                    av_raw=False,
                    av_attempt=attempt,
                    av_caller=caller,
                    av_status_code=int(response.status_code),
                    av_elapsed_ms=round(elapsed_ms, 1),
                    av_payload_keys=payload_keys,
                )
                self._maybe_log_summary()
                return payload  # type: ignore[return-value]
        except Exception as exc:
            self._record_metric("failed", 1)
            level = logging.ERROR
            if isinstance(exc, AlphaVantageInvalidSymbolError):
                level = logging.INFO
            elif isinstance(exc, AlphaVantageThrottleError):
                level = logging.WARNING

            # Best effort: include the attempt counter for the failure.
            attempt_no: Optional[int] = None
            try:
                attempt_no = int(attempt)  # type: ignore[name-defined]
            except Exception:
                attempt_no = None

            self._log(
                level,
                "Alpha Vantage request failed",
                av_event="request_failed",
                av_request_id=req_id,
                av_function=function,
                av_symbol=str(symbol) if symbol is not None else None,
                av_raw=bool(raw),
                av_attempt=attempt_no,
                av_caller=caller,
                av_max_retries=max_retries,
                av_error_type=type(exc).__name__,
                av_error=str(exc),
            )
            self._maybe_log_summary()
            raise
        raise RuntimeError("Unreachable")

    # ------------------------------------------------------------------
    # Generic request helpers
    # ------------------------------------------------------------------
    def fetch(self, function: str, symbol: Optional[str] = None, **params: Any) -> Dict[str, Any]:
        """Fetch a JSON response from any Alpha Vantage endpoint.

        Parameters
        ----------
        function : str
            The API function name (e.g. ``'TIME_SERIES_DAILY'``).
        symbol : str, optional
            The primary symbol for the request.  Many endpoints
            require this argument; for functions that do not take a
            symbol (e.g. macroeconomic indicators) set this to
            ``None``.
        **params : dict, optional
            Additional query parameters as documented by Alpha
            Vantage (e.g. ``interval``, ``outputsize``, ``datatype``).

        Returns
        -------
        dict
            The parsed JSON response.
        """
        query_params: Dict[str, Any] = {"function": function}
        if symbol:
            query_params["symbol"] = symbol
        # Merge additional parameters
        query_params.update(params)
        return self._request(query_params, raw=False)

    def fetch_csv(self, function: str, symbol: Optional[str] = None, **params: Any) -> str:
        """Fetch a CSV response from any Alpha Vantage endpoint.

        Alpha Vantage supports a ``datatype=csv`` parameter for many
        functions.  When this helper is used the raw CSV text is
        returned instead of JSON.

        Parameters
        ----------
        function : str
            API function name.
        symbol : str, optional
            Primary symbol for the request.
        **params : dict
            Additional query parameters.

        Returns
        -------
        str
            Raw CSV data as returned by the API.
        """
        query_params: Dict[str, Any] = {"function": function, "datatype": "csv"}
        if symbol:
            query_params["symbol"] = symbol
        query_params.update(params)
        return self._request(query_params, raw=True)

    def fetch_many(self, request_params: Iterable[Dict[str, Any]]) -> List[Union[Dict[str, Any], str]]:
        """Fetch multiple endpoints concurrently.

        This method accepts an iterable of parameter dictionaries.  Each
        dictionary must contain at least a ``"function"`` key and may
        optionally contain a ``"symbol"`` entry and any additional
        parameters supported by the API.  The calls will be executed
        concurrently using a thread pool limited by
        ``config.max_workers``.  Results are returned in the same order
        as the input sequence.

        Because all workers share the same rate limiter, the overall
        throughput will never exceed the configured calls per minute.

        Parameters
        ----------
        request_params : iterable of dict
            Each dict describes one API call with keys ``"function"``,
            ``"symbol"`` and other parameters.

        Returns
        -------
        list of dict or str
            List of parsed JSON objects or raw CSV strings in the same
            order as provided.
        """
        reqs = list(request_params)
        results: List[Union[Dict[str, Any], str]] = [None] * len(reqs)  # type: ignore[list-item]

        def worker(index: int, params: Dict[str, Any]) -> Union[Dict[str, Any], str]:
            # Unpack function and symbol from the dict; copy so we don't mutate the caller's data
            params_copy = dict(params)
            func = params_copy.pop("function")
            symbol = params_copy.pop("symbol", None)
            # Determine if CSV is requested based on explicit datatype
            datatype = params_copy.get("datatype")
            if datatype and str(datatype).lower() == "csv":
                return self.fetch_csv(func, symbol, **params_copy)
            return self.fetch(func, symbol, **params_copy)

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_index = {}
            for idx, params in enumerate(reqs):
                future_to_index[executor.submit(worker, idx, params)] = idx
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    results[idx] = {"error": str(e)}
        return results

    # ------------------------------------------------------------------
    # High‑level convenience methods
    # ------------------------------------------------------------------
    def get_listing_status(self, *, state: Optional[str] = "active", date: Optional[str] = None) -> str:
        """
        Retrieve Alpha Vantage's listing status CSV.

        This endpoint returns a CSV payload (not JSON) and does not require a symbol.

        Parameters
        ----------
        state : {'active', 'delisted', None}, optional
            Filter to active or delisted listings. When None, Alpha Vantage returns the
            provider default (often active).
        date : str, optional
            Optional listing status snapshot date in YYYY-MM-DD format (premium keys may
            support historical snapshots).

        Returns
        -------
        str
            Raw CSV text.
        """
        params: Dict[str, Any] = {"function": "LISTING_STATUS"}
        if state:
            params["state"] = state
        if date:
            params["date"] = date
        return self._request(params, raw=True)  # type: ignore[return-value]

    def get_daily_time_series(
        self,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve daily open/high/low/close/volume data for a symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol to query (e.g. ``"AAPL"``).
        outputsize : {'compact', 'full'}, optional
            ``'compact'`` returns only the latest 100 data points,
            whereas ``'full'`` returns the entire available history【34†L395-L402】.
        adjusted : bool, optional
            If ``True``, return adjusted closing prices (dividend and
            split adjusted) using the ``TIME_SERIES_DAILY_ADJUSTED``
            function.
        datatype : {'json', 'csv'}, optional
            Format of the response.  When ``'csv'``, the raw CSV text
            is returned.  Otherwise a JSON object is returned.

        Returns
        -------
        dict or str
            Parsed JSON response or raw CSV text.
        """
        function = "TIME_SERIES_DAILY_ADJUSTED" if adjusted else "TIME_SERIES_DAILY"
        params = {"outputsize": outputsize}
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_weekly_time_series(
        self, symbol: str, adjusted: bool = False, datatype: str = "json"
    ) -> Union[Dict[str, Any], str]:
        """Retrieve weekly OHLCV data for a symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol.
        adjusted : bool, optional
            Whether to request the adjusted time series.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function = "TIME_SERIES_WEEKLY_ADJUSTED" if adjusted else "TIME_SERIES_WEEKLY"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_monthly_time_series(
        self, symbol: str, adjusted: bool = False, datatype: str = "json"
    ) -> Union[Dict[str, Any], str]:
        """Retrieve monthly OHLCV data for a symbol."""
        function = "TIME_SERIES_MONTHLY_ADJUSTED" if adjusted else "TIME_SERIES_MONTHLY"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_intraday_time_series(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: str = "compact",
        month: Optional[str] = None,
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve intraday price series for a symbol.

        Alpha Vantage supports various intervals (e.g. 1min, 5min,
        15min, 30min, 60min).  The ``outputsize`` parameter for
        intraday data defaults to the last 30 days; specifying
        ``month`` allows retrieving a particular historical month up to
        20 years back for premium plans【23†L193-L202】.

        Parameters
        ----------
        symbol : str
            Ticker symbol.
        interval : str, optional
            Time step between points ("1min", "5min", etc.).
        outputsize : {'compact', 'full'}, optional
            Data volume to return.  ``'full'`` is only available for
            premium keys for intraday data.
        month : str, optional
            A specific month in ``YYYY-MM`` format to fetch historical
            data.  Requires premium subscription.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function = "TIME_SERIES_INTRADAY"
        params: Dict[str, Any] = {"interval": interval, "outputsize": outputsize}
        if month:
            params["month"] = month
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_fx_time_series(
        self,
        from_symbol: str,
        to_symbol: str,
        interval: str = "daily",
        outputsize: str = "compact",
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve FX exchange rate series between two currencies.

        Valid intervals are ``'daily'``, ``'weekly'`` and ``'monthly'``;
        intraday FX series are premium only and are not exposed here.

        Parameters
        ----------
        from_symbol : str
            Base currency (e.g. ``"EUR"``).
        to_symbol : str
            Quote currency (e.g. ``"USD"``).
        interval : {'daily', 'weekly', 'monthly'}, optional
            Frequency of the data.
        outputsize : {'compact', 'full'}, optional
            Number of points to return.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function_map = {
            "daily": "FX_DAILY",
            "weekly": "FX_WEEKLY",
            "monthly": "FX_MONTHLY",
        }
        function = function_map.get(interval.lower()) or "FX_DAILY"
        params: Dict[str, Any] = {
            "from_symbol": from_symbol,
            "to_symbol": to_symbol,
            "outputsize": outputsize,
        }
        # For FX functions, the "symbol" parameter is not used
        if datatype.lower() == "csv":
            return self.fetch_csv(function, None, **params)
        return self.fetch(function, None, **params)

    def get_crypto_time_series(
        self,
        symbol: str,
        market: str = "USD",
        interval: str = "daily",
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve cryptocurrency price series for a given market.

        Supported intervals are ``'daily'``, ``'weekly'`` and ``'monthly'``.

        Parameters
        ----------
        symbol : str
            Cryptocurrency ticker (e.g. ``"BTC"``).
        market : str, optional
            Quoted currency (e.g. ``"USD"``, ``"EUR"``).
        interval : {'daily', 'weekly', 'monthly'}, optional
            Frequency of the data.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function_map = {
            "daily": "DIGITAL_CURRENCY_DAILY",
            "weekly": "DIGITAL_CURRENCY_WEEKLY",
            "monthly": "DIGITAL_CURRENCY_MONTHLY",
        }
        function = function_map.get(interval.lower()) or "DIGITAL_CURRENCY_DAILY"
        params: Dict[str, Any] = {"market": market}
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_technical_indicator(
        self,
        indicator: str,
        symbol: str,
        interval: str,
        series_type: str = "close",
        time_period: Optional[int] = None,
        datatype: str = "json",
        **kwargs: Any,
    ) -> Union[Dict[str, Any], str]:
        """Retrieve a technical indicator series.

        Alpha Vantage supports dozens of technical analysis functions
        (e.g. SMA, EMA, RSI, MACD).  The generic API uses the
        indicator name as the function parameter.  In addition to the
        standard arguments documented here, many indicators accept
        extra parameters (e.g. ``series_type``, ``time_period``,
        ``slow_period``, ``fast_period``).  Any additional
        keyword arguments passed to this method will be forwarded
        directly to the API.

        Parameters
        ----------
        indicator : str
            The indicator function name (e.g. ``"SMA"``, ``"EMA"``).
        symbol : str
            The symbol to calculate the indicator for.
        interval : str
            The time frame ("1min", "5min", "daily", etc.).
        series_type : {'open', 'high', 'low', 'close'}, optional
            Which price field to use.  Not all indicators require this.
        time_period : int, optional
            The number of points used in the lookback window.  Not
            applicable for all indicators.
        datatype : {'json', 'csv'}, optional
            Response format.
        **kwargs : dict
            Extra query parameters accepted by the chosen indicator.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        params: Dict[str, Any] = {"interval": interval, "series_type": series_type}
        if time_period is not None:
            params["time_period"] = time_period
        # Merge additional parameters
        params.update(kwargs)
        if datatype.lower() == "csv":
            return self.fetch_csv(indicator, symbol, **params)
        return self.fetch(indicator, symbol, **params)

    def get_company_overview(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve a company overview (metadata and summary metrics)."""
        function = "OVERVIEW"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_income_statement(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve the income statement for a company."""
        function = "INCOME_STATEMENT"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_balance_sheet(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve the balance sheet for a company."""
        function = "BALANCE_SHEET"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_cash_flow(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve the cash flow statement for a company."""
        function = "CASH_FLOW"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_earnings(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve historical earnings (EPS) for a company."""
        function = "EARNINGS"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_earnings_calendar(
        self,
        symbol: Optional[str] = None,
        *,
        horizon: str = "12month",
    ) -> str:
        """Retrieve upcoming earnings-calendar rows as CSV."""
        function = "EARNINGS_CALENDAR"
        return self.fetch_csv(function, symbol, horizon=horizon)

    # ------------------------------------------------------------------
    # Parsing helpers (delegated to utils)
    # ------------------------------------------------------------------
    @staticmethod
    def parse_time_series(response_json: Dict[str, Any]) -> Any:
        """Convert a time series JSON into a pandas DataFrame.

        This is a thin wrapper around :func:`utils.parse_time_series` for
        convenience.  See that function for details.
        """
        return parse_time_series(response_json)

    @staticmethod
    def parse_financial_reports(response_json: Dict[str, Any], report_type: str = "annualReports") -> Any:
        """Convert a financial statement JSON into a pandas DataFrame.

        This wraps :func:`utils.parse_financial_reports`.
        """
        return parse_financial_reports(response_json, report_type=report_type)
