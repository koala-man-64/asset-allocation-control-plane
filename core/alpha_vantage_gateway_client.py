from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import httpx

from asset_allocation_runtime_common.api_gateway_auth import build_access_token_provider
logger = logging.getLogger(__name__)
_MIN_API_GATEWAY_TIMEOUT_SECONDS = 600.0
_DEFAULT_API_WARMUP_ENABLED = True
_DEFAULT_API_WARMUP_MAX_ATTEMPTS = 3
_DEFAULT_API_WARMUP_BASE_DELAY_SECONDS = 1.0
_DEFAULT_API_WARMUP_MAX_DELAY_SECONDS = 8.0
_DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS = 5.0
_DEFAULT_API_READINESS_ENABLED = True
_DEFAULT_API_READINESS_MAX_ATTEMPTS = 6
_DEFAULT_API_READINESS_SLEEP_SECONDS = 10.0
_DEFAULT_REQUEST_RETRY_ATTEMPTS = 3
_DEFAULT_REQUEST_RETRY_BASE_DELAY_SECONDS = 120.0
_DEFAULT_REQUEST_RETRY_MAX_DELAY_SECONDS = 300.0
_API_WARMUP_PROBE_PATH = "/healthz"
_RETRYABLE_WARMUP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_RETRYABLE_REQUEST_STATUS_CODES = {504}


class AlphaVantageGatewayError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        detail: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail
        self.payload = payload


class AlphaVantageGatewayAuthError(AlphaVantageGatewayError):
    pass


class AlphaVantageGatewayThrottleError(AlphaVantageGatewayError):
    pass


class AlphaVantageGatewayInvalidSymbolError(AlphaVantageGatewayError):
    pass


class AlphaVantageGatewayUnavailableError(AlphaVantageGatewayError):
    pass


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
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return bool(default)
    lowered = raw.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


@dataclass(frozen=True)
class AlphaVantageGatewayClientConfig:
    base_url: str
    api_scope: Optional[str]
    timeout_seconds: float
    warmup_enabled: bool = _DEFAULT_API_WARMUP_ENABLED
    warmup_max_attempts: int = _DEFAULT_API_WARMUP_MAX_ATTEMPTS
    warmup_base_delay_seconds: float = _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS
    warmup_max_delay_seconds: float = _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS
    warmup_probe_timeout_seconds: float = _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS
    readiness_enabled: bool = _DEFAULT_API_READINESS_ENABLED
    readiness_max_attempts: int = _DEFAULT_API_READINESS_MAX_ATTEMPTS
    readiness_sleep_seconds: float = _DEFAULT_API_READINESS_SLEEP_SECONDS
    request_retry_attempts: int = _DEFAULT_REQUEST_RETRY_ATTEMPTS
    request_retry_base_delay_seconds: float = _DEFAULT_REQUEST_RETRY_BASE_DELAY_SECONDS
    request_retry_max_delay_seconds: float = _DEFAULT_REQUEST_RETRY_MAX_DELAY_SECONDS


class AlphaVantageGatewayClient:
    """
    Minimal sync client for the API-hosted Alpha Vantage gateway.

    ETL jobs should use this instead of calling Alpha Vantage directly.
    """

    def __init__(
        self,
        config: AlphaVantageGatewayClientConfig,
        *,
        http_client: Optional[httpx.Client] = None,
        access_token_provider: Optional[Callable[[], str]] = None,
    ) -> None:
        self.config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(config.timeout_seconds), trust_env=False)
        self._access_token_provider = access_token_provider
        self._warmup_lock = threading.Lock()
        self._warmup_attempted = False
        self._warmup_succeeded = not config.warmup_enabled
        self._readiness_lock = threading.Lock()
        self._readiness_attempted = False
        self._readiness_succeeded = not config.readiness_enabled

    @staticmethod
    def from_env() -> "AlphaVantageGatewayClient":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL"))
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for Alpha Vantage ETL via API gateway.")

        api_scope = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_SCOPE"))
        if not api_scope:
            raise ValueError(
                "ASSET_ALLOCATION_API_SCOPE is required for Alpha Vantage ETL via the Asset Allocation API gateway."
            )

        timeout_seconds = _env_float(
            "ASSET_ALLOCATION_API_TIMEOUT_SECONDS",
            _env_float("ALPHA_VANTAGE_TIMEOUT_SECONDS", _MIN_API_GATEWAY_TIMEOUT_SECONDS),
        )
        if timeout_seconds < _MIN_API_GATEWAY_TIMEOUT_SECONDS:
            logger.warning(
                "ASSET_ALLOCATION_API_TIMEOUT_SECONDS=%s is too low for Alpha Vantage cooldown waits; using %s.",
                timeout_seconds,
                _MIN_API_GATEWAY_TIMEOUT_SECONDS,
            )
            timeout_seconds = _MIN_API_GATEWAY_TIMEOUT_SECONDS

        warmup_max_attempts = max(1, _env_int("ASSET_ALLOCATION_API_WARMUP_ATTEMPTS", _DEFAULT_API_WARMUP_MAX_ATTEMPTS))
        warmup_base_delay_seconds = max(
            0.0,
            _env_float("ASSET_ALLOCATION_API_WARMUP_BASE_SECONDS", _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS),
        )
        warmup_max_delay_seconds = max(
            warmup_base_delay_seconds,
            _env_float("ASSET_ALLOCATION_API_WARMUP_MAX_SECONDS", _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS),
        )
        warmup_probe_timeout_seconds = max(
            0.1,
            _env_float("ASSET_ALLOCATION_API_WARMUP_PROBE_TIMEOUT_SECONDS", _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS),
        )
        readiness_max_attempts = max(
            1,
            _env_int("ASSET_ALLOCATION_API_READINESS_ATTEMPTS", _DEFAULT_API_READINESS_MAX_ATTEMPTS),
        )
        readiness_sleep_seconds = max(
            0.0,
            _env_float("ASSET_ALLOCATION_API_READINESS_SLEEP_SECONDS", _DEFAULT_API_READINESS_SLEEP_SECONDS),
        )
        request_retry_attempts = max(
            1,
            _env_int("ALPHA_VANTAGE_GATEWAY_RETRY_ATTEMPTS", _DEFAULT_REQUEST_RETRY_ATTEMPTS),
        )
        request_retry_base_delay_seconds = max(
            0.0,
            _env_float("ALPHA_VANTAGE_GATEWAY_RETRY_BASE_SECONDS", _DEFAULT_REQUEST_RETRY_BASE_DELAY_SECONDS),
        )
        request_retry_max_delay_seconds = max(
            request_retry_base_delay_seconds,
            _env_float("ALPHA_VANTAGE_GATEWAY_RETRY_MAX_SECONDS", _DEFAULT_REQUEST_RETRY_MAX_DELAY_SECONDS),
        )

        return AlphaVantageGatewayClient(
            AlphaVantageGatewayClientConfig(
                base_url=str(base_url).rstrip("/"),
                api_scope=api_scope,
                timeout_seconds=float(timeout_seconds),
                warmup_enabled=True,
                warmup_max_attempts=warmup_max_attempts,
                warmup_base_delay_seconds=warmup_base_delay_seconds,
                warmup_max_delay_seconds=warmup_max_delay_seconds,
                warmup_probe_timeout_seconds=warmup_probe_timeout_seconds,
                readiness_enabled=True,
                readiness_max_attempts=readiness_max_attempts,
                readiness_sleep_seconds=readiness_sleep_seconds,
                request_retry_attempts=request_retry_attempts,
                request_retry_base_delay_seconds=request_retry_base_delay_seconds,
                request_retry_max_delay_seconds=request_retry_max_delay_seconds,
            )
        )

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> "AlphaVantageGatewayClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._access_token_provider is None:
            if not self.config.api_scope:
                raise AlphaVantageGatewayAuthError(
                    "ASSET_ALLOCATION_API_SCOPE is required for bearer-token access to the Asset Allocation API gateway."
                )
            self._access_token_provider = build_access_token_provider(self.config.api_scope)
        try:
            headers["Authorization"] = f"Bearer {self._access_token_provider()}"
        except Exception as exc:
            raise AlphaVantageGatewayAuthError(
                "Failed to acquire bearer token for the Asset Allocation API gateway."
            ) from exc
        caller_job = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_NAME"))
        caller_execution = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME"))
        if caller_job:
            headers["X-Caller-Job"] = str(caller_job)
        if caller_execution:
            headers["X-Caller-Execution"] = str(caller_execution)
        return headers

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
            return json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return response.reason_phrase

    def _warm_up_gateway(self) -> bool:
        if not self.config.warmup_enabled:
            return True
        if self._warmup_attempted:
            return self._warmup_succeeded

        with self._warmup_lock:
            if not self.config.warmup_enabled:
                self._warmup_succeeded = True
                return True
            if self._warmup_attempted:
                return self._warmup_succeeded
            warmup_succeeded = False
            try:
                delay_seconds = max(0.0, float(self.config.warmup_base_delay_seconds))
                max_delay_seconds = max(delay_seconds, float(self.config.warmup_max_delay_seconds))
                attempts = max(1, int(self.config.warmup_max_attempts))
                probe_timeout = min(float(self.config.timeout_seconds), float(self.config.warmup_probe_timeout_seconds))
                warmup_timeout = httpx.Timeout(probe_timeout)
                probe_url = f"{self.config.base_url}{_API_WARMUP_PROBE_PATH}"

                for attempt in range(1, attempts + 1):
                    should_retry = attempt < attempts
                    try:
                        resp = self._http.get(probe_url, headers=self._build_headers(), timeout=warmup_timeout)
                        if resp.status_code < 400:
                            warmup_succeeded = True
                            if attempt > 1:
                                logger.info(
                                    "Alpha Vantage gateway warm-up recovered after %s attempts (url=%s).",
                                    attempt,
                                    probe_url,
                                )
                            return True

                        if resp.status_code not in _RETRYABLE_WARMUP_STATUS_CODES or not should_retry:
                            logger.warning(
                                "Alpha Vantage gateway warm-up probe failed (status=%s, attempt=%s/%s, url=%s).",
                                resp.status_code,
                                attempt,
                                attempts,
                                probe_url,
                            )
                            return False
                        logger.info(
                            "Alpha Vantage gateway warm-up probe retrying after status=%s (attempt=%s/%s, sleep=%.1fs).",
                            resp.status_code,
                            attempt,
                            attempts,
                            delay_seconds,
                        )
                    except httpx.TimeoutException as exc:
                        if not should_retry:
                            logger.warning(
                                "Alpha Vantage gateway warm-up probe timed out after %s attempts (url=%s): %s",
                                attempts,
                                probe_url,
                                exc,
                            )
                            return False
                        logger.info(
                            "Alpha Vantage gateway warm-up timeout (attempt=%s/%s, sleep=%.1fs): %s",
                            attempt,
                            attempts,
                            delay_seconds,
                            exc,
                        )
                    except Exception as exc:
                        if not should_retry:
                            logger.warning(
                                "Alpha Vantage gateway warm-up probe failed after %s attempts (url=%s): %s: %s",
                                attempts,
                                probe_url,
                                type(exc).__name__,
                                exc,
                            )
                            return False
                        logger.info(
                            "Alpha Vantage gateway warm-up transient failure (attempt=%s/%s, sleep=%.1fs): %s: %s",
                            attempt,
                            attempts,
                            delay_seconds,
                            type(exc).__name__,
                            exc,
                        )

                    if delay_seconds > 0.0:
                        time.sleep(delay_seconds)
                    delay_seconds = min(max_delay_seconds, max(delay_seconds * 2.0, 0.1))
                return warmup_succeeded
            finally:
                self._warmup_attempted = True
                self._warmup_succeeded = warmup_succeeded

    def warm_up_gateway(self, *, force: bool = False) -> bool:
        if force:
            with self._warmup_lock:
                self._warmup_attempted = False
                self._warmup_succeeded = not self.config.warmup_enabled
        return self._warm_up_gateway()

    def _ensure_gateway_ready(self) -> bool:
        if not self.config.readiness_enabled:
            return self._warm_up_gateway()
        if self._readiness_attempted:
            return self._readiness_succeeded

        with self._readiness_lock:
            if self._readiness_attempted:
                return self._readiness_succeeded

            attempts = max(1, int(self.config.readiness_max_attempts))
            pause = max(0.0, float(self.config.readiness_sleep_seconds))
            ready = False

            for attempt in range(1, attempts + 1):
                ready = self.warm_up_gateway(force=attempt > 1)
                if ready:
                    if attempt > 1:
                        logger.info(
                            "Alpha Vantage gateway readiness recovered after %s attempts (url=%s).",
                            attempt,
                            f"{self.config.base_url}{_API_WARMUP_PROBE_PATH}",
                        )
                    break

                if attempt >= attempts:
                    logger.warning(
                        "Alpha Vantage gateway readiness failed after %s attempts (url=%s).",
                        attempts,
                        f"{self.config.base_url}{_API_WARMUP_PROBE_PATH}",
                    )
                    break

                logger.info(
                    "Alpha Vantage gateway readiness retrying (attempt=%s/%s, sleep=%.1fs).",
                    attempt,
                    attempts,
                    pause,
                )
                if pause > 0.0:
                    time.sleep(pause)

            self._readiness_attempted = True
            self._readiness_succeeded = ready
            return ready

    def _reset_gateway_state(self) -> None:
        with self._warmup_lock:
            self._warmup_attempted = False
            self._warmup_succeeded = not self.config.warmup_enabled
        with self._readiness_lock:
            self._readiness_attempted = False
            self._readiness_succeeded = not self.config.readiness_enabled

    def _retry_request_delay(self, delay_seconds: float) -> float:
        current = max(0.0, float(delay_seconds))
        if current <= 0.0:
            return 0.0
        return min(float(self.config.request_retry_max_delay_seconds), max(current * 2.0, 1.0))

    def _request(self, path: str, *, params: Optional[dict[str, Any]] = None) -> httpx.Response:
        url = f"{self.config.base_url}{path}"
        attempts = max(1, int(self.config.request_retry_attempts))
        delay_seconds = max(0.0, float(self.config.request_retry_base_delay_seconds))

        for attempt in range(1, attempts + 1):
            if not self._ensure_gateway_ready():
                raise AlphaVantageGatewayUnavailableError(
                    "API gateway readiness check failed.",
                    status_code=503,
                    detail="Gateway health probe did not become ready.",
                    payload={"path": path, "probe_path": _API_WARMUP_PROBE_PATH},
                )
            try:
                resp = self._http.get(url, params=params or {}, headers=self._build_headers())
            except httpx.TimeoutException as exc:
                if attempt < attempts:
                    logger.warning(
                        "Alpha Vantage gateway request timed out (path=%s, attempt=%s/%s, sleep=%.1fs).",
                        path,
                        attempt,
                        attempts,
                        delay_seconds,
                    )
                    if delay_seconds > 0.0:
                        time.sleep(delay_seconds)
                    delay_seconds = self._retry_request_delay(delay_seconds)
                    self._reset_gateway_state()
                    continue
                raise AlphaVantageGatewayError(f"API gateway timeout calling {path}", payload={"path": path}) from exc
            except Exception as exc:
                raise AlphaVantageGatewayError(
                    f"API gateway call failed: {type(exc).__name__}: {exc}", payload={"path": path}
                ) from exc

            if resp.status_code < 400:
                return resp

            detail = self._extract_detail(resp)
            payload = {"path": path, "status_code": int(resp.status_code), "detail": detail}
            if resp.status_code in _RETRYABLE_REQUEST_STATUS_CODES and attempt < attempts:
                logger.warning(
                    "Alpha Vantage gateway request retrying after status=%s (path=%s, attempt=%s/%s, sleep=%.1fs, detail=%s).",
                    resp.status_code,
                    path,
                    attempt,
                    attempts,
                    delay_seconds,
                    detail[:220],
                )
                if delay_seconds > 0.0:
                    time.sleep(delay_seconds)
                delay_seconds = self._retry_request_delay(delay_seconds)
                self._reset_gateway_state()
                continue

            if resp.status_code in {401, 403}:
                raise AlphaVantageGatewayAuthError(
                    "API gateway auth failed.", status_code=resp.status_code, detail=detail, payload=payload
                )
            if resp.status_code == 404:
                raise AlphaVantageGatewayInvalidSymbolError(
                    detail or "Symbol not found.", status_code=resp.status_code, detail=detail, payload=payload
                )
            if resp.status_code == 429:
                raise AlphaVantageGatewayThrottleError(
                    detail or "Throttled.", status_code=resp.status_code, detail=detail, payload=payload
                )
            if resp.status_code == 503:
                raise AlphaVantageGatewayUnavailableError(
                    detail or "Gateway unavailable.", status_code=resp.status_code, detail=detail, payload=payload
                )
            raise AlphaVantageGatewayError(
                f"API gateway error (status={resp.status_code}).",
                status_code=resp.status_code,
                detail=detail,
                payload=payload,
            )

        raise AlphaVantageGatewayError(f"API gateway retry budget exhausted calling {path}", payload={"path": path})

    def get_listing_status_csv(self, *, state: str = "active", date: Optional[str] = None) -> str:
        params: dict[str, Any] = {"state": state}
        if date:
            params["date"] = date
        resp = self._request("/api/providers/alpha-vantage/listing-status", params=params)
        return str(resp.text or "")

    def get_daily_time_series_csv(
        self,
        *,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
    ) -> str:
        resp = self._request(
            "/api/providers/alpha-vantage/time-series/daily",
            params={"symbol": symbol, "outputsize": outputsize, "adjusted": "true" if adjusted else "false"},
        )
        return str(resp.text or "")

    def get_earnings(self, *, symbol: str) -> dict[str, Any]:
        resp = self._request("/api/providers/alpha-vantage/earnings", params={"symbol": symbol})
        return resp.json()

    def get_earnings_calendar_csv(
        self,
        *,
        symbol: Optional[str] = None,
        horizon: str = "12month",
    ) -> str:
        params: dict[str, Any] = {"horizon": horizon}
        if symbol:
            params["symbol"] = symbol
        resp = self._request("/api/providers/alpha-vantage/earnings-calendar", params=params)
        return str(resp.text or "")
