from __future__ import annotations

import collections
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional

import httpx

from quiver_provider.config import QuiverConfig
from quiver_provider.errors import (
    QuiverAuthError,
    QuiverEntitlementError,
    QuiverInvalidRequestError,
    QuiverNotConfiguredError,
    QuiverNotFoundError,
    QuiverProtocolError,
    QuiverRateLimitError,
    QuiverTimeoutError,
    QuiverUnavailableError,
)

logger = logging.getLogger(__name__)
_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class _PerMinuteRateLimiter:
    def __init__(self, rate_limit_per_min: int) -> None:
        self._rate_limit = max(1, int(rate_limit_per_min))
        self._timestamps: collections.deque[float] = collections.deque()
        self._condition = threading.Condition()

    def acquire(self) -> None:
        with self._condition:
            while True:
                now = time.monotonic()
                while self._timestamps and (now - self._timestamps[0]) >= 60.0:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._rate_limit:
                    self._timestamps.append(now)
                    return
                sleep_seconds = max(0.01, 60.0 - (now - self._timestamps[0]))
                self._condition.wait(timeout=sleep_seconds)


@dataclass(frozen=True)
class QuiverHTTPResponse:
    status_code: int
    url: str
    payload: Any


class QuiverClient:
    def __init__(
        self,
        config: QuiverConfig,
        *,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if not config.api_key:
            raise QuiverNotConfiguredError("QUIVER_API_KEY is not configured.")

        self.config = config
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(
            timeout=httpx.Timeout(config.timeout_seconds),
            base_url=str(config.base_url).rstrip("/"),
            headers={"Authorization": f"Bearer {config.api_key}"},
            trust_env=False,
        )
        self._rate_limiter = _PerMinuteRateLimiter(config.rate_limit_per_min)
        self._concurrency_gate = threading.BoundedSemaphore(max(1, config.max_concurrency))

    def close(self) -> None:
        if self._owns_http:
            try:
                self._http.close()
            except Exception:
                pass

    def __enter__(self) -> "QuiverClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _redact(self, value: object) -> str:
        text = str(value or "")
        api_key = str(self.config.api_key or "")
        if api_key:
            text = text.replace(api_key, "[REDACTED]")
        return text

    def _sanitize_params(self, params: Optional[dict[str, Any]]) -> dict[str, Any]:
        safe: dict[str, Any] = {}
        for key, value in dict(params or {}).items():
            lowered = str(key).lower()
            if "key" in lowered or "token" in lowered or lowered == "authorization":
                continue
            safe[key] = value
        return safe

    def _request_url(self, path_or_url: str) -> str:
        text = str(path_or_url or "").strip()
        if text.startswith("http://") or text.startswith("https://"):
            return text
        return f"{self.config.base_url.rstrip('/')}/{text.lstrip('/')}"

    def _request_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.config.api_key}"}

    def _extract_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            text = (response.text or "").strip()
            return text or response.reason_phrase

        if isinstance(payload, dict):
            for key in ("detail", "message", "error", "errors"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return json.dumps(payload, ensure_ascii=False)
        if isinstance(payload, list):
            return f"Unexpected list payload ({len(payload)} items)."
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return response.reason_phrase

    def _retry_after_seconds(self, response: httpx.Response) -> Optional[float]:
        raw = _strip_or_none(response.headers.get("Retry-After"))
        if not raw:
            return None
        try:
            return max(0.0, float(raw))
        except Exception:
            pass
        try:
            parsed = parsedate_to_datetime(raw)
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delta = (parsed - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)

    def _backoff_seconds(self, *, attempt: int, response: Optional[httpx.Response] = None) -> float:
        retry_after = self._retry_after_seconds(response) if response is not None else None
        if retry_after is not None:
            return retry_after
        base = max(0.0, float(self.config.backoff_base_seconds))
        return base * (2 ** max(0, attempt - 1))

    def _log_retry(
        self,
        *,
        path_or_url: str,
        attempt: int,
        response: Optional[httpx.Response],
        params: dict[str, Any],
    ) -> None:
        status = getattr(response, "status_code", None)
        logger.warning(
            "Retrying Quiver request path=%s attempt=%s status=%s params=%s",
            self._redact(path_or_url),
            attempt,
            status,
            self._sanitize_params(params),
        )

    def _raise_http_error(self, *, response: httpx.Response, path_or_url: str) -> None:
        detail = self._extract_detail(response)
        payload = {
            "path": path_or_url,
            "status_code": int(response.status_code),
            "detail": detail,
        }
        lowered = detail.lower()

        if response.status_code == 400:
            raise QuiverInvalidRequestError(
                detail or "Invalid Quiver request.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )
        if response.status_code == 401:
            raise QuiverAuthError(
                detail or "Quiver authentication failed.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )
        if response.status_code == 403:
            if any(token in lowered for token in ("premium", "entitle", "subscription", "tier")):
                raise QuiverEntitlementError(
                    detail or "Quiver entitlement denied.",
                    status_code=response.status_code,
                    detail=detail,
                    payload=payload,
                )
            raise QuiverAuthError(
                detail or "Quiver authorization failed.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )
        if response.status_code == 404:
            raise QuiverNotFoundError(
                detail or "Quiver resource not found.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )
        if response.status_code == 429:
            raise QuiverRateLimitError(
                detail or "Quiver rate limit exceeded.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )
        raise QuiverUnavailableError(
            detail or f"Quiver request failed with status={response.status_code}.",
            status_code=response.status_code,
            detail=detail,
            payload=payload,
        )

    def get_json(self, path_or_url: str, *, params: Optional[dict[str, Any]] = None) -> Any:
        request_params = {key: value for key, value in dict(params or {}).items() if value is not None}
        attempts = max(0, int(self.config.max_retries)) + 1

        for attempt in range(1, attempts + 1):
            self._rate_limiter.acquire()
            with self._concurrency_gate:
                try:
                    response = self._http.get(
                        self._request_url(path_or_url),
                        params=request_params,
                        headers=self._request_headers(),
                    )
                except httpx.TimeoutException as exc:
                    if attempt < attempts:
                        self._log_retry(path_or_url=path_or_url, attempt=attempt, response=None, params=request_params)
                        time.sleep(self._backoff_seconds(attempt=attempt))
                        continue
                    raise QuiverTimeoutError(
                        f"Quiver timeout calling {path_or_url}.",
                        payload={"path": path_or_url},
                    ) from exc
                except Exception as exc:
                    if attempt < attempts:
                        self._log_retry(path_or_url=path_or_url, attempt=attempt, response=None, params=request_params)
                        time.sleep(self._backoff_seconds(attempt=attempt))
                        continue
                    raise QuiverUnavailableError(
                        f"Quiver call failed: {type(exc).__name__}: {exc}",
                        payload={"path": path_or_url},
                    ) from exc

            if response.status_code < 400:
                try:
                    return response.json()
                except Exception as exc:
                    raise QuiverProtocolError(
                        f"Quiver returned non-JSON content for {path_or_url}.",
                        status_code=response.status_code,
                        detail=(response.text or "").strip()[:240] or None,
                        payload={"path": path_or_url},
                    ) from exc

            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < attempts:
                self._log_retry(path_or_url=path_or_url, attempt=attempt, response=response, params=request_params)
                time.sleep(self._backoff_seconds(attempt=attempt, response=response))
                continue

            self._raise_http_error(response=response, path_or_url=path_or_url)

        raise QuiverUnavailableError(
            f"Quiver request exhausted retries for {path_or_url}.",
            payload={"path": path_or_url},
        )
