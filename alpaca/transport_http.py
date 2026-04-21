from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from alpaca.config import AlpacaConfig, AlpacaEnvironmentConfig
from alpaca.errors import (
    AlpacaAmbiguousWriteError,
    AlpacaApiError,
    AlpacaAuthError,
    AlpacaConflictError,
    AlpacaInvalidResponseError,
    AlpacaNetworkError,
    AlpacaNotFoundError,
    AlpacaPermissionError,
    AlpacaRateLimitError,
    AlpacaServerError,
    AlpacaTimeoutError,
    AlpacaValidationError,
)

logger = logging.getLogger(__name__)

_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}
_ConfigLike = AlpacaConfig | AlpacaEnvironmentConfig


def _extract_message(payload: Any, *, fallback: str) -> str:
    if isinstance(payload, dict):
        for key in ("message", "detail", "error", "code"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return fallback


def _error_payload_from_response(response: httpx.Response) -> dict[str, Any]:
    body: Any
    try:
        body = response.json()
    except ValueError:
        body = (response.text or "").strip()[:500]
    payload: dict[str, Any] = {"status_code": response.status_code}
    request_id = response.headers.get("X-Request-ID")
    if request_id:
        payload["request_id"] = request_id
    if body:
        payload["body"] = body
    return payload


def _error_from_response(response: httpx.Response) -> AlpacaApiError:
    payload = _error_payload_from_response(response)
    message = _extract_message(
        payload.get("body"),
        fallback=f"Alpaca returned HTTP {response.status_code} for {response.request.method} {response.request.url.path}.",
    )
    status_code = response.status_code
    if status_code == 400 or status_code == 422:
        return AlpacaValidationError(message, payload=payload)
    if status_code == 401:
        return AlpacaAuthError(message, payload=payload)
    if status_code == 403:
        return AlpacaPermissionError(message, payload=payload)
    if status_code == 404:
        return AlpacaNotFoundError(message, payload=payload)
    if status_code == 409:
        return AlpacaConflictError(message, payload=payload)
    if status_code == 429:
        return AlpacaRateLimitError(message, payload=payload)
    if status_code >= 500:
        return AlpacaServerError(message, status_code=status_code, payload=payload)
    return AlpacaApiError(message, status_code=status_code, payload=payload)


def _request_error_payload(exc: httpx.RequestError, *, method: str, endpoint: str) -> dict[str, Any]:
    return {
        "method": method,
        "endpoint": endpoint,
        "error": str(exc),
    }


class AlpacaHttpTransport:
    def __init__(self, config: _ConfigLike) -> None:
        self._config = config
        self._base_url = config.get_trading_base_url()
        self._headers = {
            "APCA-API-KEY-ID": config.get_api_key(),
            "APCA-API-SECRET-KEY": config.get_api_secret(),
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(
            base_url=self._base_url,
            headers=self._headers,
            timeout=config.http.timeout_s,
            trust_env=False,
        )
        self._last_request_id: str | None = None

    @property
    def last_request_id(self) -> str | None:
        return self._last_request_id

    def close(self) -> None:
        self._client.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        write_operation: bool = False,
    ) -> Any:
        retries = max(0, int(self._config.http.max_retries))
        backoff = max(0.0, float(self._config.http.backoff_base_s))

        for attempt in range(retries + 1):
            try:
                response = self._client.request(method, endpoint, params=params, json=json_data)
                self._last_request_id = response.headers.get("X-Request-ID")
                response.raise_for_status()
                if response.status_code == 204:
                    return {}
                try:
                    return response.json()
                except ValueError as exc:
                    raise AlpacaInvalidResponseError(
                        f"Alpaca returned a non-JSON success response for {method} {endpoint}.",
                        payload=_error_payload_from_response(response),
                    ) from exc
            except httpx.HTTPStatusError as exc:
                self._last_request_id = exc.response.headers.get("X-Request-ID")
                status_code = exc.response.status_code
                if not write_operation and status_code in _RETRYABLE_STATUSES and attempt < retries:
                    logger.warning(
                        "Retrying Alpaca read %s %s after status=%s request_id=%s attempt=%s/%s",
                        method,
                        endpoint,
                        status_code,
                        self._last_request_id or "n/a",
                        attempt + 1,
                        retries,
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise _error_from_response(exc.response) from exc
            except httpx.TimeoutException as exc:
                payload = _request_error_payload(exc, method=method, endpoint=endpoint)
                if write_operation:
                    raise AlpacaAmbiguousWriteError(
                        f"Timed out while performing Alpaca write {method} {endpoint}; submission state is unknown.",
                        payload=payload,
                    ) from exc
                if attempt < retries:
                    logger.warning(
                        "Retrying Alpaca read %s %s after timeout attempt=%s/%s",
                        method,
                        endpoint,
                        attempt + 1,
                        retries,
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise AlpacaTimeoutError(
                    f"Timed out while calling Alpaca {method} {endpoint}.",
                    payload=payload,
                ) from exc
            except httpx.RequestError as exc:
                payload = _request_error_payload(exc, method=method, endpoint=endpoint)
                if write_operation:
                    raise AlpacaAmbiguousWriteError(
                        f"Network failure while performing Alpaca write {method} {endpoint}; submission state is unknown.",
                        payload=payload,
                    ) from exc
                if attempt < retries:
                    logger.warning(
                        "Retrying Alpaca read %s %s after network error attempt=%s/%s error=%s",
                        method,
                        endpoint,
                        attempt + 1,
                        retries,
                        exc,
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise AlpacaNetworkError(
                    f"Network failure while calling Alpaca {method} {endpoint}.",
                    payload=payload,
                ) from exc

        raise RuntimeError("Unreachable code")

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, json_data: dict[str, Any] | None = None) -> Any:
        return self._request("POST", endpoint, json_data=json_data, write_operation=True)

    def delete(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("DELETE", endpoint, params=params, write_operation=True)

    def put(self, endpoint: str, json_data: dict[str, Any] | None = None) -> Any:
        return self._request("PUT", endpoint, json_data=json_data, write_operation=True)

    def patch(self, endpoint: str, json_data: dict[str, Any] | None = None) -> Any:
        return self._request("PATCH", endpoint, json_data=json_data, write_operation=True)
