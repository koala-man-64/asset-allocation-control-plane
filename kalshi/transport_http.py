from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from kalshi.config import KalshiEnvironmentConfig
from kalshi.errors import (
    KalshiAmbiguousWriteError,
    KalshiApiError,
    KalshiAuthError,
    KalshiConflictError,
    KalshiInvalidResponseError,
    KalshiNetworkError,
    KalshiNotConfiguredError,
    KalshiNotFoundError,
    KalshiPermissionError,
    KalshiRateLimitError,
    KalshiServerError,
    KalshiTimeoutError,
    KalshiValidationError,
)
from kalshi.signing import build_auth_headers, load_private_key

logger = logging.getLogger(__name__)

_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}


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
    request_id = (
        response.headers.get("X-Request-ID")
        or response.headers.get("x-request-id")
        or response.headers.get("CF-Ray")
    )
    if request_id:
        payload["request_id"] = request_id
    if body:
        payload["body"] = body
    return payload


def _error_from_response(response: httpx.Response) -> KalshiApiError:
    payload = _error_payload_from_response(response)
    message = _extract_message(
        payload.get("body"),
        fallback=f"Kalshi returned HTTP {response.status_code} for {response.request.method} {response.request.url.path}.",
    )
    status_code = response.status_code
    if status_code in {400, 422}:
        return KalshiValidationError(message, payload=payload)
    if status_code == 401:
        return KalshiAuthError(message, payload=payload)
    if status_code == 403:
        return KalshiPermissionError(message, payload=payload)
    if status_code == 404:
        return KalshiNotFoundError(message, payload=payload)
    if status_code == 409:
        return KalshiConflictError(message, payload=payload)
    if status_code == 429:
        return KalshiRateLimitError(message, payload=payload)
    if status_code >= 500:
        return KalshiServerError(message, status_code=status_code, payload=payload)
    return KalshiApiError(message, status_code=status_code, payload=payload)


def _request_error_payload(exc: httpx.RequestError, *, method: str, endpoint: str) -> dict[str, Any]:
    return {
        "method": method,
        "endpoint": endpoint,
        "error": str(exc),
    }


class KalshiHttpTransport:
    def __init__(self, config: KalshiEnvironmentConfig) -> None:
        self._config = config
        self._base_url = config.get_base_url()
        private_key_pem = config.get_private_key_pem()
        self._private_key = load_private_key(private_key_pem) if private_key_pem else None
        self._client = httpx.Client(base_url=self._base_url, timeout=config.http.timeout_s, trust_env=False)
        self._last_request_id: str | None = None

    @property
    def last_request_id(self) -> str | None:
        return self._last_request_id

    def close(self) -> None:
        self._client.close()

    def _auth_headers(self, *, method: str, endpoint: str, has_json_body: bool) -> dict[str, str]:
        api_key_id = self._config.get_api_key_id()
        if not api_key_id or self._private_key is None:
            raise KalshiNotConfiguredError(f"Kalshi {self._config.environment} credentials are not configured.")
        return build_auth_headers(
            self._private_key,
            api_key_id,
            method=method,
            base_url=self._base_url,
            endpoint=endpoint,
            content_type="application/json" if has_json_body else None,
        )

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        authenticated: bool,
        write_operation: bool,
    ) -> Any:
        retries = max(0, int(self._config.http.read_retry_attempts))
        backoff = max(0.0, float(self._config.http.read_retry_base_s))
        headers = self._auth_headers(method=method, endpoint=endpoint, has_json_body=json_data is not None) if authenticated else None

        for attempt in range(retries + 1):
            try:
                response = self._client.request(method, endpoint, params=params, json=json_data, headers=headers)
                self._last_request_id = (
                    response.headers.get("X-Request-ID")
                    or response.headers.get("x-request-id")
                    or response.headers.get("CF-Ray")
                )
                response.raise_for_status()
                if response.status_code == 204:
                    return {}
                try:
                    return response.json()
                except ValueError as exc:
                    raise KalshiInvalidResponseError(
                        f"Kalshi returned a non-JSON success response for {method} {endpoint}.",
                        payload=_error_payload_from_response(response),
                    ) from exc
            except httpx.HTTPStatusError as exc:
                self._last_request_id = (
                    exc.response.headers.get("X-Request-ID")
                    or exc.response.headers.get("x-request-id")
                    or exc.response.headers.get("CF-Ray")
                )
                status_code = exc.response.status_code
                if not write_operation and status_code in _RETRYABLE_STATUSES and attempt < retries:
                    logger.warning(
                        "Retrying Kalshi read %s %s after status=%s request_id=%s attempt=%s/%s",
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
                    raise KalshiAmbiguousWriteError(
                        f"Timed out while performing Kalshi write {method} {endpoint}; submission state is unknown.",
                        payload=payload,
                    ) from exc
                if attempt < retries:
                    logger.warning(
                        "Retrying Kalshi read %s %s after timeout attempt=%s/%s",
                        method,
                        endpoint,
                        attempt + 1,
                        retries,
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise KalshiTimeoutError(f"Timed out while calling Kalshi {method} {endpoint}.", payload=payload) from exc
            except httpx.RequestError as exc:
                payload = _request_error_payload(exc, method=method, endpoint=endpoint)
                if write_operation:
                    raise KalshiAmbiguousWriteError(
                        f"Network failure while performing Kalshi write {method} {endpoint}; submission state is unknown.",
                        payload=payload,
                    ) from exc
                if attempt < retries:
                    logger.warning(
                        "Retrying Kalshi read %s %s after network error attempt=%s/%s error=%s",
                        method,
                        endpoint,
                        attempt + 1,
                        retries,
                        exc,
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise KalshiNetworkError(
                    f"Network failure while calling Kalshi {method} {endpoint}.",
                    payload=payload,
                ) from exc

        raise RuntimeError("Unreachable code")

    def get(self, endpoint: str, params: dict[str, Any] | None = None, *, authenticated: bool) -> Any:
        return self._request("GET", endpoint, params=params, authenticated=authenticated, write_operation=False)

    def post(self, endpoint: str, json_data: dict[str, Any] | None = None, *, authenticated: bool) -> Any:
        return self._request(
            "POST",
            endpoint,
            json_data=json_data,
            authenticated=authenticated,
            write_operation=True,
        )

    def delete(self, endpoint: str, params: dict[str, Any] | None = None, *, authenticated: bool) -> Any:
        return self._request(
            "DELETE",
            endpoint,
            params=params,
            authenticated=authenticated,
            write_operation=True,
        )
