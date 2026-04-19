from __future__ import annotations

import json
import time
from typing import Any, Mapping, Optional
from urllib.parse import parse_qs, parse_qsl, quote, urlparse

import requests
from oauthlib.oauth1 import Client as OAuth1Client
from oauthlib.oauth1.rfc5849 import signature, utils
from requests import Response
from requests_oauthlib import OAuth1Session

from etrade_provider.config import ETradeEnvironmentConfig
from etrade_provider.errors import (
    ETradeAmbiguousWriteError,
    ETradeApiError,
    ETradeBrokerAuthError,
    ETradeNotConfiguredError,
    ETradeRateLimitError,
    ETradeValidationError,
)


_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}


class ETradeOAuth1Client(OAuth1Client):
    """
    E*TRADE's OAuth sample signature omits oauth_version and double-encodes the
    oauth_token in the base-string parameter component. Their reference Python
    client follows the same behavior, so we align the signer here while still
    using oauthlib for header rendering and session plumbing.
    """

    def get_oauth_params(self, request):  # type: ignore[override]
        params = super().get_oauth_params(request)
        return [item for item in params if item[0] != "oauth_version"]

    def get_oauth_signature(self, request):  # type: ignore[override]
        params = list(parse_qsl(urlparse(request.uri).query, keep_blank_values=True))
        params.extend(list(request.oauth_params))
        params.sort(key=lambda item: (utils.escape(item[0]), utils.escape(item[1])))

        encoded_pairs: list[str] = []
        for name, value in params:
            encoded_name = utils.escape(name)
            encoded_value = utils.escape(utils.escape(value)) if name == "oauth_token" else utils.escape(value)
            encoded_pairs.append(f"{encoded_name}%3D{encoded_value}")

        base_uri = signature.base_string_uri(request.uri, request.headers.get("Host", None))
        base_string = (
            f"{request.http_method.upper()}&"
            f"{utils.escape(base_uri)}&"
            f"{'%26'.join(encoded_pairs)}"
        )
        return signature.sign_hmac_sha1_with_client(base_string, self)


def _append_json_suffix(path: str) -> str:
    raw = str(path or "").strip()
    if not raw:
        return raw
    if raw.endswith(".json"):
        return raw
    if "?" in raw:
        base, query = raw.split("?", 1)
        return f"{base}.json?{query}"
    return f"{raw}.json"


def _extract_message(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("error", "Error", "message", "Message", "detail", "description"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        messages = payload.get("Messages") or payload.get("MessageList") or payload.get("messages") or payload.get("messageList")
        if isinstance(messages, dict):
            items = messages.get("Message") or messages.get("message")
            if isinstance(items, list):
                descriptions = [
                    str(item.get("description") or item.get("Description") or "").strip()
                    for item in items
                    if isinstance(item, dict)
                ]
                descriptions = [item for item in descriptions if item]
                if descriptions:
                    return "; ".join(descriptions)
    if isinstance(payload, list):
        for item in payload:
            text = _extract_message(item)
            if text:
                return text
    return ""


def _safe_json_loads(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return None


class ETradeClient:
    def __init__(
        self,
        config: ETradeEnvironmentConfig,
        *,
        timeout_seconds: float = 15.0,
        read_retry_attempts: int = 2,
        read_retry_base_delay_seconds: float = 1.0,
    ) -> None:
        self.config = config
        self.timeout_seconds = float(timeout_seconds)
        self.read_retry_attempts = max(1, int(read_retry_attempts))
        self.read_retry_base_delay_seconds = max(0.0, float(read_retry_base_delay_seconds))

    def _ensure_configured(self) -> None:
        if not self.config.is_configured:
            raise ETradeNotConfiguredError(
                f"E*TRADE {self.config.environment} credentials are not configured."
            )

    def _oauth_session(
        self,
        *,
        resource_owner_key: str | None = None,
        resource_owner_secret: str | None = None,
        verifier: str | None = None,
        callback_uri: str | None = None,
    ) -> OAuth1Session:
        self._ensure_configured()
        return OAuth1Session(
            client_key=str(self.config.consumer_key),
            client_secret=str(self.config.consumer_secret),
            resource_owner_key=resource_owner_key,
            resource_owner_secret=resource_owner_secret,
            verifier=verifier,
            callback_uri=callback_uri,
            signature_type="AUTH_HEADER",
            client_class=ETradeOAuth1Client,
        )

    def fetch_request_token(self, *, callback_uri: str | None = None) -> dict[str, Any]:
        session = self._oauth_session(callback_uri=callback_uri or "oob")
        try:
            payload = session.fetch_request_token(self.config.request_token_url, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise ETradeBrokerAuthError("Failed to request an E*TRADE request token.") from exc
        return dict(payload)

    def build_authorize_url(self, *, request_token: str) -> str:
        return (
            f"{self.config.authorize_url.rstrip('/')}"
            f"?key={quote(str(self.config.consumer_key or ''), safe='')}"
            f"&token={quote(str(request_token or ''), safe='')}"
        )

    def fetch_access_token(
        self,
        *,
        request_token: str,
        request_token_secret: str,
        verifier: str,
    ) -> dict[str, Any]:
        session = self._oauth_session(
            resource_owner_key=request_token,
            resource_owner_secret=request_token_secret,
            verifier=verifier,
        )
        try:
            payload = session.fetch_access_token(self.config.access_token_url, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise ETradeBrokerAuthError("Failed to exchange the request token for an E*TRADE access token.") from exc
        return dict(payload)

    def renew_access_token(self, *, access_token: str, access_token_secret: str) -> str:
        response = self._request(
            "GET",
            self.config.renew_access_token_url,
            access_token=access_token,
            access_token_secret=access_token_secret,
            attempts=1,
            write_operation=False,
            expects_json=False,
        )
        return str(response.text or "").strip()

    def revoke_access_token(self, *, access_token: str, access_token_secret: str) -> str:
        response = self._request(
            "GET",
            self.config.revoke_access_token_url,
            access_token=access_token,
            access_token_secret=access_token_secret,
            attempts=1,
            write_operation=False,
            expects_json=False,
        )
        return str(response.text or "").strip()

    def list_accounts(self, *, access_token: str, access_token_secret: str) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/v1/accounts/list",
            access_token=access_token,
            access_token_secret=access_token_secret,
        )

    def get_balance(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        inst_type: str,
        real_time_nav: bool = True,
        account_type: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"instType": inst_type, "realTimeNAV": str(bool(real_time_nav)).lower()}
        if account_type:
            params["accountType"] = account_type
        return self._request_json(
            "GET",
            f"/v1/accounts/{account_key}/balance",
            access_token=access_token,
            access_token_secret=access_token_secret,
            params=params,
        )

    def get_portfolio(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"/v1/accounts/{account_key}/portfolio",
            access_token=access_token,
            access_token_secret=access_token_secret,
            params=params,
        )

    def get_quotes(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        symbols: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"/v1/market/quote/{symbols}",
            access_token=access_token,
            access_token_secret=access_token_secret,
            params=params,
        )

    def list_orders(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"/v1/accounts/{account_key}/orders",
            access_token=access_token,
            access_token_secret=access_token_secret,
            params=params,
        )

    def preview_order(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            f"/v1/accounts/{account_key}/orders/preview",
            access_token=access_token,
            access_token_secret=access_token_secret,
            json_body=payload,
            attempts=1,
            write_operation=False,
        )

    def place_order(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            f"/v1/accounts/{account_key}/orders/place",
            access_token=access_token,
            access_token_secret=access_token_secret,
            json_body=payload,
            attempts=1,
            write_operation=True,
        )

    def cancel_order(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        order_id: int,
    ) -> dict[str, Any]:
        payload = {"CancelOrderRequest": {"orderId": int(order_id)}}
        return self._request_json(
            "PUT",
            f"/v1/accounts/{account_key}/orders/cancel",
            access_token=access_token,
            access_token_secret=access_token_secret,
            json_body=payload,
            attempts=1,
            write_operation=True,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        access_token: str,
        access_token_secret: str,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
        attempts: Optional[int] = None,
        write_operation: bool = False,
    ) -> dict[str, Any]:
        response = self._request(
            method,
            path,
            access_token=access_token,
            access_token_secret=access_token_secret,
            params=params,
            json_body=json_body,
            attempts=attempts or self.read_retry_attempts,
            write_operation=write_operation,
            expects_json=True,
        )
        payload = _safe_json_loads(response.text or "")
        if not isinstance(payload, dict):
            raise ETradeApiError(
                "E*TRADE returned a non-JSON response when JSON was expected.",
                code="invalid_response",
                status_code=response.status_code,
                payload={"path": path},
            )
        return payload

    def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        access_token: str,
        access_token_secret: str,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
        attempts: int,
        write_operation: bool,
        expects_json: bool,
    ) -> Response:
        session = self._oauth_session(
            resource_owner_key=access_token,
            resource_owner_secret=access_token_secret,
        )
        url = str(path_or_url or "").strip()
        if url.startswith("/"):
            url = f"{self.config.api_base_url.rstrip('/')}{_append_json_suffix(url) if expects_json else url}"

        headers = {"Accept": "application/json" if expects_json else "text/plain"}
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        total_attempts = max(1, int(attempts))
        for attempt in range(1, total_attempts + 1):
            try:
                response = session.request(
                    method=method,
                    url=url,
                    params=dict(params or {}),
                    json=dict(json_body or {}) if json_body is not None else None,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if write_operation:
                    raise ETradeAmbiguousWriteError(
                        "E*TRADE write request completed with an unknown submission state. "
                        "Reconcile through the orders endpoint before retrying.",
                        payload={"path": path_or_url, "method": method},
                    ) from exc
                if attempt >= total_attempts:
                    raise ETradeApiError(
                        f"E*TRADE request failed after {total_attempts} attempt(s).",
                        code="network_error",
                        payload={"path": path_or_url, "method": method},
                    ) from exc
                self._sleep_before_retry(attempt)
                continue
            except requests.RequestException as exc:
                raise ETradeApiError(
                    f"E*TRADE request failed: {type(exc).__name__}: {exc}",
                    code="request_error",
                    payload={"path": path_or_url, "method": method},
                ) from exc

            if response.status_code < 400:
                return response

            if not write_operation and response.status_code in _RETRYABLE_STATUSES and attempt < total_attempts:
                self._sleep_before_retry(attempt)
                continue

            self._raise_http_error(response=response, path=path_or_url, method=method)
        raise RuntimeError("Unreachable")

    def _sleep_before_retry(self, attempt: int) -> None:
        delay = max(0.0, self.read_retry_base_delay_seconds) * (2 ** max(0, attempt - 1))
        if delay > 0:
            time.sleep(delay)

    def _raise_http_error(self, *, response: Response, path: str, method: str) -> None:
        raw_text = str(response.text or "").strip()
        payload = _safe_json_loads(raw_text)
        message = _extract_message(payload)
        if not message:
            if raw_text and raw_text.startswith("oauth_"):
                parsed = parse_qs(raw_text, keep_blank_values=True)
                oauth_problem = parsed.get("oauth_problem")
                if oauth_problem:
                    message = str(oauth_problem[0]).strip()
            if not message:
                message = raw_text or response.reason or f"E*TRADE returned HTTP {response.status_code}."

        error_payload: dict[str, Any] = {
            "path": path,
            "method": method,
            "status_code": int(response.status_code),
        }
        if isinstance(payload, dict):
            error_payload["body"] = payload
        elif raw_text:
            error_payload["body"] = raw_text[:500]

        if response.status_code in {401, 403}:
            raise ETradeBrokerAuthError(message, status_code=response.status_code, payload=error_payload)
        if response.status_code == 429:
            raise ETradeRateLimitError(message, status_code=response.status_code, payload=error_payload)
        if response.status_code == 400:
            raise ETradeValidationError(message, status_code=response.status_code, payload=error_payload)
        raise ETradeApiError(message, status_code=response.status_code, payload=error_payload)
