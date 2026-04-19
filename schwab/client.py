from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import parse_qs, unquote, urlencode, urlparse

import httpx

from schwab.config import SchwabConfig
from schwab.errors import (
    SchwabAuthError,
    SchwabError,
    SchwabNotConfiguredError,
    SchwabNotFoundError,
    SchwabRateLimitError,
    SchwabServerError,
)


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class SchwabHTTPResponse:
    """Normalized response container for Schwab HTTP calls."""

    status_code: int
    url: str
    payload: Any
    headers: httpx.Headers


@dataclass(frozen=True)
class SchwabOAuthTokens:
    """Normalized OAuth token response."""

    access_token: str
    refresh_token: str
    id_token: str
    token_type: str
    scope: str
    expires_in: int
    raw: Mapping[str, Any]

    @staticmethod
    def from_payload(payload: Mapping[str, Any]) -> "SchwabOAuthTokens":
        access_token = str(payload.get("access_token") or "")
        if not access_token:
            raise SchwabError("Schwab token response did not include access_token.", payload=dict(payload))
        return SchwabOAuthTokens(
            access_token=access_token,
            refresh_token=str(payload.get("refresh_token") or ""),
            id_token=str(payload.get("id_token") or ""),
            token_type=str(payload.get("token_type") or ""),
            scope=str(payload.get("scope") or ""),
            expires_in=int(payload.get("expires_in") or 0),
            raw=dict(payload),
        )


class SchwabClient:
    """Thin Schwab Trader API client built from the user-provided PDFs."""

    def __init__(
        self,
        config: SchwabConfig,
        *,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.config = config
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(
            timeout=httpx.Timeout(config.timeout_seconds),
            trust_env=False,
        )

    def close(self) -> None:
        if self._owns_http:
            try:
                self._http.close()
            except Exception:
                pass

    def __enter__(self) -> "SchwabClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def extract_authorization_code(redirected_url: str) -> str:
        """Extract the authorization code from the callback URL."""

        parsed = urlparse(str(redirected_url or ""))
        code_values = parse_qs(parsed.query).get("code") or []
        if not code_values:
            raise ValueError("No OAuth authorization code was found in the redirected URL.")
        return unquote(str(code_values[0]))

    def build_authorization_url(self, *, state: str | None = None) -> str:
        client_id = _strip_or_none(self.config.client_id)
        callback_url = _strip_or_none(self.config.app_callback_url)
        if not client_id:
            raise SchwabNotConfiguredError("SCHWAB_CLIENT_ID is required to build the authorization URL.")
        if not callback_url:
            raise SchwabNotConfiguredError("SCHWAB_APP_CALLBACK_URL is required to build the authorization URL.")

        params: dict[str, str] = {
            "client_id": client_id,
            "redirect_uri": callback_url,
        }
        if state:
            params["state"] = str(state)
        return f"{self.config.get_authorization_url()}?{urlencode(params)}"

    def exchange_authorization_code(self, authorization_code: str) -> SchwabOAuthTokens:
        callback_url = _strip_or_none(self.config.app_callback_url)
        if not callback_url:
            raise SchwabNotConfiguredError("SCHWAB_APP_CALLBACK_URL is required to exchange an authorization code.")

        code = _strip_or_none(authorization_code)
        if not code:
            raise ValueError("authorization_code is required")

        response = self._request(
            "POST",
            self.config.get_token_url(),
            auth_mode="basic",
            form_data={
                "grant_type": "authorization_code",
                "code": unquote(code),
                "redirect_uri": callback_url,
            },
        )
        if not isinstance(response.payload, Mapping):
            raise SchwabError("Unexpected Schwab token response.", payload={"url": response.url})
        return SchwabOAuthTokens.from_payload(response.payload)

    def refresh_access_token(self, refresh_token: str | None = None) -> SchwabOAuthTokens:
        token = _strip_or_none(refresh_token) or _strip_or_none(self.config.refresh_token)
        if not token:
            raise SchwabNotConfiguredError("A Schwab refresh token is required to refresh the access token.")

        response = self._request(
            "POST",
            self.config.get_token_url(),
            auth_mode="basic",
            form_data={
                "grant_type": "refresh_token",
                "refresh_token": token,
            },
        )
        if not isinstance(response.payload, Mapping):
            raise SchwabError("Unexpected Schwab refresh-token response.", payload={"url": response.url})
        return SchwabOAuthTokens.from_payload(response.payload)

    def get_account_numbers(self, *, access_token: str | None = None) -> Any:
        return self._trader_request("GET", "/accounts/accountNumbers", access_token=access_token).payload

    def get_accounts(
        self,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        return self._trader_request("GET", "/accounts", access_token=access_token, params=params).payload

    def get_account(
        self,
        account_number: str,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        account = self._require_value(account_number, "account_number")
        path = f"/accounts/{account}"
        return self._trader_request("GET", path, access_token=access_token, params=params).payload

    def list_orders(
        self,
        *,
        access_token: str | None = None,
        account_number: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        if account_number:
            path = f"/accounts/{self._require_value(account_number, 'account_number')}/orders"
        else:
            path = "/orders"
        return self._trader_request("GET", path, access_token=access_token, params=params).payload

    def get_order(
        self,
        account_number: str,
        order_id: str | int,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        account = self._require_value(account_number, "account_number")
        order = self._require_value(order_id, "order_id")
        path = f"/accounts/{account}/orders/{order}"
        return self._trader_request("GET", path, access_token=access_token, params=params).payload

    def place_order(
        self,
        account_number: str,
        order: Mapping[str, Any],
        *,
        access_token: str | None = None,
    ) -> SchwabHTTPResponse:
        account = self._require_value(account_number, "account_number")
        if not order:
            raise ValueError("order is required")
        path = f"/accounts/{account}/orders"
        return self._trader_request("POST", path, access_token=access_token, json_body=order)

    def preview_order(
        self,
        account_number: str,
        order: Mapping[str, Any],
        *,
        access_token: str | None = None,
    ) -> Any:
        account = self._require_value(account_number, "account_number")
        if not order:
            raise ValueError("order is required")
        path = f"/accounts/{account}/previewOrder"
        return self._trader_request("POST", path, access_token=access_token, json_body=order).payload

    def replace_order(
        self,
        account_number: str,
        order_id: str | int,
        order: Mapping[str, Any],
        *,
        access_token: str | None = None,
    ) -> SchwabHTTPResponse:
        account = self._require_value(account_number, "account_number")
        order_identifier = self._require_value(order_id, "order_id")
        if not order:
            raise ValueError("order is required")
        path = f"/accounts/{account}/orders/{order_identifier}"
        return self._trader_request("PUT", path, access_token=access_token, json_body=order)

    def cancel_order(
        self,
        account_number: str,
        order_id: str | int,
        *,
        access_token: str | None = None,
    ) -> SchwabHTTPResponse:
        account = self._require_value(account_number, "account_number")
        order_identifier = self._require_value(order_id, "order_id")
        path = f"/accounts/{account}/orders/{order_identifier}"
        return self._trader_request("DELETE", path, access_token=access_token)

    def list_transactions(
        self,
        account_number: str,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        account = self._require_value(account_number, "account_number")
        path = f"/accounts/{account}/transactions"
        return self._trader_request("GET", path, access_token=access_token, params=params).payload

    def get_transaction(
        self,
        account_number: str,
        transaction_id: str | int,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        account = self._require_value(account_number, "account_number")
        transaction = self._require_value(transaction_id, "transaction_id")
        path = f"/accounts/{account}/transactions/{transaction}"
        return self._trader_request("GET", path, access_token=access_token, params=params).payload

    def get_user_preference(
        self,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        return self._trader_request("GET", "/userPreference", access_token=access_token, params=params).payload

    def _trader_request(
        self,
        method: str,
        path: str,
        *,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
    ) -> SchwabHTTPResponse:
        base = self.config.trader_base_url.rstrip("/")
        normalized_path = "/" + str(path or "").lstrip("/")
        return self._request(
            method,
            f"{base}{normalized_path}",
            auth_mode="bearer",
            access_token=access_token,
            params=params,
            json_body=json_body,
        )

    def _request(
        self,
        method: str,
        url: str,
        *,
        auth_mode: str,
        access_token: str | None = None,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
        form_data: Optional[Mapping[str, Any]] = None,
    ) -> SchwabHTTPResponse:
        headers = self._build_headers(auth_mode=auth_mode, access_token=access_token)
        request_kwargs: dict[str, Any] = {"headers": headers}
        if params is not None:
            request_kwargs["params"] = {key: value for key, value in params.items() if value is not None}
        if json_body is not None:
            request_kwargs["json"] = dict(json_body)
        if form_data is not None:
            request_kwargs["data"] = {key: value for key, value in form_data.items() if value is not None}
            request_kwargs["headers"] = {**headers, "Content-Type": "application/x-www-form-urlencoded"}

        try:
            response = self._http.request(str(method).upper(), str(url), **request_kwargs)
        except httpx.TimeoutException as exc:
            raise SchwabError(f"Schwab timeout calling {url}", payload={"url": url}) from exc
        except Exception as exc:
            raise SchwabError(
                f"Schwab call failed: {type(exc).__name__}: {exc}",
                payload={"url": url},
            ) from exc

        if response.status_code < 400:
            return SchwabHTTPResponse(
                status_code=int(response.status_code),
                url=str(response.url),
                payload=self._parse_payload(response),
                headers=response.headers,
            )

        detail = self._extract_detail(response)
        payload = {
            "url": str(response.url),
            "status_code": int(response.status_code),
            "detail": detail,
        }

        if response.status_code in {401, 403}:
            raise SchwabAuthError("Schwab auth failed.", status_code=response.status_code, detail=detail, payload=payload)
        if response.status_code == 404:
            raise SchwabNotFoundError(detail or "Not found.", status_code=response.status_code, detail=detail, payload=payload)
        if response.status_code == 429:
            raise SchwabRateLimitError(
                detail or "Rate limited.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )
        if 500 <= response.status_code <= 599:
            raise SchwabServerError(
                detail or "Schwab server error.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )

        raise SchwabError(
            f"Schwab error (status={response.status_code}).",
            status_code=response.status_code,
            detail=detail,
            payload=payload,
        )

    def _build_headers(self, *, auth_mode: str, access_token: str | None) -> dict[str, str]:
        if auth_mode == "basic":
            client_id = _strip_or_none(self.config.client_id)
            client_secret = _strip_or_none(self.config.client_secret)
            if not client_id:
                raise SchwabNotConfiguredError("SCHWAB_CLIENT_ID is required for Schwab OAuth.")
            if not client_secret:
                raise SchwabNotConfiguredError("SCHWAB_CLIENT_SECRET is required for Schwab OAuth.")
            credential_bytes = f"{client_id}:{client_secret}".encode("utf-8")
            encoded = base64.b64encode(credential_bytes).decode("ascii")
            return {"Authorization": f"Basic {encoded}"}

        if auth_mode == "bearer":
            token = _strip_or_none(access_token) or _strip_or_none(self.config.access_token)
            if not token:
                raise SchwabNotConfiguredError(
                    "A Schwab access token is required. Set SCHWAB_ACCESS_TOKEN or pass access_token explicitly."
                )
            return {"Authorization": f"Bearer {token}"}

        return {}

    @staticmethod
    def _parse_payload(response: httpx.Response) -> Any:
        text = response.text
        if not text:
            return None
        try:
            return response.json()
        except Exception:
            return text

    @staticmethod
    def _extract_detail(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            text = (response.text or "").strip()
            return text or response.reason_phrase

        if isinstance(payload, dict):
            for key in ("message", "error", "detail"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return json.dumps(payload, ensure_ascii=False)

        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return response.reason_phrase

    @staticmethod
    def _require_value(value: object, field_name: str) -> str:
        resolved = _strip_or_none(value)
        if not resolved:
            raise ValueError(f"{field_name} is required")
        return resolved
