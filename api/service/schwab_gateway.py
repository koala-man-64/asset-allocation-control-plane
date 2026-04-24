from __future__ import annotations

import logging
import secrets
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping, Optional

from api.service.settings import SchwabSettings
from schwab import SchwabClient, SchwabConfig, SchwabHTTPResponse, SchwabOAuthTokens
from schwab.errors import (
    SchwabAuthError,
    SchwabError,
    SchwabNotConfiguredError,
    SchwabNotFoundError,
    SchwabRateLimitError,
    SchwabServerError,
)

logger = logging.getLogger("asset-allocation.api.schwab")


class SchwabGatewayValidationError(ValueError):
    """Raised when a control-plane Schwab request is malformed."""


class SchwabGatewaySessionExpiredError(RuntimeError):
    """Raised when Schwab tokens are absent, expired, or rejected."""


class SchwabGatewayAmbiguousWriteError(RuntimeError):
    """Raised when a Schwab write may have reached the provider but no outcome is known."""


@dataclass
class _PendingAuthState:
    state: str
    subject: Optional[str]
    created_at: datetime
    expires_at: datetime


@dataclass
class _TokenState:
    access_token: str = ""
    refresh_token: str = ""
    expires_at: Optional[datetime] = None
    connected_at: Optional[datetime] = None
    refreshed_at: Optional[datetime] = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _isoformat_or_none(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _nonblank(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SchwabGatewayValidationError(f"{field_name} is required.")
    return text


class SchwabGateway:
    def __init__(self, settings: SchwabSettings, *, client: Optional[SchwabClient] = None) -> None:
        self._settings = settings
        self._lock = threading.RLock()
        self._pending_auth: dict[str, _PendingAuthState] = {}
        self._tokens = _TokenState(
            access_token=settings.access_token or "",
            refresh_token=settings.refresh_token or "",
            connected_at=_utc_now() if settings.access_token or settings.refresh_token else None,
        )
        self._client = client or SchwabClient(self._build_config(settings))

    @staticmethod
    def _build_config(settings: SchwabSettings) -> SchwabConfig:
        return SchwabConfig(
            client_id=settings.client_id or "",
            client_secret=settings.client_secret or "",
            app_callback_url=settings.callback_url or "",
            access_token=settings.access_token or "",
            refresh_token=settings.refresh_token or "",
            timeout_seconds=settings.timeout_seconds,
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            logger.debug("Schwab client close failed during shutdown.", exc_info=True)

    def start_connect(self, *, subject: Optional[str]) -> dict[str, Any]:
        now = _utc_now()
        state = secrets.token_urlsafe(24)
        try:
            authorize_url = self._client.build_authorization_url(state=state)
        except SchwabNotConfiguredError:
            raise
        except Exception as exc:
            raise SchwabError(f"Failed to build Schwab authorization URL: {exc}") from exc

        pending = _PendingAuthState(
            state=state,
            subject=subject,
            created_at=now,
            expires_at=now + timedelta(minutes=5),
        )
        with self._lock:
            self._purge_expired_locked(now)
            self._pending_auth[state] = pending

        response = {
            "authorize_url": authorize_url,
            "state": state,
            "state_expires_at": _isoformat_or_none(pending.expires_at),
        }
        if self._settings.callback_url:
            response["callback_url"] = self._settings.callback_url
        return response

    def complete_connect(self, *, code: str, state: str, subject: Optional[str]) -> dict[str, Any]:
        pending = self._pending_state(state=state)
        if pending.subject and subject and pending.subject != subject:
            raise SchwabGatewayValidationError("Schwab OAuth state belongs to a different authenticated subject.")
        return self._finish_connect(code=code, state=state)

    def complete_connect_from_callback(self, *, code: str, state: str) -> dict[str, Any]:
        self._pending_state(state=state)
        return self._finish_connect(code=code, state=state)

    def get_session_state(self) -> dict[str, Any]:
        now = _utc_now()
        with self._lock:
            self._purge_expired_locked(now)
            token_expires_at = self._tokens.expires_at
            connected = bool(self._tokens.access_token or self._tokens.refresh_token)
            return {
                "configured": bool(self._settings.client_id and self._settings.client_secret),
                "connected": connected,
                "pending_connect": bool(self._pending_auth),
                "pending_connect_expires_at": _isoformat_or_none(
                    min((entry.expires_at for entry in self._pending_auth.values()), default=None)
                ),
                "has_access_token": bool(self._tokens.access_token),
                "has_refresh_token": bool(self._tokens.refresh_token),
                "token_expires_at": _isoformat_or_none(token_expires_at),
                "connected_at": _isoformat_or_none(self._tokens.connected_at),
                "refreshed_at": _isoformat_or_none(self._tokens.refreshed_at),
            }

    def disconnect(self) -> dict[str, Any]:
        with self._lock:
            self._pending_auth.clear()
            self._tokens = _TokenState()
        return {"disconnected": True}

    def get_account_numbers(self, *, subject: Optional[str]) -> Any:
        return self._execute_read(
            operation="account_numbers",
            subject=subject,
            call=lambda access_token: self._client.get_account_numbers(access_token=access_token),
        )

    def get_accounts(self, *, subject: Optional[str], fields: Optional[str] = None) -> Any:
        params = {"fields": fields} if fields else None
        return self._execute_read(
            operation="accounts",
            subject=subject,
            call=lambda access_token: self._client.get_accounts(access_token=access_token, params=params),
        )

    def get_account(self, *, account_number: str, subject: Optional[str], fields: Optional[str] = None) -> Any:
        account = _nonblank(account_number, "account_number")
        params = {"fields": fields} if fields else None
        return self._execute_read(
            operation="account",
            subject=subject,
            call=lambda access_token: self._client.get_account(account, access_token=access_token, params=params),
        )

    def get_balance(self, *, account_number: str, subject: Optional[str]) -> dict[str, Any]:
        payload = self.get_account(account_number=account_number, subject=subject)
        account_payload = payload.get("securitiesAccount") if isinstance(payload, Mapping) else None
        if not isinstance(account_payload, Mapping):
            return {"account_number": account_number, "response": payload}
        return {
            "account_number": account_number,
            "currentBalances": account_payload.get("currentBalances"),
            "initialBalances": account_payload.get("initialBalances"),
            "projectedBalances": account_payload.get("projectedBalances"),
            "securitiesAccount": dict(account_payload),
        }

    def get_positions(self, *, account_number: str, subject: Optional[str]) -> Any:
        return self.get_account(account_number=account_number, subject=subject, fields="positions")

    def list_orders(
        self,
        *,
        subject: Optional[str],
        account_number: Optional[str] = None,
        max_results: Optional[int] = None,
        from_entered_time: Optional[str] = None,
        to_entered_time: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Any:
        params: dict[str, Any] = {}
        if max_results is not None:
            params["maxResults"] = max_results
        if from_entered_time:
            params["fromEnteredTime"] = from_entered_time
        if to_entered_time:
            params["toEnteredTime"] = to_entered_time
        if status:
            params["status"] = status

        account = account_number.strip() if account_number else None
        return self._execute_read(
            operation="orders",
            subject=subject,
            call=lambda access_token: self._client.list_orders(
                access_token=access_token,
                account_number=account,
                params=params or None,
            ),
        )

    def get_order(self, *, account_number: str, order_id: str, subject: Optional[str]) -> Any:
        account = _nonblank(account_number, "account_number")
        order = _nonblank(order_id, "order_id")
        return self._execute_read(
            operation="order",
            subject=subject,
            call=lambda access_token: self._client.get_order(account, order, access_token=access_token),
        )

    def preview_order(self, *, account_number: str, order: Mapping[str, Any], subject: Optional[str]) -> Any:
        account = _nonblank(account_number, "account_number")
        normalized_order = self._require_order(order)
        return self._execute_write(
            action="preview",
            subject=subject,
            call=lambda access_token: self._client.preview_order(account, normalized_order, access_token=access_token),
        )

    def place_order(
        self,
        *,
        account_number: str,
        order: Mapping[str, Any],
        subject: Optional[str],
    ) -> dict[str, Any]:
        account = _nonblank(account_number, "account_number")
        normalized_order = self._require_order(order)
        response = self._execute_write(
            action="place",
            subject=subject,
            call=lambda access_token: self._client.place_order(account, normalized_order, access_token=access_token),
        )
        return self._response_summary(response)

    def replace_order(
        self,
        *,
        account_number: str,
        order_id: str,
        order: Mapping[str, Any],
        subject: Optional[str],
    ) -> dict[str, Any]:
        account = _nonblank(account_number, "account_number")
        order_identifier = _nonblank(order_id, "order_id")
        normalized_order = self._require_order(order)
        response = self._execute_write(
            action="replace",
            subject=subject,
            call=lambda access_token: self._client.replace_order(
                account,
                order_identifier,
                normalized_order,
                access_token=access_token,
            ),
        )
        return self._response_summary(response)

    def cancel_order(self, *, account_number: str, order_id: str, subject: Optional[str]) -> dict[str, Any]:
        account = _nonblank(account_number, "account_number")
        order_identifier = _nonblank(order_id, "order_id")
        response = self._execute_write(
            action="cancel",
            subject=subject,
            call=lambda access_token: self._client.cancel_order(account, order_identifier, access_token=access_token),
        )
        return self._response_summary(response)

    def list_transactions(
        self,
        *,
        account_number: str,
        subject: Optional[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        types: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> Any:
        account = _nonblank(account_number, "account_number")
        params: dict[str, Any] = {}
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        if types:
            params["types"] = types
        if symbol:
            params["symbol"] = symbol
        return self._execute_read(
            operation="transactions",
            subject=subject,
            call=lambda access_token: self._client.list_transactions(
                account,
                access_token=access_token,
                params=params or None,
            ),
        )

    def get_transaction(self, *, account_number: str, transaction_id: str, subject: Optional[str]) -> Any:
        account = _nonblank(account_number, "account_number")
        transaction = _nonblank(transaction_id, "transaction_id")
        return self._execute_read(
            operation="transaction",
            subject=subject,
            call=lambda access_token: self._client.get_transaction(account, transaction, access_token=access_token),
        )

    def get_user_preference(self, *, subject: Optional[str]) -> Any:
        return self._execute_read(
            operation="user_preference",
            subject=subject,
            call=lambda access_token: self._client.get_user_preference(access_token=access_token),
        )

    def _finish_connect(self, *, code: str, state: str) -> dict[str, Any]:
        authorization_code = _nonblank(code, "code")
        tokens = self._client.exchange_authorization_code(authorization_code)
        now = _utc_now()
        with self._lock:
            self._store_tokens_locked(tokens, now=now)
            self._pending_auth.pop(state, None)
        return {
            "connected": True,
            "token_expires_at": _isoformat_or_none(self._tokens.expires_at),
            "has_refresh_token": bool(self._tokens.refresh_token),
        }

    def _pending_state(self, *, state: str) -> _PendingAuthState:
        state_value = _nonblank(state, "state")
        now = _utc_now()
        with self._lock:
            self._purge_expired_locked(now)
            pending = self._pending_auth.get(state_value)
        if pending is None:
            raise SchwabGatewayValidationError("The callback did not match an active Schwab authorization request.")
        return pending

    def _execute_read(
        self,
        *,
        operation: str,
        subject: Optional[str],
        call: Callable[[str], Any],
    ) -> Any:
        del operation, subject
        access_token = self._access_token_for_read()
        try:
            return call(access_token)
        except SchwabAuthError as exc:
            if not self._has_refresh_token():
                self._clear_access_token()
                raise SchwabGatewaySessionExpiredError("Schwab rejected the broker session. Reconnect required.") from exc
            try:
                return call(self._refresh_access_token())
            except SchwabAuthError as retry_exc:
                self._clear_access_token()
                raise SchwabGatewaySessionExpiredError(
                    "Schwab rejected the refreshed broker session. Reconnect required."
                ) from retry_exc

    def _execute_write(
        self,
        *,
        action: str,
        subject: Optional[str],
        call: Callable[[str], Any],
    ) -> Any:
        del subject
        access_token = self._access_token_for_write()
        try:
            return call(access_token)
        except SchwabAuthError as exc:
            self._clear_access_token()
            raise SchwabGatewaySessionExpiredError(
                "Schwab rejected the broker session. Reconnect before previewing or trading."
            ) from exc
        except SchwabError as exc:
            if exc.status_code is None:
                raise SchwabGatewayAmbiguousWriteError(
                    f"Schwab {action} outcome is unknown. Reconcile through order history before retrying."
                ) from exc
            raise

    def _access_token_for_read(self) -> str:
        now = _utc_now()
        with self._lock:
            self._purge_expired_locked(now)
            token = self._tokens.access_token
            expires_at = self._tokens.expires_at
            if token and (expires_at is None or expires_at > now):
                return token
            refresh_token = self._tokens.refresh_token
        if refresh_token:
            return self._refresh_access_token()
        raise SchwabGatewaySessionExpiredError("No active Schwab broker session exists. Connect first.")

    def _access_token_for_write(self) -> str:
        return self._access_token_for_read()

    def _refresh_access_token(self) -> str:
        with self._lock:
            refresh_token = self._tokens.refresh_token
        if not refresh_token:
            raise SchwabGatewaySessionExpiredError("No Schwab refresh token is available. Reconnect required.")
        tokens = self._client.refresh_access_token(refresh_token=refresh_token)
        now = _utc_now()
        with self._lock:
            self._store_tokens_locked(tokens, now=now, refreshed=True)
            return self._tokens.access_token

    def _has_refresh_token(self) -> bool:
        with self._lock:
            return bool(self._tokens.refresh_token)

    def _clear_access_token(self) -> None:
        with self._lock:
            self._tokens.access_token = ""
            self._tokens.expires_at = None

    def _store_tokens_locked(self, tokens: SchwabOAuthTokens, *, now: datetime, refreshed: bool = False) -> None:
        expires_at = now + timedelta(seconds=tokens.expires_in) if tokens.expires_in > 0 else None
        refresh_token = tokens.refresh_token or self._tokens.refresh_token
        connected_at = self._tokens.connected_at or now
        self._tokens = _TokenState(
            access_token=tokens.access_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
            connected_at=connected_at,
            refreshed_at=now if refreshed else self._tokens.refreshed_at,
        )

    def _purge_expired_locked(self, now: datetime) -> None:
        expired_states = [state for state, pending in self._pending_auth.items() if pending.expires_at <= now]
        for state in expired_states:
            self._pending_auth.pop(state, None)

    @staticmethod
    def _require_order(order: Mapping[str, Any]) -> dict[str, Any]:
        if not order:
            raise SchwabGatewayValidationError("order is required.")
        return dict(order)

    @staticmethod
    def _response_summary(response: SchwabHTTPResponse) -> dict[str, Any]:
        return {
            "status_code": response.status_code,
            "location": response.headers.get("Location"),
            "response": response.payload,
        }


__all__ = [
    "SchwabGateway",
    "SchwabGatewayAmbiguousWriteError",
    "SchwabGatewaySessionExpiredError",
    "SchwabGatewayValidationError",
    "SchwabAuthError",
    "SchwabError",
    "SchwabNotConfiguredError",
    "SchwabNotFoundError",
    "SchwabRateLimitError",
    "SchwabServerError",
]
