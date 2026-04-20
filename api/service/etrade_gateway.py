from __future__ import annotations

import hashlib
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Mapping, Optional
from zoneinfo import ZoneInfo

from api.service.settings import ETradeSettings
from etrade_provider import (
    ETradeApiError,
    ETradeBrokerAuthError,
    ETradeClient,
    ETradeConfig,
    ETradeEnvironmentConfig,
    ETradeInactiveSessionError,
    ETradeNotConfiguredError,
    ETradeSessionExpiredError,
    ETradeValidationError,
)

logger = logging.getLogger("asset-allocation.api.etrade")

ETradeEnvironment = Literal["sandbox", "live"]
_EASTERN_TZ = ZoneInfo("America/New_York")
_SUPPORTED_ENVIRONMENTS: tuple[ETradeEnvironment, ETradeEnvironment] = ("sandbox", "live")
_SUPPORTED_PRICE_TYPES = {
    "MARKET",
    "LIMIT",
    "STOP",
    "STOP_LIMIT",
    "MARKET_ON_OPEN",
    "MARKET_ON_CLOSE",
    "LIMIT_ON_OPEN",
    "LIMIT_ON_CLOSE",
}
_SUPPORTED_ORDER_TERMS = {
    "GOOD_FOR_DAY",
    "GOOD_UNTIL_CANCEL",
    "IMMEDIATE_OR_CANCEL",
    "FILL_OR_KILL",
}
_SUPPORTED_MARKET_SESSIONS = {"REGULAR", "EXTENDED"}
_EQUITY_SIDES = {"BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"}
_OPTION_SIDES = {"BUY_OPEN", "BUY_CLOSE", "SELL_OPEN", "SELL_CLOSE"}
_SUPPORTED_SORT_ORDERS = {"ASC", "DESC"}
_TRANSACTION_GROUP_PATHS = {
    "TRADES": "Trades",
    "WITHDRAWALS": "Withdrawals",
    "CASH": "Cash",
}


@dataclass
class _PendingAuthState:
    environment: ETradeEnvironment
    request_token: str
    request_token_secret: str
    authorize_url: str
    callback_confirmed: bool
    created_at: datetime
    expires_at: datetime


@dataclass
class _BrokerSessionState:
    environment: ETradeEnvironment
    access_token: str
    access_token_secret: str
    created_at: datetime
    expires_at: datetime
    last_activity_at: datetime
    renewed_at: Optional[datetime] = None


@dataclass
class _PreviewCacheEntry:
    environment: ETradeEnvironment
    account_key: str
    client_order_id: str
    preview_id: str
    normalized_order: dict[str, Any]
    place_payload: dict[str, Any]
    preview_response: dict[str, Any]
    created_at: datetime
    expires_at: datetime


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_environment(environment: str) -> ETradeEnvironment:
    normalized = str(environment or "").strip().lower()
    if normalized not in _SUPPORTED_ENVIRONMENTS:
        raise ETradeValidationError(f"Unsupported E*TRADE environment={environment!r}.")
    return normalized  # type: ignore[return-value]


def _isoformat_or_none(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _next_eastern_midnight(now: datetime) -> datetime:
    eastern_now = now.astimezone(_EASTERN_TZ)
    next_midnight = datetime.combine(eastern_now.date() + timedelta(days=1), time.min, tzinfo=_EASTERN_TZ)
    return next_midnight.astimezone(UTC)


def _hash_account_key(account_key: str) -> str:
    return hashlib.sha256(account_key.encode("utf-8")).hexdigest()[:12]


def _collect_message_codes(payload: Any) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()

    def _visit(node: Any) -> None:
        if isinstance(node, dict):
            code_value = node.get("code")
            if code_value is not None:
                code = str(code_value).strip()
                if code and code not in seen:
                    seen.add(code)
                    codes.append(code)
            for value in node.values():
                _visit(value)
            return
        if isinstance(node, list):
            for item in node:
                _visit(item)

    _visit(payload)
    return codes


def _extract_nested_payload(root: Mapping[str, Any], key: str) -> Any:
    value = root.get(key)
    if value is not None:
        return value
    return root


def _extract_preview_id(payload: Mapping[str, Any]) -> str:
    root = _extract_nested_payload(payload, "PreviewOrderResponse")
    preview_ids = root.get("PreviewIds") if isinstance(root, dict) else None
    if isinstance(preview_ids, dict):
        preview_value = preview_ids.get("previewId")
        if preview_value is not None:
            return str(preview_value)
    if isinstance(preview_ids, list):
        for item in preview_ids:
            if isinstance(item, dict) and item.get("previewId") is not None:
                return str(item.get("previewId"))
    raise ETradeApiError("E*TRADE preview response did not include a preview ID.", code="invalid_preview_response")


def _extract_order_id(payload: Mapping[str, Any]) -> Optional[str]:
    root = payload
    for key in ("PlaceOrderResponse", "CancelOrderResponse", "Order", "order"):
        nested = root.get(key) if isinstance(root, dict) else None
        if isinstance(nested, dict):
            root = nested
            break
    if isinstance(root, dict):
        order_id = root.get("orderId")
        if order_id is not None:
            return str(order_id)
        order_ids = root.get("OrderIds")
        if isinstance(order_ids, dict) and order_ids.get("orderId") is not None:
            return str(order_ids.get("orderId"))
        if isinstance(order_ids, list):
            for item in order_ids:
                if isinstance(item, dict) and item.get("orderId") is not None:
                    return str(item.get("orderId"))
    return None


def _first(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _format_date_mmddyyyy(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return _parse_iso_date(value).strftime("%m%d%Y")


def _parse_iso_date(value: str) -> date:
    raw = str(value or "").strip()
    if not raw:
        raise ETradeValidationError("Date is required.")
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        raise ETradeValidationError(f"Invalid date={value!r}. Use YYYY-MM-DD.") from None


def _format_number(value: Any, *, default: str = "0") -> str:
    if value is None or str(value).strip() == "":
        return default
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ETradeValidationError(f"Invalid numeric value={value!r}.") from exc
    rendered = format(number, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or default


def _generate_client_order_id() -> str:
    return uuid.uuid4().hex[:20]


class ETradeGateway:
    def __init__(self, settings: ETradeSettings) -> None:
        self._settings = settings
        self._lock = threading.RLock()
        self._pending_auth: dict[ETradeEnvironment, _PendingAuthState] = {}
        self._sessions: dict[ETradeEnvironment, _BrokerSessionState] = {}
        self._preview_cache: dict[str, _PreviewCacheEntry] = {}
        self._clients = self._build_clients(settings)

    @staticmethod
    def _build_clients(settings: ETradeSettings) -> dict[ETradeEnvironment, ETradeClient]:
        config = ETradeConfig(
            sandbox=ETradeEnvironmentConfig(
                environment="sandbox",
                consumer_key=settings.sandbox_consumer_key,
                consumer_secret=settings.sandbox_consumer_secret,
                api_base_url="https://apisb.etrade.com",
            ),
            live=ETradeEnvironmentConfig(
                environment="live",
                consumer_key=settings.live_consumer_key,
                consumer_secret=settings.live_consumer_secret,
                api_base_url="https://api.etrade.com",
            ),
            timeout_seconds=settings.timeout_seconds,
            read_retry_attempts=settings.read_retry_attempts,
            read_retry_base_delay_seconds=settings.read_retry_base_delay_seconds,
        )
        return {
            "sandbox": ETradeClient(
                config.sandbox,
                timeout_seconds=config.timeout_seconds,
                read_retry_attempts=config.read_retry_attempts,
                read_retry_base_delay_seconds=config.read_retry_base_delay_seconds,
            ),
            "live": ETradeClient(
                config.live,
                timeout_seconds=config.timeout_seconds,
                read_retry_attempts=config.read_retry_attempts,
                read_retry_base_delay_seconds=config.read_retry_base_delay_seconds,
            ),
        }

    def close(self) -> None:
        for environment in _SUPPORTED_ENVIRONMENTS:
            try:
                self.disconnect(environment=environment)
            except Exception:
                logger.debug("E*TRADE disconnect during shutdown failed for %s.", environment, exc_info=True)
        with self._lock:
            self._pending_auth.clear()
            self._sessions.clear()
            self._preview_cache.clear()

    def start_connect(self, *, environment: str) -> dict[str, Any]:
        env = _normalize_environment(environment)
        client = self._client_for(env)
        now = _utc_now()
        payload = client.fetch_request_token(callback_uri="oob")
        request_token = str(payload.get("oauth_token") or "").strip()
        request_token_secret = str(payload.get("oauth_token_secret") or "").strip()
        if not request_token or not request_token_secret:
            raise ETradeApiError("E*TRADE request-token response was incomplete.", code="invalid_request_token_response")

        pending = _PendingAuthState(
            environment=env,
            request_token=request_token,
            request_token_secret=request_token_secret,
            authorize_url=client.build_authorize_url(request_token=request_token),
            callback_confirmed=str(payload.get("oauth_callback_confirmed") or "").strip().lower() == "true",
            created_at=now,
            expires_at=now + timedelta(seconds=self._settings.pending_auth_ttl_seconds),
        )
        with self._lock:
            self._purge_expired_locked(now)
            self._pending_auth[env] = pending
        return {
            "environment": env,
            "authorize_url": pending.authorize_url,
            "callback_confirmed": pending.callback_confirmed,
            "request_token_expires_at": _isoformat_or_none(pending.expires_at),
        }

    def complete_connect(self, *, environment: str, verifier: str) -> dict[str, Any]:
        env = _normalize_environment(environment)
        verifier_value = str(verifier or "").strip()
        if not verifier_value:
            raise ETradeValidationError("verifier is required.")
        with self._lock:
            self._purge_expired_locked(_utc_now())
            pending = self._pending_auth.get(env)
        if pending is None:
            raise ETradeValidationError(f"No pending E*TRADE connect flow exists for {env}.")
        return self._finish_connect(pending=pending, verifier=verifier_value)

    def complete_connect_from_callback(self, *, request_token: str, verifier: str) -> dict[str, Any]:
        request_token_value = str(request_token or "").strip()
        verifier_value = str(verifier or "").strip()
        if not request_token_value or not verifier_value:
            raise ETradeValidationError("oauth_token and oauth_verifier are required.")

        with self._lock:
            self._purge_expired_locked(_utc_now())
            pending = next(
                (entry for entry in self._pending_auth.values() if entry.request_token == request_token_value),
                None,
            )
        if pending is None:
            raise ETradeValidationError("The callback did not match an active E*TRADE authorization request.")
        return self._finish_connect(pending=pending, verifier=verifier_value)

    def get_session_state(self, *, environment: Optional[str] = None) -> dict[str, Any]:
        now = _utc_now()
        with self._lock:
            self._purge_expired_locked(now)
            if environment is not None:
                env = _normalize_environment(environment)
                return self._session_snapshot_locked(env, now)
            return {
                "sessions": [self._session_snapshot_locked(env, now) for env in _SUPPORTED_ENVIRONMENTS],
            }

    def disconnect(self, *, environment: str) -> dict[str, Any]:
        env = _normalize_environment(environment)
        with self._lock:
            session = self._sessions.pop(env, None)
            self._pending_auth.pop(env, None)
            preview_ids = [preview_id for preview_id, entry in self._preview_cache.items() if entry.environment == env]
            for preview_id in preview_ids:
                self._preview_cache.pop(preview_id, None)

        revoked = False
        if session is not None:
            try:
                self._client_for(env).revoke_access_token(
                    access_token=session.access_token,
                    access_token_secret=session.access_token_secret,
                )
                revoked = True
            except Exception:
                logger.warning("Failed to revoke E*TRADE access token for %s.", env, exc_info=True)
        return {"environment": env, "disconnected": True, "revoked": revoked}

    def list_accounts(self, *, environment: str, subject: Optional[str]) -> Optional[dict[str, Any]]:
        env = _normalize_environment(environment)
        return self._execute_optional_read(
            environment=env,
            operation="accounts",
            subject=subject,
            call=lambda session: self._client_for(env).list_accounts(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
            ),
        )

    def get_balance(
        self,
        *,
        environment: str,
        account_key: str,
        subject: Optional[str],
        account_type: Optional[str] = None,
        real_time_nav: bool = False,
    ) -> dict[str, Any]:
        env = _normalize_environment(environment)
        return self._execute_read(
            environment=env,
            operation="balance",
            subject=subject,
            call=lambda session: self._client_for(env).get_balance(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=account_key,
                inst_type="BROKERAGE",
                real_time_nav=real_time_nav,
                account_type=account_type,
            ),
        )

    def get_portfolio(
        self,
        *,
        environment: str,
        account_key: str,
        subject: Optional[str],
        count: Optional[int] = None,
        page_number: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        market_session: Optional[str] = None,
        totals_required: bool = False,
        lots_required: bool = False,
        view: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        env = _normalize_environment(environment)
        params: dict[str, Any] = {}
        if count is not None:
            params["count"] = count
        if page_number is not None:
            params["pageNumber"] = page_number
        if sort_by:
            params["sortBy"] = str(sort_by).strip()
        if sort_order:
            params["sortOrder"] = str(sort_order).strip().upper()
        if market_session:
            params["marketSession"] = str(market_session).strip().upper()
        if totals_required:
            params["totalsRequired"] = "true"
        if lots_required:
            params["lotsRequired"] = "true"
        if view:
            params["view"] = str(view).strip().upper()

        return self._execute_optional_read(
            environment=env,
            operation="portfolio",
            subject=subject,
            call=lambda session: self._client_for(env).get_portfolio(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=account_key,
                params=params or None,
            ),
        )

    def get_quotes(
        self,
        *,
        environment: str,
        symbols: list[str],
        subject: Optional[str],
        detail_flag: Optional[str] = None,
        require_earnings_date: bool = False,
        override_symbol_count: bool = False,
        skip_mini_options_check: bool = False,
    ) -> dict[str, Any]:
        env = _normalize_environment(environment)
        normalized_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
        if not normalized_symbols:
            raise ETradeValidationError("At least one symbol is required.")
        if len(normalized_symbols) > 50:
            raise ETradeValidationError("E*TRADE quotes support at most 50 symbols.")

        params: dict[str, Any] = {}
        if detail_flag:
            params["detailFlag"] = str(detail_flag).strip().upper()
        if require_earnings_date:
            params["requireEarningsDate"] = "true"
        if override_symbol_count:
            params["overrideSymbolCount"] = "true"
        if skip_mini_options_check:
            params["skipMiniOptionsCheck"] = "true"

        return self._execute_read(
            environment=env,
            operation="quotes",
            subject=subject,
            call=lambda session: self._client_for(env).get_quotes(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                symbols=",".join(normalized_symbols),
                params=params or None,
            ),
        )

    def list_orders(
        self,
        *,
        environment: str,
        account_key: str,
        subject: Optional[str],
        count: Optional[int] = None,
        marker: Optional[str] = None,
        status: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        symbol: Optional[str] = None,
        security_type: Optional[str] = None,
        transaction_type: Optional[str] = None,
        market_session: Optional[str] = None,
    ) -> dict[str, Any]:
        env = _normalize_environment(environment)
        params: dict[str, Any] = {}
        if count is not None:
            params["count"] = count
        if marker:
            params["marker"] = str(marker).strip()
        if status:
            params["status"] = str(status).strip().upper()
        if from_date:
            params["fromDate"] = _format_date_mmddyyyy(from_date)
        if to_date:
            params["toDate"] = _format_date_mmddyyyy(to_date)
        if symbol:
            params["symbol"] = str(symbol).strip().upper()
        if security_type:
            params["securityType"] = str(security_type).strip().upper()
        if transaction_type:
            params["transactionType"] = str(transaction_type).strip().upper()
        if market_session:
            params["marketSession"] = str(market_session).strip().upper()

        return self._execute_read(
            environment=env,
            operation="orders",
            subject=subject,
            call=lambda session: self._client_for(env).list_orders(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=account_key,
                params=params or None,
            ),
        )

    def list_transactions(
        self,
        *,
        environment: str,
        account_key: str,
        subject: Optional[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sort_order: Optional[str] = None,
        marker: Optional[str] = None,
        count: Optional[int] = None,
        transaction_group: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        env = _normalize_environment(environment)
        params: dict[str, Any] = {}
        normalized_group: Optional[str] = None

        if count is not None:
            params["count"] = count
        if marker:
            params["marker"] = str(marker).strip()
        if sort_order:
            normalized_sort_order = str(sort_order).strip().upper()
            if normalized_sort_order not in _SUPPORTED_SORT_ORDERS:
                raise ETradeValidationError("sortOrder must be ASC or DESC.")
            params["sortOrder"] = normalized_sort_order
        if transaction_group:
            normalized_group = str(transaction_group).strip().upper()
            if normalized_group not in _TRANSACTION_GROUP_PATHS:
                raise ETradeValidationError("transactionGroup must be one of TRADES, WITHDRAWALS, or CASH.")

        has_start_date = bool(str(start_date or "").strip())
        has_end_date = bool(str(end_date or "").strip())
        if has_start_date != has_end_date:
            raise ETradeValidationError("startDate and endDate must be provided together.")
        if has_start_date and has_end_date:
            parsed_start_date = _parse_iso_date(str(start_date))
            parsed_end_date = _parse_iso_date(str(end_date))
            if parsed_end_date < parsed_start_date:
                raise ETradeValidationError("endDate must be on or after startDate.")
            params["startDate"] = parsed_start_date.strftime("%m%d%Y")
            params["endDate"] = parsed_end_date.strftime("%m%d%Y")

        return self._execute_optional_read(
            environment=env,
            operation="transactions",
            subject=subject,
            call=lambda session: self._client_for(env).list_transactions(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=account_key,
                transaction_group=_TRANSACTION_GROUP_PATHS.get(normalized_group) if normalized_group else None,
                params=params or None,
            ),
        )

    def get_transaction_details(
        self,
        *,
        environment: str,
        account_key: str,
        transaction_id: str,
        subject: Optional[str],
        store_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        env = _normalize_environment(environment)
        transaction_id_value = str(transaction_id or "").strip()
        if not transaction_id_value:
            raise ETradeValidationError("transaction_id is required.")
        store_id_value = str(store_id or "").strip() or None

        return self._execute_optional_read(
            environment=env,
            operation="transaction_detail",
            subject=subject,
            call=lambda session: self._client_for(env).get_transaction_details(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=account_key,
                transaction_id=transaction_id_value,
                store_id=store_id_value,
            ),
        )

    def preview_order(self, *, environment: str, order: Mapping[str, Any], subject: Optional[str]) -> dict[str, Any]:
        env = _normalize_environment(environment)
        normalized_order = self._normalize_order_request(order)
        session = self._session_for_write(env)
        preview_request = self._build_provider_order_request(
            normalized_order=normalized_order,
            root_key="PreviewOrderRequest",
            client_order_id=_generate_client_order_id(),
            preview_id=None,
        )
        try:
            payload = self._client_for(env).preview_order(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=normalized_order["account_key"],
                payload=preview_request,
            )
        except ETradeBrokerAuthError as exc:
            self._clear_session(env)
            raise ETradeSessionExpiredError("E*TRADE rejected the broker session. Reconnect required.") from exc

        preview_id = _extract_preview_id(payload)
        now = _utc_now()
        expires_at = now + timedelta(seconds=self._settings.preview_ttl_seconds)
        cache_entry = _PreviewCacheEntry(
            environment=env,
            account_key=normalized_order["account_key"],
            client_order_id=preview_request["PreviewOrderRequest"]["clientOrderId"],
            preview_id=preview_id,
            normalized_order=dict(normalized_order),
            place_payload=self._build_provider_order_request(
                normalized_order=normalized_order,
                root_key="PlaceOrderRequest",
                client_order_id=preview_request["PreviewOrderRequest"]["clientOrderId"],
                preview_id=preview_id,
            ),
            preview_response=payload,
            created_at=now,
            expires_at=expires_at,
        )
        with self._lock:
            self._purge_expired_locked(now)
            self._preview_cache[preview_id] = cache_entry
            current = self._sessions.get(env)
            if current is not None:
                current.last_activity_at = now

        self._log_audit_event(
            action="preview",
            subject=subject,
            environment=env,
            account_key=normalized_order["account_key"],
            symbol=self._extract_symbol(normalized_order),
            preview_id=preview_id,
            order_id=None,
            provider_payload=payload,
        )
        return {
            "environment": env,
            "preview_id": preview_id,
            "preview_expires_at": _isoformat_or_none(expires_at),
            "response": payload,
        }

    def place_order(self, *, environment: str, preview_id: str, subject: Optional[str]) -> dict[str, Any]:
        env = _normalize_environment(environment)
        preview_id_value = str(preview_id or "").strip()
        if not preview_id_value:
            raise ETradeValidationError("preview_id is required.")
        session = self._session_for_write(env)
        with self._lock:
            self._purge_expired_locked(_utc_now())
            entry = self._preview_cache.get(preview_id_value)
        if entry is None or entry.environment != env:
            raise ETradeValidationError("The preview_id is missing, expired, or belongs to a different environment.")

        try:
            payload = self._client_for(env).place_order(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=entry.account_key,
                payload=entry.place_payload,
            )
        except ETradeBrokerAuthError as exc:
            self._clear_session(env)
            raise ETradeSessionExpiredError("E*TRADE rejected the broker session. Reconnect required.") from exc

        order_id = _extract_order_id(payload)
        now = _utc_now()
        with self._lock:
            self._preview_cache.pop(preview_id_value, None)
            current = self._sessions.get(env)
            if current is not None:
                current.last_activity_at = now

        self._log_audit_event(
            action="place",
            subject=subject,
            environment=env,
            account_key=entry.account_key,
            symbol=self._extract_symbol(entry.normalized_order),
            preview_id=preview_id_value,
            order_id=order_id,
            provider_payload=payload,
        )
        return {
            "environment": env,
            "preview_id": preview_id_value,
            "order_id": order_id,
            "response": payload,
        }

    def cancel_order(
        self,
        *,
        environment: str,
        account_key: str,
        order_id: int,
        subject: Optional[str],
    ) -> dict[str, Any]:
        env = _normalize_environment(environment)
        session = self._session_for_write(env)
        try:
            payload = self._client_for(env).cancel_order(
                access_token=session.access_token,
                access_token_secret=session.access_token_secret,
                account_key=account_key,
                order_id=order_id,
            )
        except ETradeBrokerAuthError as exc:
            self._clear_session(env)
            raise ETradeSessionExpiredError("E*TRADE rejected the broker session. Reconnect required.") from exc

        now = _utc_now()
        with self._lock:
            current = self._sessions.get(env)
            if current is not None:
                current.last_activity_at = now

        self._log_audit_event(
            action="cancel",
            subject=subject,
            environment=env,
            account_key=account_key,
            symbol=None,
            preview_id=None,
            order_id=str(order_id),
            provider_payload=payload,
        )
        return {"environment": env, "order_id": str(order_id), "response": payload}

    def _finish_connect(self, *, pending: _PendingAuthState, verifier: str) -> dict[str, Any]:
        client = self._client_for(pending.environment)
        token_payload = client.fetch_access_token(
            request_token=pending.request_token,
            request_token_secret=pending.request_token_secret,
            verifier=verifier,
        )
        access_token = str(token_payload.get("oauth_token") or "").strip()
        access_secret = str(token_payload.get("oauth_token_secret") or "").strip()
        if not access_token or not access_secret:
            raise ETradeApiError("E*TRADE access-token response was incomplete.", code="invalid_access_token_response")

        now = _utc_now()
        session = _BrokerSessionState(
            environment=pending.environment,
            access_token=access_token,
            access_token_secret=access_secret,
            created_at=now,
            expires_at=_next_eastern_midnight(now),
            last_activity_at=now,
        )
        with self._lock:
            self._pending_auth.pop(pending.environment, None)
            self._sessions[pending.environment] = session
            preview_ids = [preview_id for preview_id, entry in self._preview_cache.items() if entry.environment == pending.environment]
            for preview_id in preview_ids:
                self._preview_cache.pop(preview_id, None)
        return {
            "environment": pending.environment,
            "connected": True,
            "expires_at": _isoformat_or_none(session.expires_at),
            "last_activity_at": _isoformat_or_none(session.last_activity_at),
        }

    def _client_for(self, environment: ETradeEnvironment) -> ETradeClient:
        client = self._clients.get(environment)
        if client is None:
            raise ETradeValidationError(f"Unsupported E*TRADE environment={environment!r}.")
        if not client.config.is_configured:
            raise ETradeNotConfiguredError(f"E*TRADE {environment} credentials are not configured.")
        return client

    def _session_snapshot_locked(self, environment: ETradeEnvironment, now: datetime) -> dict[str, Any]:
        pending = self._pending_auth.get(environment)
        session = self._sessions.get(environment)
        configured = self._clients[environment].config.is_configured
        expires_at = session.expires_at if session else None
        time_to_expiry_seconds = int((expires_at - now).total_seconds()) if expires_at else None
        idle_seconds = int((now - session.last_activity_at).total_seconds()) if session else None
        return {
            "environment": environment,
            "configured": configured,
            "connected": bool(session and expires_at and expires_at > now),
            "pending_connect": pending is not None,
            "pending_connect_expires_at": _isoformat_or_none(pending.expires_at if pending else None),
            "token_expires_at": _isoformat_or_none(expires_at),
            "last_activity_at": _isoformat_or_none(session.last_activity_at if session else None),
            "renewed_at": _isoformat_or_none(session.renewed_at if session else None),
            "idle_seconds": idle_seconds,
            "idle_renew_due": bool(idle_seconds is not None and idle_seconds >= self._settings.idle_renew_seconds),
            "near_expiry": bool(
                time_to_expiry_seconds is not None
                and time_to_expiry_seconds <= self._settings.session_expiry_guard_seconds
            ),
            "time_to_expiry_seconds": time_to_expiry_seconds,
        }

    def _purge_expired_locked(self, now: datetime) -> None:
        expired_pending = [env for env, state in self._pending_auth.items() if state.expires_at <= now]
        for env in expired_pending:
            self._pending_auth.pop(env, None)

        expired_preview_ids = [preview_id for preview_id, entry in self._preview_cache.items() if entry.expires_at <= now]
        for preview_id in expired_preview_ids:
            self._preview_cache.pop(preview_id, None)

        expired_sessions = [env for env, session in self._sessions.items() if session.expires_at <= now]
        for env in expired_sessions:
            self._sessions.pop(env, None)

    def _clear_session(self, environment: ETradeEnvironment) -> None:
        with self._lock:
            self._sessions.pop(environment, None)
            preview_ids = [preview_id for preview_id, entry in self._preview_cache.items() if entry.environment == environment]
            for preview_id in preview_ids:
                self._preview_cache.pop(preview_id, None)

    def _session_for_read(self, environment: ETradeEnvironment) -> _BrokerSessionState:
        now = _utc_now()
        with self._lock:
            self._purge_expired_locked(now)
            session = self._sessions.get(environment)
            if session is None:
                raise ETradeSessionExpiredError("No active E*TRADE broker session exists. Connect first.")
            idle_seconds = (now - session.last_activity_at).total_seconds()
            time_to_expiry_seconds = (session.expires_at - now).total_seconds()
            needs_renew = idle_seconds >= self._settings.idle_renew_seconds

        if needs_renew:
            if time_to_expiry_seconds <= self._settings.session_expiry_guard_seconds:
                self._clear_session(environment)
                raise ETradeSessionExpiredError(
                    "The E*TRADE broker session is too close to midnight Eastern expiry to renew. Reconnect required."
                )
            try:
                self._client_for(environment).renew_access_token(
                    access_token=session.access_token,
                    access_token_secret=session.access_token_secret,
                )
            except ETradeApiError as exc:
                self._clear_session(environment)
                raise ETradeSessionExpiredError("Failed to renew the idle E*TRADE broker session. Reconnect required.") from exc
            renewed_at = _utc_now()
            with self._lock:
                current = self._sessions.get(environment)
                if current is not None:
                    current.last_activity_at = renewed_at
                    current.renewed_at = renewed_at
                    session = current
        return session

    def _session_for_write(self, environment: ETradeEnvironment) -> _BrokerSessionState:
        now = _utc_now()
        with self._lock:
            self._purge_expired_locked(now)
            session = self._sessions.get(environment)
            if session is None:
                raise ETradeSessionExpiredError("No active E*TRADE broker session exists. Connect first.")
            time_to_expiry_seconds = (session.expires_at - now).total_seconds()
            if time_to_expiry_seconds <= self._settings.session_expiry_guard_seconds:
                raise ETradeSessionExpiredError(
                    "The E*TRADE broker session is too close to midnight Eastern expiry. Reconnect before previewing or trading."
                )
            idle_seconds = (now - session.last_activity_at).total_seconds()
            if idle_seconds >= self._settings.idle_renew_seconds:
                raise ETradeInactiveSessionError(
                    "The E*TRADE broker session has been idle for over two hours. Issue a read request to renew it or reconnect before trading."
                )
            return session

    def _execute_read(
        self,
        *,
        environment: ETradeEnvironment,
        operation: str,
        subject: Optional[str],
        call: Any,
    ) -> dict[str, Any]:
        del operation, subject
        session = self._session_for_read(environment)
        try:
            payload = call(session)
        except ETradeBrokerAuthError as exc:
            self._clear_session(environment)
            raise ETradeSessionExpiredError("E*TRADE rejected the broker session. Reconnect required.") from exc
        with self._lock:
            current = self._sessions.get(environment)
            if current is not None:
                current.last_activity_at = _utc_now()
        return payload

    def _execute_optional_read(
        self,
        *,
        environment: ETradeEnvironment,
        operation: str,
        subject: Optional[str],
        call: Any,
    ) -> Optional[dict[str, Any]]:
        del operation, subject
        session = self._session_for_read(environment)
        try:
            payload = call(session)
        except ETradeBrokerAuthError as exc:
            self._clear_session(environment)
            raise ETradeSessionExpiredError("E*TRADE rejected the broker session. Reconnect required.") from exc
        with self._lock:
            current = self._sessions.get(environment)
            if current is not None:
                current.last_activity_at = _utc_now()
        return payload

    def _normalize_order_request(self, order: Mapping[str, Any]) -> dict[str, Any]:
        account_key = str(order.get("account_key") or "").strip()
        if not account_key:
            raise ETradeValidationError("account_key is required.")

        asset_type = str(order.get("asset_type") or "").strip().lower()
        if asset_type not in {"equity", "option"}:
            raise ETradeValidationError("asset_type must be 'equity' or 'option'.")

        side = str(order.get("side") or "").strip().upper()
        allowed_sides = _EQUITY_SIDES if asset_type == "equity" else _OPTION_SIDES
        if side not in allowed_sides:
            raise ETradeValidationError(
                f"side={side!r} is not supported for asset_type={asset_type!r}. Allowed values: {', '.join(sorted(allowed_sides))}."
            )

        quantity_text = _format_number(order.get("quantity"), default="")
        if not quantity_text or Decimal(quantity_text) <= 0:
            raise ETradeValidationError("quantity must be greater than zero.")

        price_type = str(order.get("price_type") or "").strip().upper()
        if price_type not in _SUPPORTED_PRICE_TYPES:
            raise ETradeValidationError(
                f"price_type={price_type!r} is not supported. Allowed values: {', '.join(sorted(_SUPPORTED_PRICE_TYPES))}."
            )

        order_term = str(order.get("term") or "").strip().upper()
        if order_term not in _SUPPORTED_ORDER_TERMS:
            raise ETradeValidationError(
                f"term={order_term!r} is not supported. Allowed values: {', '.join(sorted(_SUPPORTED_ORDER_TERMS))}."
            )

        market_session = str(order.get("session") or "").strip().upper()
        if market_session not in _SUPPORTED_MARKET_SESSIONS:
            raise ETradeValidationError(
                f"session={market_session!r} is not supported. Allowed values: {', '.join(sorted(_SUPPORTED_MARKET_SESSIONS))}."
            )

        limit_price = order.get("limit_price")
        stop_price = order.get("stop_price")
        if price_type in {"LIMIT", "LIMIT_ON_OPEN", "LIMIT_ON_CLOSE", "STOP_LIMIT"} and limit_price is None:
            raise ETradeValidationError(f"limit_price is required when price_type={price_type}.")
        if price_type in {"STOP", "STOP_LIMIT"} and stop_price is None:
            raise ETradeValidationError(f"stop_price is required when price_type={price_type}.")

        normalized_order: dict[str, Any] = {
            "account_key": account_key,
            "asset_type": asset_type,
            "side": side,
            "quantity": quantity_text,
            "price_type": price_type,
            "limit_price": _format_number(limit_price, default="0"),
            "stop_price": _format_number(stop_price, default="0"),
            "term": order_term,
            "session": market_session,
            "all_or_none": bool(order.get("all_or_none", False)),
            "symbol": None,
            "option": None,
        }

        if asset_type == "equity":
            symbol = str(order.get("symbol") or "").strip().upper()
            if not symbol:
                raise ETradeValidationError("symbol is required for equity orders.")
            normalized_order["symbol"] = symbol
            return normalized_order

        option = order.get("option")
        if not isinstance(option, Mapping):
            raise ETradeValidationError("option details are required for option orders.")
        symbol = str(option.get("symbol") or "").strip().upper()
        call_put = str(option.get("call_put") or "").strip().upper()
        if not symbol:
            raise ETradeValidationError("option.symbol is required.")
        if call_put not in {"CALL", "PUT"}:
            raise ETradeValidationError("option.call_put must be CALL or PUT.")

        try:
            expiry_year = int(option.get("expiry_year"))
            expiry_month = int(option.get("expiry_month"))
            expiry_day = int(option.get("expiry_day"))
        except (TypeError, ValueError) as exc:
            raise ETradeValidationError("Option expiry fields must be integers.") from exc

        normalized_order["option"] = {
            "symbol": symbol,
            "call_put": call_put,
            "expiry_year": expiry_year,
            "expiry_month": expiry_month,
            "expiry_day": expiry_day,
            "strike_price": _format_number(option.get("strike_price"), default=""),
        }
        if not normalized_order["option"]["strike_price"]:
            raise ETradeValidationError("option.strike_price is required.")
        return normalized_order

    def _build_provider_order_request(
        self,
        *,
        normalized_order: Mapping[str, Any],
        root_key: str,
        client_order_id: str,
        preview_id: Optional[str],
    ) -> dict[str, Any]:
        asset_type = str(normalized_order["asset_type"])
        quantity = str(normalized_order["quantity"])
        if asset_type == "equity":
            product = {
                "securityType": "EQ",
                "symbol": str(normalized_order["symbol"]),
            }
            instrument = {
                "Product": product,
                "orderAction": str(normalized_order["side"]),
                "quantityType": "QUANTITY",
                "quantity": quantity,
            }
            order_type = "EQ"
        else:
            option = normalized_order["option"]
            assert isinstance(option, Mapping)
            product = {
                "symbol": str(option["symbol"]),
                "securityType": "OPTN",
                "callPut": str(option["call_put"]),
                "expiryYear": str(option["expiry_year"]),
                "expiryMonth": str(option["expiry_month"]).zfill(2),
                "expiryDay": str(option["expiry_day"]).zfill(2),
                "strikePrice": str(option["strike_price"]),
            }
            instrument = {
                "Product": product,
                "orderAction": str(normalized_order["side"]),
                "orderedQuantity": quantity,
                "quantity": quantity,
            }
            order_type = "OPTN"

        body: dict[str, Any] = {
            root_key: {
                "orderType": order_type,
                "clientOrderId": client_order_id,
                "Order": [
                    {
                        "allOrNone": str(bool(normalized_order["all_or_none"])).lower(),
                        "priceType": str(normalized_order["price_type"]),
                        "limitPrice": str(normalized_order["limit_price"]),
                        "stopPrice": str(normalized_order["stop_price"]),
                        "orderTerm": str(normalized_order["term"]),
                        "marketSession": str(normalized_order["session"]),
                        "Instrument": [instrument],
                    }
                ],
            }
        }
        if preview_id is not None:
            body[root_key]["PreviewIds"] = [{"previewId": preview_id}]
        return body

    def _extract_symbol(self, normalized_order: Mapping[str, Any]) -> Optional[str]:
        asset_type = str(normalized_order.get("asset_type") or "")
        if asset_type == "equity":
            symbol = str(normalized_order.get("symbol") or "").strip().upper()
            return symbol or None
        option = normalized_order.get("option")
        if isinstance(option, Mapping):
            symbol = str(option.get("symbol") or "").strip().upper()
            return symbol or None
        return None

    def _log_audit_event(
        self,
        *,
        action: str,
        subject: Optional[str],
        environment: ETradeEnvironment,
        account_key: str,
        symbol: Optional[str],
        preview_id: Optional[str],
        order_id: Optional[str],
        provider_payload: Mapping[str, Any],
    ) -> None:
        logger.info(
            "etrade_audit action=%s subject=%s environment=%s account_hash=%s symbol=%s preview_id=%s order_id=%s provider_codes=%s",
            action,
            subject or "-",
            environment,
            _hash_account_key(account_key),
            symbol or "-",
            preview_id or "-",
            order_id or "-",
            ",".join(_collect_message_codes(provider_payload)) or "-",
        )


__all__ = [
    "ETradeEnvironment",
    "ETradeGateway",
]
