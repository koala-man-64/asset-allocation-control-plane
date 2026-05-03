from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountActionResponse,
    BrokerAccountActionStatus,
    BrokerAccountActionType,
    BrokerConnectionHealth,
)
from asset_allocation_contracts.trade_desk import (
    TradeAccountSummary,
    TradeDataFreshness,
    TradeOrder,
    TradePosition,
)

from api.service.alpaca_gateway import AlpacaGateway
from api.service.etrade_gateway import ETradeGateway
from api.service.schwab_gateway import SchwabGateway
from api.service.settings import BrokerAccountStatusRefreshSettings
from core.trade_desk_repository import TradeAccountRecord, TradeDeskRepository, utc_now

logger = logging.getLogger("asset-allocation.api.broker_account_refresh")

RefreshTrigger = Literal["scheduled", "manual", "reconnect"]

_CONNECTIVITY_REASON_MARKERS = (
    "auth",
    "connect",
    "credential",
    "oauth",
    "reauth",
    "reconnect",
    "session",
    "token",
)


@dataclass(frozen=True)
class BrokerAccountRefreshOutcome:
    account: TradeAccountSummary
    status: BrokerAccountActionStatus
    message: str
    refreshed: bool


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        return [value]
    return []


def _get_value(payload: Any, *keys: str) -> Any:
    for key in keys:
        if isinstance(payload, Mapping) and key in payload:
            value = payload.get(key)
            if value is not None and value != "":
                return value
        value = getattr(payload, key, None)
        if value is not None and value != "":
            return value

    lower_keys = {key.lower() for key in keys}
    if isinstance(payload, Mapping):
        for actual_key, value in payload.items():
            if str(actual_key).lower() in lower_keys and value is not None and value != "":
                return value
    return None


def _first_present(payload: Any, *keys: str) -> Any:
    snake_keys: list[str] = []
    for key in keys:
        snake_keys.append(key)
        snake_keys.append(_camel_to_snake(key))
    return _get_value(payload, *snake_keys)


def _camel_to_snake(value: str) -> str:
    result: list[str] = []
    for char in value:
        if char.isupper() and result:
            result.append("_")
        result.append(char.lower())
    return "".join(result)


def _to_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if text.startswith("$"):
        text = text[1:]
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def _to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _mask_account(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) <= 4:
        return f"***{text}"
    return f"***{text[-4:]}"


def _summarize_error(exc: Exception) -> str:
    detail = str(exc).strip() or type(exc).__name__
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        detail = f"status={status_code} {detail}"
    if len(detail) > 240:
        detail = f"{detail[:237]}..."
    return detail


def _is_connectivity_reason(value: str | None) -> bool:
    text = str(value or "").lower()
    return any(marker in text for marker in _CONNECTIVITY_REASON_MARKERS)


def _collect_named_nodes(payload: Any, *names: str) -> list[Mapping[str, Any]]:
    matches: list[Mapping[str, Any]] = []
    target_names = {name.lower() for name in names}

    def visit(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if str(key).lower() in target_names:
                    for item in _as_list(value):
                        mapping = _as_mapping(item)
                        if mapping:
                            matches.append(mapping)
                visit(value)
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                visit(item)

    visit(payload)
    return matches


def _order_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return {
        "new": "accepted",
        "pending_new": "submitted",
        "pending": "submitted",
        "open": "accepted",
        "submitted": "submitted",
        "accepted": "accepted",
        "working": "accepted",
        "partially_filled": "partially_filled",
        "partial": "partially_filled",
        "filled": "filled",
        "done_for_day": "accepted",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "cancel_pending": "cancel_pending",
        "rejected": "rejected",
        "expired": "expired",
    }.get(normalized, "unknown_reconcile_required")


def _order_side(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return "sell" if "sell" in normalized else "buy"


def _order_type(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("_", " ")
    if "stop" in normalized and "limit" in normalized:
        return "stop_limit"
    if "stop" in normalized:
        return "stop"
    if "limit" in normalized:
        return "limit"
    return "market"


def _time_in_force(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return {
        "good_for_day": "day",
        "day": "day",
        "gtc": "gtc",
        "good_until_cancel": "gtc",
        "opg": "opg",
        "cls": "cls",
        "ioc": "ioc",
        "immediate_or_cancel": "ioc",
        "fok": "fok",
        "fill_or_kill": "fok",
    }.get(normalized, "day")


def _asset_class(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"etf", "option", "crypto", "mutual_fund"}:
        return normalized
    if normalized in {"equity", "stock", "common_stock", "common stock"}:
        return "equity"
    return "unknown"


class BrokerAccountStatusRefreshService:
    def __init__(
        self,
        trade_repo: TradeDeskRepository,
        settings: BrokerAccountStatusRefreshSettings,
        *,
        alpaca_gateway: AlpacaGateway | None = None,
        etrade_gateway: ETradeGateway | None = None,
        schwab_gateway: SchwabGateway | None = None,
    ) -> None:
        self._trade_repo = trade_repo
        self._settings = settings
        self._alpaca_gateway = alpaca_gateway
        self._etrade_gateway = etrade_gateway
        self._schwab_gateway = schwab_gateway

    def refresh_due_accounts(self, *, force: bool = False, trigger: RefreshTrigger = "scheduled") -> list[BrokerAccountRefreshOutcome]:
        outcomes: list[BrokerAccountRefreshOutcome] = []
        for record in self._trade_repo.list_account_records(limit=self._settings.batch_size):
            if not force and not self._is_due(record.account):
                continue
            outcomes.append(self.refresh_record(record, trigger=trigger))
        return outcomes

    def refresh_account(self, account_id: str, *, trigger: RefreshTrigger = "manual") -> BrokerAccountRefreshOutcome:
        record = self._trade_repo.get_account_record(account_id)
        if record is None:
            raise LookupError(f"Trade account '{account_id}' not found.")
        return self.refresh_record(record, trigger=trigger)

    def refresh_record(
        self,
        record: TradeAccountRecord,
        *,
        trigger: RefreshTrigger = "manual",
    ) -> BrokerAccountRefreshOutcome:
        account = record.account
        started_at = utc_now()
        with self._trade_repo.account_refresh_lock(account.accountId) as acquired:
            if not acquired:
                return BrokerAccountRefreshOutcome(
                    account=account,
                    status="in_progress",
                    message="Broker account refresh is already running.",
                    refreshed=False,
                )
            try:
                if account.provider == "alpaca":
                    outcome = self._refresh_alpaca(record, checked_at=started_at)
                elif account.provider == "etrade":
                    outcome = self._refresh_etrade(record, checked_at=started_at)
                else:
                    outcome = self._refresh_schwab(record, checked_at=started_at)
            except Exception as exc:
                message = _summarize_error(exc)
                logger.warning(
                    "Broker account refresh failed account_id=%s provider=%s environment=%s trigger=%s error=%s",
                    account.accountId,
                    account.provider,
                    account.environment,
                    trigger,
                    message,
                    exc_info=True,
                )
                outcome = self._save_unreadable_account(record, checked_at=started_at, message=message)

        logger.info(
            "Broker account refresh completed account_id=%s provider=%s environment=%s trigger=%s status=%s",
            outcome.account.accountId,
            outcome.account.provider,
            outcome.account.environment,
            trigger,
            outcome.status,
        )
        return outcome

    def action_response(
        self,
        *,
        account_id: str,
        action: BrokerAccountActionType,
        trigger: RefreshTrigger,
    ) -> BrokerAccountActionResponse:
        outcome = self.refresh_account(account_id, trigger=trigger)
        return BrokerAccountActionResponse(
            actionId=f"{action}-{uuid.uuid4().hex}",
            accountId=outcome.account.accountId,
            action=action,
            status=outcome.status,
            requestedAt=utc_now(),
            message=outcome.message,
            resultingConnectionHealth=self._connection_health(outcome.account),
            tradeReadiness=outcome.account.readiness,
            syncPaused=False,
        )

    def _is_due(self, account: TradeAccountSummary) -> bool:
        latest = self._latest_timestamp(
            account.lastSyncedAt,
            account.snapshotAsOf,
            account.freshness.balancesAsOf,
            account.freshness.positionsAsOf,
            account.freshness.ordersAsOf,
        )
        if latest is None:
            return True
        return latest <= utc_now() - timedelta(seconds=self._settings.stale_after_seconds)

    def _refresh_alpaca(
        self,
        record: TradeAccountRecord,
        *,
        checked_at: datetime,
    ) -> BrokerAccountRefreshOutcome:
        if self._alpaca_gateway is None:
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="Alpaca gateway is not initialized.",
            )
        account_payload = self._alpaca_gateway.get_account(
            environment=record.account.environment,
            subject="broker-account-refresh",
        )
        positions_payload = self._alpaca_gateway.list_positions(
            environment=record.account.environment,
            subject="broker-account-refresh",
        )
        orders_payload = self._alpaca_gateway.list_orders(
            environment=record.account.environment,
            subject="broker-account-refresh",
            status="open",
        )
        positions = self._positions_from_payload(record.account, positions_payload, checked_at=checked_at)
        orders = self._orders_from_payload(record.account, orders_payload, checked_at=checked_at)
        updated = self._connected_account(
            record.account,
            checked_at=checked_at,
            cash=_to_float(_first_present(account_payload, "cash")),
            buying_power=_to_float(_first_present(account_payload, "buyingPower", "buying_power")),
            equity=_to_float(_first_present(account_payload, "equity", "portfolioValue", "portfolio_value")),
            account_number_masked=_mask_account(_first_present(account_payload, "accountNumber", "account_number")),
            base_currency=str(_first_present(account_payload, "currency", "baseCurrency") or record.account.baseCurrency),
            position_count=len(positions),
            open_order_count=len(orders),
        )
        detail = record.detail.model_copy(update={"account": updated})
        self._trade_repo.save_account_snapshot(account=updated, detail=detail, positions=positions, orders=orders)
        return BrokerAccountRefreshOutcome(
            account=updated,
            status="completed",
            message="Broker account refreshed.",
            refreshed=True,
        )

    def _refresh_etrade(
        self,
        record: TradeAccountRecord,
        *,
        checked_at: datetime,
    ) -> BrokerAccountRefreshOutcome:
        if self._etrade_gateway is None:
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="E*TRADE gateway is not initialized.",
                reconnect_required=True,
            )
        session = self._etrade_gateway.get_session_state(environment=record.account.environment)
        if not session.get("configured"):
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message=f"E*TRADE {record.account.environment} credentials are not configured.",
            )
        if not session.get("connected"):
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message=f"E*TRADE {record.account.environment} OAuth session is not connected. Reconnect required.",
                reconnect_required=True,
            )
        if not record.providerAccountKey:
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="Configured E*TRADE account is missing provider_account_key.",
            )

        balance = self._etrade_gateway.get_balance(
            environment=record.account.environment,
            account_key=record.providerAccountKey,
            subject="broker-account-refresh",
        )
        portfolio = self._etrade_gateway.get_portfolio(
            environment=record.account.environment,
            account_key=record.providerAccountKey,
            subject="broker-account-refresh",
        )
        orders_payload = self._etrade_gateway.list_orders(
            environment=record.account.environment,
            account_key=record.providerAccountKey,
            subject="broker-account-refresh",
            status="OPEN",
        )
        positions = self._positions_from_payload(
            record.account,
            _collect_named_nodes(portfolio, "Position", "position"),
            checked_at=checked_at,
        )
        orders = self._orders_from_payload(
            record.account,
            _collect_named_nodes(orders_payload, "Order", "order"),
            checked_at=checked_at,
        )
        updated = self._connected_account(
            record.account,
            checked_at=checked_at,
            cash=self._etrade_cash(balance),
            buying_power=self._etrade_buying_power(balance),
            equity=self._etrade_equity(balance),
            position_count=len(positions),
            open_order_count=len(orders),
        )
        detail = record.detail.model_copy(update={"account": updated})
        self._trade_repo.save_account_snapshot(account=updated, detail=detail, positions=positions, orders=orders)
        return BrokerAccountRefreshOutcome(
            account=updated,
            status="completed",
            message="Broker account refreshed.",
            refreshed=True,
        )

    def _refresh_schwab(
        self,
        record: TradeAccountRecord,
        *,
        checked_at: datetime,
    ) -> BrokerAccountRefreshOutcome:
        if self._schwab_gateway is None:
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="Schwab gateway is not initialized.",
                reconnect_required=True,
            )
        session = self._schwab_gateway.get_session_state()
        if not session.get("configured"):
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="Schwab credentials are not configured.",
            )
        if not session.get("connected"):
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="Schwab OAuth session is not connected. Reconnect required.",
                reconnect_required=True,
            )
        if not record.providerAccountKey:
            return self._save_unreadable_account(
                record,
                checked_at=checked_at,
                message="Configured Schwab account is missing provider_account_key.",
            )

        balance = self._schwab_gateway.get_balance(
            account_number=record.providerAccountKey,
            subject="broker-account-refresh",
        )
        positions_payload = self._schwab_gateway.get_positions(
            account_number=record.providerAccountKey,
            subject="broker-account-refresh",
        )
        orders_payload = self._schwab_gateway.list_orders(
            account_number=record.providerAccountKey,
            subject="broker-account-refresh",
            status="WORKING",
        )
        positions = self._positions_from_payload(
            record.account,
            _collect_named_nodes(positions_payload, "positions", "position"),
            checked_at=checked_at,
        )
        orders = self._orders_from_payload(record.account, orders_payload, checked_at=checked_at)
        updated = self._connected_account(
            record.account,
            checked_at=checked_at,
            cash=self._schwab_balance(balance, "cashBalance", "cashAvailableForWithdrawal", "availableFunds"),
            buying_power=self._schwab_balance(balance, "buyingPower", "cashAvailableForTrading", "availableFunds"),
            equity=self._schwab_balance(balance, "liquidationValue", "accountValue", "equity"),
            position_count=len(positions),
            open_order_count=len(orders),
        )
        detail = record.detail.model_copy(update={"account": updated})
        self._trade_repo.save_account_snapshot(account=updated, detail=detail, positions=positions, orders=orders)
        return BrokerAccountRefreshOutcome(
            account=updated,
            status="completed",
            message="Broker account refreshed.",
            refreshed=True,
        )

    def _connected_account(
        self,
        account: TradeAccountSummary,
        *,
        checked_at: datetime,
        cash: float | None = None,
        buying_power: float | None = None,
        equity: float | None = None,
        account_number_masked: str | None = None,
        base_currency: str | None = None,
        position_count: int = 0,
        open_order_count: int = 0,
    ) -> TradeAccountSummary:
        readiness = account.readiness
        readiness_reason = account.readinessReason
        if account.readiness != "blocked" and _is_connectivity_reason(account.readinessReason):
            readiness = "ready"
            readiness_reason = None
        elif account.readiness == "review" and not account.readinessReason:
            readiness = "ready"

        capabilities = account.capabilities.model_copy(
            update={
                "canReadAccount": True,
                "canReadPositions": True,
                "canReadOrders": True,
                "canReadHistory": True,
                "unsupportedReason": None,
            }
        )
        return account.model_copy(
            update={
                "accountNumberMasked": account_number_masked or account.accountNumberMasked,
                "baseCurrency": str(base_currency or account.baseCurrency).upper(),
                "readiness": readiness,
                "readinessReason": readiness_reason,
                "capabilities": capabilities,
                "cash": cash if cash is not None else account.cash,
                "buyingPower": buying_power if buying_power is not None else account.buyingPower,
                "equity": equity if equity is not None else account.equity,
                "openOrderCount": max(0, open_order_count),
                "positionCount": max(0, position_count),
                "lastSyncedAt": checked_at,
                "snapshotAsOf": checked_at,
                "freshness": TradeDataFreshness(
                    balancesState="fresh",
                    positionsState="fresh",
                    ordersState="fresh",
                    balancesAsOf=checked_at,
                    positionsAsOf=checked_at,
                    ordersAsOf=checked_at,
                    maxAgeSeconds=self._settings.stale_after_seconds,
                ),
            }
        )

    def _save_unreadable_account(
        self,
        record: TradeAccountRecord,
        *,
        checked_at: datetime,
        message: str,
        reconnect_required: bool = False,
    ) -> BrokerAccountRefreshOutcome:
        account = self._unreadable_account(
            record.account,
            checked_at=checked_at,
            message=message,
            reconnect_required=reconnect_required,
        )
        detail = record.detail.model_copy(update={"account": account})
        self._trade_repo.save_account_snapshot(account=account, detail=detail)
        return BrokerAccountRefreshOutcome(
            account=account,
            status="completed" if reconnect_required else "failed",
            message=message,
            refreshed=True,
        )

    def _unreadable_account(
        self,
        account: TradeAccountSummary,
        *,
        checked_at: datetime,
        message: str,
        reconnect_required: bool,
    ) -> TradeAccountSummary:
        had_success = self._latest_timestamp(
            account.lastSyncedAt,
            account.freshness.balancesAsOf,
            account.freshness.positionsAsOf,
            account.freshness.ordersAsOf,
        )
        state = "stale" if had_success else "unknown"
        capabilities = account.capabilities.model_copy(
            update={
                "canReadAccount": False,
                "canReadPositions": False,
                "canReadOrders": False,
                "readOnly": True,
                "unsupportedReason": message,
            }
        )
        readiness_reason = message
        if reconnect_required and "reconnect" not in message.lower():
            readiness_reason = f"{message} Reconnect required."
        return account.model_copy(
            update={
                "readiness": "review" if account.readiness != "blocked" else account.readiness,
                "readinessReason": readiness_reason,
                "capabilities": capabilities,
                "snapshotAsOf": checked_at,
                "freshness": TradeDataFreshness(
                    balancesState=state,
                    positionsState=state,
                    ordersState=state,
                    maxAgeSeconds=self._settings.stale_after_seconds,
                    staleReason=readiness_reason,
                ),
            }
        )

    def _positions_from_payload(
        self,
        account: TradeAccountSummary,
        payload: Any,
        *,
        checked_at: datetime,
    ) -> list[TradePosition]:
        positions: list[TradePosition] = []
        for item in _as_list(payload):
            mapping = _as_mapping(item)
            raw = mapping or item
            symbol = str(_first_present(raw, "symbol", "instrumentSymbol") or "").strip().upper()
            if not symbol:
                instrument = _as_mapping(_first_present(raw, "instrument"))
                symbol = str(_first_present(instrument, "symbol") or "").strip().upper()
            if not symbol:
                continue
            try:
                positions.append(
                    TradePosition(
                        accountId=account.accountId,
                        symbol=symbol,
                        assetClass=_asset_class(_first_present(raw, "assetClass", "securityType")),
                        quantity=_to_float(_first_present(raw, "quantity", "qty", "longQuantity")) or 0.0,
                        marketValue=_to_float(_first_present(raw, "marketValue", "market_value")) or 0.0,
                        averageEntryPrice=_to_float(
                            _first_present(raw, "averageEntryPrice", "avgEntryPrice", "averagePrice")
                        ),
                        lastPrice=_to_float(_first_present(raw, "lastPrice", "currentPrice", "current_price")),
                        costBasis=_to_float(_first_present(raw, "costBasis", "cost_basis")),
                        unrealizedPnl=_to_float(
                            _first_present(raw, "unrealizedPnl", "unrealizedPL", "unrealized_pl")
                        ),
                        unrealizedPnlPercent=_to_float(
                            _first_present(raw, "unrealizedPnlPercent", "unrealizedPLPct", "unrealized_plpc")
                        ),
                        dayPnl=_to_float(_first_present(raw, "dayPnl", "unrealizedDayGain", "unrealized_intraday_pl")),
                        asOf=checked_at,
                    )
                )
            except ValueError:
                logger.debug("Skipping invalid broker position payload account_id=%s symbol=%s", account.accountId, symbol)
        return positions

    def _orders_from_payload(
        self,
        account: TradeAccountSummary,
        payload: Any,
        *,
        checked_at: datetime,
    ) -> list[TradeOrder]:
        orders: list[TradeOrder] = []
        for item in _as_list(payload):
            raw = _as_mapping(item) or item
            provider_order_id = str(_first_present(raw, "id", "orderId", "order_id") or "").strip()
            symbol = str(_first_present(raw, "symbol") or "").strip().upper()
            if not symbol:
                instrument = _as_mapping(_first_present(raw, "instrument"))
                symbol = str(_first_present(instrument, "symbol") or "").strip().upper()
            if not provider_order_id or not symbol:
                continue
            order_id = f"{account.accountId}:{provider_order_id}"[:128]
            try:
                orders.append(
                    TradeOrder(
                        orderId=order_id,
                        accountId=account.accountId,
                        provider=account.provider,
                        environment=account.environment,
                        status=_order_status(_first_present(raw, "status", "orderStatus")),
                        symbol=symbol,
                        side=_order_side(_first_present(raw, "side", "orderAction")),
                        orderType=_order_type(_first_present(raw, "type", "orderType", "priceType")),
                        timeInForce=_time_in_force(_first_present(raw, "timeInForce", "duration", "orderTerm")),
                        assetClass=_asset_class(_first_present(raw, "assetClass", "securityType")),
                        providerOrderId=provider_order_id,
                        quantity=_to_float(_first_present(raw, "qty", "quantity", "orderedQuantity")),
                        notional=_to_float(_first_present(raw, "notional")),
                        limitPrice=_to_float(_first_present(raw, "limitPrice", "limit_price")),
                        stopPrice=_to_float(_first_present(raw, "stopPrice", "stop_price")),
                        filledQuantity=_to_float(_first_present(raw, "filledQty", "filledQuantity")) or 0.0,
                        averageFillPrice=_to_float(_first_present(raw, "filledAvgPrice", "averageExecutionPrice")),
                        submittedAt=_to_datetime(_first_present(raw, "submittedAt", "enteredTime")),
                        createdAt=_to_datetime(_first_present(raw, "createdAt", "enteredTime")) or checked_at,
                        updatedAt=_to_datetime(_first_present(raw, "updatedAt", "closeTime")) or checked_at,
                    )
                )
            except ValueError:
                logger.debug(
                    "Skipping invalid broker order payload account_id=%s provider_order_id=%s",
                    account.accountId,
                    provider_order_id,
                )
        return orders

    @staticmethod
    def _etrade_balance_views(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        root = _as_mapping(payload.get("BalanceResponse") or payload)
        return [
            root,
            _as_mapping(root.get("Computed") or root.get("computed")),
            _as_mapping(root.get("accountBalance") or root.get("AccountBalance")),
            _as_mapping(root.get("Cash") or root.get("cash")),
            _as_mapping(root.get("Margin") or root.get("margin")),
        ]

    def _etrade_equity(self, payload: Mapping[str, Any]) -> float | None:
        return self._first_numeric_from_views(
            self._etrade_balance_views(payload),
            "netAccountValue",
            "totalAccountValue",
            "accountValue",
            "netLiquidation",
            "equity",
        )

    def _etrade_cash(self, payload: Mapping[str, Any]) -> float | None:
        return self._first_numeric_from_views(
            self._etrade_balance_views(payload),
            "cashBalance",
            "cashAvailableForWithdrawal",
            "cashAvailableForInvestment",
            "netCash",
            "availableCash",
        )

    def _etrade_buying_power(self, payload: Mapping[str, Any]) -> float | None:
        return self._first_numeric_from_views(
            self._etrade_balance_views(payload),
            "buyingPower",
            "marginBuyingPower",
            "cashBuyingPower",
            "cashAvailableForInvestment",
        )

    def _schwab_balance(self, payload: Mapping[str, Any], *keys: str) -> float | None:
        account_payload = _as_mapping(payload.get("securitiesAccount"))
        views = [
            payload,
            _as_mapping(payload.get("currentBalances")),
            _as_mapping(payload.get("initialBalances")),
            _as_mapping(account_payload.get("currentBalances")),
            _as_mapping(account_payload.get("initialBalances")),
        ]
        return self._first_numeric_from_views(views, *keys)

    @staticmethod
    def _first_numeric_from_views(views: list[Mapping[str, Any]], *keys: str) -> float | None:
        for view in views:
            value = _first_present(view, *keys)
            numeric = _to_float(value)
            if numeric is not None:
                return numeric
        return None

    @staticmethod
    def _sync_status(freshness: TradeDataFreshness) -> str:
        states = [freshness.balancesState, freshness.positionsState, freshness.ordersState]
        if all(state == "fresh" for state in states):
            return "fresh"
        if any(state == "stale" for state in states):
            return "stale"
        if all(state == "unknown" for state in states):
            return "never_synced"
        return "stale"

    def _connection_health(self, account: TradeAccountSummary) -> BrokerConnectionHealth:
        sync_status = self._sync_status(account.freshness)
        can_read = account.capabilities.canReadAccount
        latest = self._latest_timestamp(
            account.lastSyncedAt,
            account.snapshotAsOf,
            account.freshness.balancesAsOf,
            account.freshness.positionsAsOf,
            account.freshness.ordersAsOf,
        )
        reconnect_required = (not can_read) and _is_connectivity_reason(
            account.capabilities.unsupportedReason or account.readinessReason or account.freshness.staleReason
        )
        connection_state = "connected"
        if reconnect_required:
            connection_state = "reconnect_required"
        elif not can_read:
            connection_state = "disconnected"
        elif sync_status != "fresh":
            connection_state = "degraded"

        return BrokerConnectionHealth(
            overallStatus="healthy" if can_read and sync_status == "fresh" and account.readiness == "ready" else "warning",
            authStatus="reauth_required" if reconnect_required else "authenticated" if can_read else "not_connected",
            connectionState=connection_state,
            syncStatus=sync_status,
            lastCheckedAt=latest or utc_now(),
            lastSuccessfulSyncAt=latest if can_read and sync_status in {"fresh", "stale"} else None,
            lastFailedSyncAt=latest if not can_read else None,
            staleReason=account.freshness.staleReason if sync_status in {"stale", "never_synced"} else None,
            failureMessage=account.capabilities.unsupportedReason if not can_read else None,
            syncPaused=False,
        )

    @staticmethod
    def _latest_timestamp(*values: datetime | None) -> datetime | None:
        populated = [value for value in values if value is not None]
        return max(populated) if populated else None


async def run_broker_account_status_refresh_loop(
    service: BrokerAccountStatusRefreshService,
    settings: BrokerAccountStatusRefreshSettings,
    stop_event: asyncio.Event,
) -> None:
    logger.info(
        "Broker account status refresh worker starting interval_seconds=%s stale_after_seconds=%s batch_size=%s refresh_on_startup=%s",
        settings.interval_seconds,
        settings.stale_after_seconds,
        settings.batch_size,
        settings.refresh_on_startup,
    )
    if settings.refresh_on_startup:
        await asyncio.to_thread(service.refresh_due_accounts, force=True, trigger="scheduled")

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
        await asyncio.to_thread(service.refresh_due_accounts, trigger="scheduled")

    logger.info("Broker account status refresh worker stopped.")
