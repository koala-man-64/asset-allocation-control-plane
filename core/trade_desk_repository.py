from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountListResponse,
    TradeAccountSummary,
    TradeDeskAuditEvent,
    TradeDeskAuditEventListResponse,
    TradeOrder,
    TradeOrderHistoryResponse,
    TradePosition,
    TradePositionListResponse,
)
from asset_allocation_runtime_common.foundation.postgres import connect


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def stable_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _json_payload(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


@dataclass(frozen=True)
class TradeAccountRecord:
    account: TradeAccountSummary
    detail: TradeAccountDetail
    providerAccountKey: str | None


@dataclass(frozen=True)
class IdempotencyRecord:
    requestHash: str
    responsePayload: dict[str, Any]


class TradeDeskRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def list_accounts(self) -> TradeAccountListResponse:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT account_payload
                    FROM core.trade_accounts
                    WHERE enabled = true
                    ORDER BY name, account_id
                    """
                )
                rows = cur.fetchall()
        accounts = [TradeAccountSummary.model_validate(_json_payload(row[0])) for row in rows]
        return TradeAccountListResponse(accounts=accounts, generatedAt=utc_now())

    def get_account_record(self, account_id: str) -> TradeAccountRecord | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT account_payload, detail_payload, provider_account_key
                    FROM core.trade_accounts
                    WHERE account_id = %s AND enabled = true
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
        if not row:
            return None

        account = TradeAccountSummary.model_validate(_json_payload(row[0]))
        detail_payload = _json_payload(row[1])
        detail = TradeAccountDetail.model_validate(detail_payload) if detail_payload else TradeAccountDetail(account=account)
        provider_account_key = str(row[2]).strip() if row[2] else None
        return TradeAccountRecord(account=account, detail=detail, providerAccountKey=provider_account_key or None)

    def list_positions(self, account_id: str) -> TradePositionListResponse:
        record = self.get_account_record(account_id)
        if record is None:
            raise LookupError(f"Trade account '{account_id}' not found.")

        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT position_payload
                    FROM core.trade_positions
                    WHERE account_id = %s
                    ORDER BY symbol
                    """,
                    (account_id,),
                )
                rows = cur.fetchall()
        positions = [TradePosition.model_validate(_json_payload(row[0])) for row in rows]
        return TradePositionListResponse(
            accountId=account_id,
            positions=positions,
            generatedAt=utc_now(),
            freshness=record.account.freshness,
        )

    def list_orders(self, account_id: str, *, include_terminal: bool = False) -> TradeOrderHistoryResponse:
        record = self.get_account_record(account_id)
        if record is None:
            raise LookupError(f"Trade account '{account_id}' not found.")

        terminal = ("filled", "cancelled", "rejected", "expired")
        predicate = "" if include_terminal else "AND status <> ALL(%s)"
        params: tuple[Any, ...] = (account_id,) if include_terminal else (account_id, list(terminal))
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT order_payload
                    FROM core.trade_orders
                    WHERE account_id = %s
                    {predicate}
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT 500
                    """,
                    params,
                )
                rows = cur.fetchall()
        orders = [TradeOrder.model_validate(_json_payload(row[0])) for row in rows]
        return TradeOrderHistoryResponse(accountId=account_id, orders=orders, generatedAt=utc_now())

    def get_order(self, account_id: str, order_id: str) -> TradeOrder | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_payload
                    FROM core.trade_orders
                    WHERE account_id = %s AND order_id = %s
                    """,
                    (account_id, order_id),
                )
                row = cur.fetchone()
        if not row:
            return None
        return TradeOrder.model_validate(_json_payload(row[0]))

    def list_history(self, account_id: str) -> TradeOrderHistoryResponse:
        return self.list_orders(account_id, include_terminal=True)

    def list_audit_events(self, account_id: str, *, limit: int = 100) -> TradeDeskAuditEventListResponse:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT event_payload
                    FROM core.trade_desk_audit_events
                    WHERE account_id = %s
                    ORDER BY occurred_at DESC, event_id DESC
                    LIMIT %s
                    """,
                    (account_id, limit),
                )
                rows = cur.fetchall()
        events = [TradeDeskAuditEvent.model_validate(_json_payload(row[0])) for row in rows]
        return TradeDeskAuditEventListResponse(accountId=account_id, events=events, generatedAt=utc_now())

    def save_order(
        self,
        order: TradeOrder,
        *,
        request_payload: dict[str, Any] | None,
        response_payload: dict[str, Any] | None,
        request_hash: str | None,
    ) -> None:
        payload = order.model_dump(mode="json")
        now = utc_now()
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.trade_orders (
                        order_id,
                        account_id,
                        provider,
                        environment,
                        status,
                        symbol,
                        side,
                        client_request_id,
                        idempotency_key,
                        provider_order_id,
                        request_hash,
                        request_payload,
                        response_payload,
                        order_payload,
                        reconciliation_required,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        provider_order_id = EXCLUDED.provider_order_id,
                        response_payload = EXCLUDED.response_payload,
                        order_payload = EXCLUDED.order_payload,
                        reconciliation_required = EXCLUDED.reconciliation_required,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        order.orderId,
                        order.accountId,
                        order.provider,
                        order.environment,
                        order.status,
                        order.symbol,
                        order.side,
                        order.clientRequestId,
                        order.idempotencyKey,
                        order.providerOrderId,
                        request_hash,
                        _json_dumps(request_payload or {}),
                        _json_dumps(response_payload or {}),
                        _json_dumps(payload),
                        order.reconciliationRequired,
                        now,
                        now,
                    ),
                )

    def get_idempotency(self, account_id: str, action: str, idempotency_key: str) -> IdempotencyRecord | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT request_hash, response_payload
                    FROM core.trade_order_idempotency
                    WHERE account_id = %s AND action = %s AND idempotency_key = %s
                    """,
                    (account_id, action, idempotency_key),
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                        UPDATE core.trade_order_idempotency
                        SET replay_count = replay_count + 1, last_replayed_at = %s
                        WHERE account_id = %s AND action = %s AND idempotency_key = %s
                        """,
                        (utc_now(), account_id, action, idempotency_key),
                    )
        if not row:
            return None
        return IdempotencyRecord(requestHash=str(row[0]), responsePayload=_json_payload(row[1]) or {})

    def save_idempotency(
        self,
        *,
        account_id: str,
        action: str,
        idempotency_key: str,
        request_hash: str,
        actor: str | None,
        response_payload: dict[str, Any],
        provider_order_id: str | None,
    ) -> None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.trade_order_idempotency (
                        account_id,
                        action,
                        idempotency_key,
                        request_hash,
                        actor,
                        response_payload,
                        provider_order_id,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (account_id, action, idempotency_key) DO NOTHING
                    """,
                    (
                        account_id,
                        action,
                        idempotency_key,
                        request_hash,
                        actor,
                        _json_dumps(response_payload),
                        provider_order_id,
                        utc_now(),
                    ),
                )

    def save_audit_event(self, event: TradeDeskAuditEvent) -> None:
        payload = event.model_dump(mode="json")
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.trade_desk_audit_events (
                        event_id,
                        account_id,
                        order_id,
                        provider,
                        environment,
                        event_type,
                        severity,
                        actor,
                        client_request_id,
                        idempotency_key,
                        status_before,
                        status_after,
                        event_payload,
                        occurred_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        event.eventId,
                        event.accountId,
                        event.orderId,
                        event.provider,
                        event.environment,
                        event.eventType,
                        event.severity,
                        event.actor,
                        event.clientRequestId,
                        event.idempotencyKey,
                        event.statusBefore,
                        event.statusAfter,
                        _json_dumps(payload),
                        event.occurredAt,
                    ),
                )


def new_trade_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"
