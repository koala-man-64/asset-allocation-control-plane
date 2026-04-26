from __future__ import annotations

import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from asset_allocation_contracts.notifications import (
    CreateNotificationRequest,
    NotificationDecisionRequest,
    NotificationDeliveryResult,
    NotificationRecipient,
    TradeApprovalPayload,
)
from asset_allocation_contracts.trade_desk import TradeOrder, TradeOrderPlaceResponse, TradeOrderPreviewRequest

from api.service.notification_delivery import DeliverySendResult
from api.service.notification_service import NotificationError, NotificationService
from api.service.settings import NotificationSettings
from api.service.trade_desk_service import TradeDeskError
from core.notification_repository import (
    NotificationDeliveryAttemptRecord,
    NotificationRequestRecord,
    NotificationTokenRecord,
)
from core.trade_desk_repository import stable_hash


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _trade_order() -> TradeOrderPreviewRequest:
    return TradeOrderPreviewRequest(
        accountId="acct-paper",
        environment="paper",
        clientRequestId="trade-client-1",
        symbol="msft",
        side="buy",
        orderType="limit",
        quantity=10,
        limitPrice=100,
    )


def _message_payload(*, description: str = "Daily rebalance finished.") -> CreateNotificationRequest:
    return CreateNotificationRequest(
        sourceRepo="asset-allocation-jobs",
        sourceSystem="rebalance-job",
        clientRequestId="client-message-1",
        idempotencyKey="notification-idem-message-0001",
        kind="message",
        title="Rebalance complete",
        description=description,
        targetUrl="https://app.example.com/runs/run-1",
        recipients=[
            NotificationRecipient(
                recipientId="pm",
                displayName="PM",
                email="pm@example.com",
                phoneNumber="+15555550100",
                channels=["email", "sms"],
            )
        ],
    )


def _trade_payload(*, expires_at: datetime | None = None) -> CreateNotificationRequest:
    order = _trade_order()
    return CreateNotificationRequest(
        sourceRepo="asset-allocation-jobs",
        sourceSystem="rebalance-job",
        clientRequestId="client-trade-1",
        idempotencyKey="notification-idem-trade-0001",
        kind="trade_approval",
        title="Approve MSFT buy",
        description="Buy 10 shares of MSFT for the core paper account.",
        targetUrl="https://app.example.com/trade-desk/previews/preview-1",
        recipients=[
            NotificationRecipient(
                recipientId="pm",
                displayName="PM",
                email="pm@example.com",
                phoneNumber="+15555550100",
                channels=["email"],
            )
        ],
        expiresAt=expires_at,
        tradeApproval=TradeApprovalPayload(
            accountId="acct-paper",
            previewId="preview-1",
            orderHash=stable_hash(order.model_dump(mode="json")),
            placeIdempotencyKey="place-idem-0000001",
            order=order,
        ),
    )


class FakeNotificationRepository:
    def __init__(self) -> None:
        self.requests: dict[str, NotificationRequestRecord] = {}
        self.recipients: dict[str, list[dict[str, Any]]] = {}
        self.tokens_by_id: dict[str, NotificationTokenRecord] = {}
        self.tokens_by_hash: dict[str, NotificationTokenRecord] = {}
        self.attempts: dict[str, NotificationDeliveryAttemptRecord] = {}
        self.idempotency: dict[tuple[str, str], str] = {}
        self.audit_events: list[dict[str, Any]] = []

    def get_by_idempotency(self, source_repo: str, idempotency_key: str) -> NotificationRequestRecord | None:
        request_id = self.idempotency.get((source_repo, idempotency_key))
        return self.requests.get(request_id or "")

    def get_request(self, request_id: str) -> NotificationRequestRecord | None:
        return self.requests.get(request_id)

    def create_request(
        self,
        *,
        record: NotificationRequestRecord,
        recipients: list[dict[str, Any]],
        tokens: list[dict[str, Any]],
        delivery_attempts: list[dict[str, Any]],
    ) -> None:
        self.requests[record.requestId] = record
        self.idempotency[(record.sourceRepo, record.idempotencyKey)] = record.requestId
        self.recipients[record.requestId] = recipients
        for token in tokens:
            token_record = NotificationTokenRecord(
                tokenId=token["tokenId"],
                requestId=record.requestId,
                recipientId=token["recipientId"],
                tokenHash=token["tokenHash"],
                expiresAt=token.get("expiresAt"),
                viewedAt=None,
                usedAt=None,
                createdAt=record.createdAt,
            )
            self._store_token(token_record)
        for attempt in delivery_attempts:
            self.attempts[attempt["attemptId"]] = NotificationDeliveryAttemptRecord(
                attemptId=attempt["attemptId"],
                requestId=record.requestId,
                recipientId=attempt["recipientId"],
                channel=attempt["channel"],
                address=attempt["address"],
                status="pending",
                provider=None,
                providerMessageId=None,
                attemptNumber=attempt.get("attemptNumber", 1),
                sanitizedError=None,
                attemptedAt=record.createdAt,
            )

    def list_pending_delivery_attempts(self, request_id: str) -> list[NotificationDeliveryAttemptRecord]:
        return [
            attempt
            for attempt in self.attempts.values()
            if attempt.requestId == request_id and attempt.status == "pending"
        ]

    def update_delivery_attempt(
        self,
        *,
        attempt_id: str,
        status: str,
        provider: str | None,
        provider_message_id: str | None,
        sanitized_error: str | None,
        attempted_at: datetime,
    ) -> None:
        self.attempts[attempt_id] = replace(
            self.attempts[attempt_id],
            status=status,
            provider=provider,
            providerMessageId=provider_message_id,
            sanitizedError=sanitized_error,
            attemptedAt=attempted_at,
        )

    def list_delivery_results(self, request_id: str) -> list[NotificationDeliveryResult]:
        return [
            NotificationDeliveryResult(
                recipientId=attempt.recipientId,
                channel=attempt.channel,  # type: ignore[arg-type]
                address=attempt.address,
                status=attempt.status,  # type: ignore[arg-type]
                provider=attempt.provider,
                providerMessageId=attempt.providerMessageId,
                attemptedAt=attempt.attemptedAt,
                sanitizedError=attempt.sanitizedError,
            )
            for attempt in self.attempts.values()
            if attempt.requestId == request_id
        ]

    def get_token_by_hash(self, token_hash: str) -> NotificationTokenRecord | None:
        return self.tokens_by_hash.get(token_hash)

    def mark_token_viewed(self, token_id: str, viewed_at: datetime) -> None:
        token = self.tokens_by_id[token_id]
        if token.viewedAt is None:
            self._store_token(replace(token, viewedAt=viewed_at))

    def mark_token_used(self, token_id: str, used_at: datetime) -> bool:
        token = self.tokens_by_id[token_id]
        if token.usedAt is not None:
            return False
        self._store_token(replace(token, usedAt=used_at))
        return True

    def update_request_status(self, request_id: str, status: str, updated_at: datetime) -> None:
        self.requests[request_id] = replace(self.requests[request_id], status=status, updatedAt=updated_at)

    def update_decision(
        self,
        *,
        request_id: str,
        status: str,
        decision_status: str,
        decision: str | None,
        decided_at: datetime,
        decided_by: str,
    ) -> None:
        self.requests[request_id] = replace(
            self.requests[request_id],
            status=status,
            decisionStatus=decision_status,
            decision=decision,
            decidedAt=decided_at,
            decidedBy=decided_by,
            updatedAt=decided_at,
        )

    def update_execution(
        self,
        *,
        request_id: str,
        execution_status: str,
        execution_order_id: str | None,
        execution_message: str | None,
        updated_at: datetime,
    ) -> None:
        self.requests[request_id] = replace(
            self.requests[request_id],
            executionStatus=execution_status,
            executionOrderId=execution_order_id,
            executionMessage=execution_message,
            updatedAt=updated_at,
        )

    def save_audit_event(
        self,
        *,
        event_id: str,
        request_id: str,
        token_id: str | None,
        event_type: str,
        actor: str | None,
        summary: str,
        payload: dict[str, Any],
        occurred_at: datetime,
    ) -> None:
        self.audit_events.append(
            {
                "eventId": event_id,
                "requestId": request_id,
                "tokenId": token_id,
                "eventType": event_type,
                "actor": actor,
                "summary": summary,
                "payload": payload,
                "occurredAt": occurred_at,
            }
        )

    def _store_token(self, token: NotificationTokenRecord) -> None:
        self.tokens_by_id[token.tokenId] = token
        self.tokens_by_hash[token.tokenHash] = token


class FakeDeliveryClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    def send_email(self, *, to: str, subject: str, body: str) -> DeliverySendResult:
        self.sent.append({"channel": "email", "to": to, "subject": subject, "body": body})
        return DeliverySendResult(provider="fake-email", providerMessageId=f"email-{len(self.sent)}")

    def send_sms(self, *, to: str, body: str) -> DeliverySendResult:
        self.sent.append({"channel": "sms", "to": to, "subject": "", "body": body})
        return DeliverySendResult(provider="fake-sms", providerMessageId=f"sms-{len(self.sent)}")


class FakeTradeDeskService:
    def __init__(self, *, fail: TradeDeskError | None = None) -> None:
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    def place_order(self, account_id: str, payload, *, actor: str | None) -> TradeOrderPlaceResponse:
        self.calls.append({"accountId": account_id, "payload": payload, "actor": actor})
        if self.fail is not None:
            raise self.fail
        now = _now()
        order = TradeOrder(
            orderId="order-1",
            accountId=payload.accountId,
            provider="alpaca",
            environment=payload.environment,
            status="accepted",
            symbol=payload.symbol,
            side=payload.side,
            orderType=payload.orderType,
            timeInForce=payload.timeInForce,
            quantity=payload.quantity,
            notional=payload.notional,
            limitPrice=payload.limitPrice,
            createdAt=now,
            updatedAt=now,
        )
        return TradeOrderPlaceResponse(order=order, submitted=True, replayed=False, message="accepted")


def _service(
    repo: FakeNotificationRepository,
    delivery: FakeDeliveryClient,
    trade_desk: FakeTradeDeskService | None = None,
) -> NotificationService:
    trade_desk = trade_desk or FakeTradeDeskService()
    return NotificationService(
        repo,  # type: ignore[arg-type]
        NotificationSettings(app_base_url="https://app.example.com", token_hash_secret="test-secret"),
        delivery,  # type: ignore[arg-type]
        lambda: trade_desk,  # type: ignore[return-value]
    )


def _token_from_delivery(delivery: FakeDeliveryClient) -> str:
    match = re.search(r"/notifications/actions/([A-Za-z0-9_-]+)", delivery.sent[0]["body"])
    assert match is not None
    return match.group(1)


def test_message_notification_sends_without_action_tokens() -> None:
    repo = FakeNotificationRepository()
    delivery = FakeDeliveryClient()

    status = _service(repo, delivery).create_notification(_message_payload(), actor="jobs-service")

    assert status.kind == "message"
    assert status.status == "delivered"
    assert status.decisionStatus == "not_required"
    assert status.executionStatus == "not_applicable"
    assert status.tradeApproval is None
    assert len(status.deliveries) == 2
    assert len(repo.tokens_by_id) == 0
    assert "https://app.example.com/runs/run-1" in delivery.sent[0]["body"]


def test_idempotent_create_replays_same_request_and_rejects_conflicting_payload() -> None:
    repo = FakeNotificationRepository()
    delivery = FakeDeliveryClient()
    service = _service(repo, delivery)
    payload = _message_payload()

    first = service.create_notification(payload, actor="jobs-service")
    replay = service.create_notification(payload, actor="jobs-service")

    assert replay.requestId == first.requestId
    assert len(delivery.sent) == 2

    with pytest.raises(NotificationError) as exc:
        service.create_notification(
            _message_payload(description="Different body with same idempotency key."),
            actor="jobs-service",
        )
    assert exc.value.status_code == 409


def test_magic_link_detail_hashes_token_and_deny_never_places_trade() -> None:
    repo = FakeNotificationRepository()
    delivery = FakeDeliveryClient()
    trade_desk = FakeTradeDeskService()
    service = _service(repo, delivery, trade_desk)

    created = service.create_notification(_trade_payload(), actor="jobs-service")
    raw_token = _token_from_delivery(delivery)
    token_record = next(iter(repo.tokens_by_id.values()))

    assert created.decisionStatus == "pending"
    assert raw_token not in token_record.tokenHash

    detail = service.get_action_detail(raw_token)
    denied = service.decide(raw_token, NotificationDecisionRequest(decision="deny", reason="Outside risk budget."))

    assert detail.tradeApproval is not None
    assert detail.tradeApproval.symbol == "MSFT"
    assert denied.decisionStatus == "denied"
    assert denied.executionStatus == "not_applicable"
    assert denied.executionMessage == "Trade approval was denied."
    assert trade_desk.calls == []
    assert any(event["eventType"] == "viewed" and event["tokenId"] == token_record.tokenId for event in repo.audit_events)


def test_approve_releases_trade_and_token_is_one_time() -> None:
    repo = FakeNotificationRepository()
    delivery = FakeDeliveryClient()
    trade_desk = FakeTradeDeskService()
    service = _service(repo, delivery, trade_desk)

    service.create_notification(_trade_payload(), actor="jobs-service")
    raw_token = _token_from_delivery(delivery)
    approved = service.decide(raw_token, NotificationDecisionRequest(decision="approve", reason="Approved."))

    assert approved.decisionStatus == "approved"
    assert approved.executionStatus == "submitted"
    assert approved.executionOrderId == "order-1"
    assert len(trade_desk.calls) == 1
    assert trade_desk.calls[0]["payload"].idempotencyKey == "place-idem-0000001"
    assert trade_desk.calls[0]["payload"].previewId == "preview-1"

    with pytest.raises(NotificationError) as exc:
        service.decide(raw_token, NotificationDecisionRequest(decision="approve"))
    assert exc.value.status_code == 404


def test_approved_trade_blocked_by_trade_desk_gate_records_blocked_execution() -> None:
    repo = FakeNotificationRepository()
    delivery = FakeDeliveryClient()
    trade_desk = FakeTradeDeskService(fail=TradeDeskError(403, "Global trade desk kill switch is active."))
    service = _service(repo, delivery, trade_desk)

    service.create_notification(_trade_payload(), actor="jobs-service")
    blocked = service.decide(_token_from_delivery(delivery), NotificationDecisionRequest(decision="approve"))

    assert blocked.decisionStatus == "approved"
    assert blocked.executionStatus == "blocked"
    assert blocked.executionMessage == "Global trade desk kill switch is active."
    assert len(trade_desk.calls) == 1


def test_expired_token_returns_non_revealing_error_and_polling_marks_expired() -> None:
    repo = FakeNotificationRepository()
    delivery = FakeDeliveryClient()
    service = _service(repo, delivery)

    created = service.create_notification(_trade_payload(expires_at=_now() - timedelta(minutes=1)), actor="jobs-service")

    assert created.status == "expired"
    assert created.decisionStatus == "expired"
    assert created.decision is None

    with pytest.raises(NotificationError) as exc:
        service.get_action_detail(_token_from_delivery(delivery))
    assert exc.value.status_code == 404
    assert exc.value.detail == "Notification action is unavailable."
