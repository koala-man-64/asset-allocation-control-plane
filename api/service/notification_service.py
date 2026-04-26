from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Any, Callable

from asset_allocation_contracts.notifications import (
    CreateNotificationRequest,
    NotificationActionDetailResponse,
    NotificationDecisionRequest,
    NotificationRecipient,
    NotificationStatusResponse,
    TradeApprovalDisplay,
    TradeApprovalPayload,
)
from asset_allocation_contracts.trade_desk import TradeOrderPlaceRequest

from api.service.notification_delivery import NotificationDeliveryClient
from api.service.settings import NotificationSettings
from api.service.trade_desk_service import TradeDeskError, TradeDeskService
from core.notification_repository import NotificationRepository, NotificationRequestRecord, NotificationTokenRecord
from core.trade_desk_repository import new_trade_id, stable_hash, utc_now


class NotificationError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class NotificationService:
    def __init__(
        self,
        repository: NotificationRepository,
        settings: NotificationSettings,
        delivery_client: NotificationDeliveryClient,
        trade_desk_factory: Callable[[], TradeDeskService],
    ) -> None:
        self._repo = repository
        self._settings = settings
        self._delivery = delivery_client
        self._trade_desk_factory = trade_desk_factory

    def create_notification(self, payload: CreateNotificationRequest, *, actor: str | None) -> NotificationStatusResponse:
        request_payload = payload.model_dump(mode="json")
        request_hash = stable_hash(request_payload)
        existing = self._repo.get_by_idempotency(payload.sourceRepo, payload.idempotencyKey)
        if existing is not None:
            if existing.requestHash != request_hash:
                raise NotificationError(409, "Idempotency key was already used for a different notification request.")
            return self.get_status(existing.requestId)

        now = utc_now()
        expires_at = payload.expiresAt
        if payload.kind == "trade_approval" and expires_at is None:
            expires_at = now + timedelta(seconds=self._settings.default_trade_approval_ttl_seconds)

        request_id = new_trade_id("notification")
        recipients = self._normalize_recipients(payload.recipients)
        raw_tokens: dict[str, str] = {}
        token_rows: list[dict[str, Any]] = []
        if payload.kind == "trade_approval":
            for recipient in recipients:
                raw_token = secrets.token_urlsafe(32)
                token_id = new_trade_id("notification-token")
                raw_tokens[recipient["recipientId"]] = raw_token
                token_rows.append(
                    {
                        "tokenId": token_id,
                        "recipientId": recipient["recipientId"],
                        "tokenHash": self._token_hash(raw_token),
                        "expiresAt": expires_at,
                    }
                )
        delivery_attempts = self._initial_delivery_attempts(recipients)

        trade_payload = payload.tradeApproval.model_dump(mode="json") if payload.tradeApproval else None
        if trade_payload is not None:
            trade_payload["computedOrderHash"] = stable_hash(payload.tradeApproval.order.model_dump(mode="json"))  # type: ignore[union-attr]

        decision_status = "pending" if payload.kind == "trade_approval" else "not_required"
        execution_status = "pending_approval" if payload.kind == "trade_approval" else "not_applicable"
        record = NotificationRequestRecord(
            requestId=request_id,
            sourceRepo=payload.sourceRepo,
            sourceSystem=payload.sourceSystem,
            clientRequestId=payload.clientRequestId,
            idempotencyKey=payload.idempotencyKey,
            requestHash=request_hash,
            kind=payload.kind,
            status="pending",
            title=payload.title,
            description=payload.description,
            targetUrl=payload.targetUrl,
            requestPayload=request_payload,
            tradeApprovalPayload=trade_payload,
            decisionStatus=decision_status,
            decision=None,
            decidedAt=None,
            decidedBy=None,
            executionStatus=execution_status,
            executionOrderId=None,
            executionMessage=None,
            expiresAt=expires_at,
            createdAt=now,
            updatedAt=now,
        )
        self._repo.create_request(
            record=record,
            recipients=recipients,
            tokens=token_rows,
            delivery_attempts=delivery_attempts,
        )
        self._audit(
            request_id=request_id,
            token_id=None,
            event_type="created",
            actor=actor,
            summary="Notification request created.",
            payload={"kind": payload.kind, "sourceRepo": payload.sourceRepo},
        )

        self._deliver(record, raw_tokens)
        return self.get_status(request_id)

    def get_status(self, request_id: str) -> NotificationStatusResponse:
        record = self._repo.get_request(request_id)
        if record is None:
            raise NotificationError(404, f"Notification request '{request_id}' not found.")
        record = self._expire_if_needed(record)
        return self._status_response(record)

    def get_action_detail(self, raw_token: str) -> NotificationActionDetailResponse:
        token, record = self._resolve_available_token(raw_token, allow_used=False)
        now = utc_now()
        if token.viewedAt is None:
            self._repo.mark_token_viewed(token.tokenId, now)
            self._audit(
                request_id=record.requestId,
                token_id=token.tokenId,
                event_type="viewed",
                actor=None,
                summary="Notification action link viewed.",
                payload={"recipientId": token.recipientId},
            )

        return NotificationActionDetailResponse(
            requestId=record.requestId,
            tokenId=token.tokenId,
            kind=record.kind,
            title=record.title,
            description=record.description,
            targetUrl=record.targetUrl,
            createdAt=record.createdAt,
            expiresAt=record.expiresAt,
            decisionStatus=record.decisionStatus,  # type: ignore[arg-type]
            executionStatus=record.executionStatus,  # type: ignore[arg-type]
            tradeApproval=self._trade_display(record),
        )

    def decide(self, raw_token: str, payload: NotificationDecisionRequest) -> NotificationStatusResponse:
        token, record = self._resolve_available_token(raw_token, allow_used=False)
        if record.kind != "trade_approval":
            raise NotificationError(400, "Notification action does not support approval decisions.")
        if record.decisionStatus != "pending":
            raise NotificationError(404, "Notification action is unavailable.")

        now = utc_now()
        if not self._repo.mark_token_used(token.tokenId, now):
            raise NotificationError(404, "Notification action is unavailable.")

        decision_status = "approved" if payload.decision == "approve" else "denied"
        actor = f"magic-link:{token.tokenId}"
        self._repo.update_decision(
            request_id=record.requestId,
            status="decided",
            decision_status=decision_status,
            decision=payload.decision,
            decided_at=now,
            decided_by=actor,
        )
        self._audit(
            request_id=record.requestId,
            token_id=token.tokenId,
            event_type=decision_status,
            actor=actor,
            summary=f"Notification action {decision_status}.",
            payload={"reason": payload.reason, "recipientId": token.recipientId},
        )

        if payload.decision == "approve":
            self._release_trade(record, actor=actor, decided_at=now)
        else:
            self._repo.update_execution(
                request_id=record.requestId,
                execution_status="not_applicable",
                execution_order_id=None,
                execution_message="Trade approval was denied.",
                updated_at=now,
            )
        return self.get_status(record.requestId)

    def _deliver(
        self,
        record: NotificationRequestRecord,
        raw_tokens: dict[str, str],
    ) -> None:
        sent = 0
        failed = 0
        for attempt in self._repo.list_pending_delivery_attempts(record.requestId):
            action_url = self._action_url(raw_tokens[attempt.recipientId]) if attempt.recipientId in raw_tokens else record.targetUrl
            body = self._message_body(record, action_url=action_url, channel=attempt.channel)
            attempted_at = utc_now()
            try:
                if attempt.channel == "email":
                    result = self._delivery.send_email(to=attempt.address, subject=record.title, body=body)
                else:
                    result = self._delivery.send_sms(to=attempt.address, body=body)
            except Exception as exc:
                failed += 1
                self._repo.update_delivery_attempt(
                    attempt_id=attempt.attemptId,
                    status="failed",
                    provider=self._settings.delivery_provider,
                    provider_message_id=None,
                    sanitized_error=self._sanitize_error(exc),
                    attempted_at=attempted_at,
                )
                self._audit(
                    request_id=record.requestId,
                    token_id=None,
                    event_type="delivery_failed",
                    actor=None,
                    summary="Notification delivery failed.",
                    payload={"recipientId": attempt.recipientId, "channel": attempt.channel},
                )
                continue

            sent += 1
            self._repo.update_delivery_attempt(
                attempt_id=attempt.attemptId,
                status="sent",
                provider=result.provider,
                provider_message_id=result.providerMessageId,
                sanitized_error=None,
                attempted_at=attempted_at,
            )
        status = "delivered" if sent > 0 else "delivery_failed"
        self._repo.update_request_status(record.requestId, status, utc_now())
        self._audit(
            request_id=record.requestId,
            token_id=None,
            event_type=status,
            actor=None,
            summary="Notification delivery completed.",
            payload={"sent": sent, "failed": failed},
        )

    def _release_trade(self, record: NotificationRequestRecord, *, actor: str, decided_at: datetime) -> None:
        try:
            trade_payload = self._trade_payload(record)
            computed_hash = stable_hash(trade_payload.order.model_dump(mode="json"))
            expected_hash = str((record.tradeApprovalPayload or {}).get("computedOrderHash") or "")
            if computed_hash != expected_hash:
                self._repo.update_execution(
                    request_id=record.requestId,
                    execution_status="blocked",
                    execution_order_id=None,
                    execution_message="Stored order intent hash does not match approval payload.",
                    updated_at=decided_at,
                )
                self._audit(
                    request_id=record.requestId,
                    token_id=None,
                    event_type="execution_blocked",
                    actor=actor,
                    summary="Trade release blocked by order hash mismatch.",
                    payload={"orderHash": trade_payload.orderHash},
                )
                return

            place_request = TradeOrderPlaceRequest(
                **trade_payload.order.model_dump(mode="python"),
                idempotencyKey=trade_payload.placeIdempotencyKey,
                previewId=trade_payload.previewId,
                confirmedAt=decided_at,
                orderHash=trade_payload.orderHash,
            )
            response = self._trade_desk_factory().place_order(trade_payload.accountId, place_request, actor=actor)
        except TradeDeskError as exc:
            execution_status = "release_failed" if exc.status_code >= 500 else "blocked"
            self._repo.update_execution(
                request_id=record.requestId,
                execution_status=execution_status,
                execution_order_id=None,
                execution_message=exc.detail,
                updated_at=utc_now(),
            )
            self._audit(
                request_id=record.requestId,
                token_id=None,
                event_type=f"execution_{execution_status}",
                actor=actor,
                summary="Trade release did not submit.",
                payload={"statusCode": exc.status_code, "detail": exc.detail},
            )
            return

        execution_status = "submitted" if response.submitted else "blocked"
        self._repo.update_execution(
            request_id=record.requestId,
            execution_status=execution_status,
            execution_order_id=response.order.orderId,
            execution_message=response.message,
            updated_at=utc_now(),
        )
        self._audit(
            request_id=record.requestId,
            token_id=None,
            event_type="execution_submitted" if response.submitted else "execution_blocked",
            actor=actor,
            summary=response.message or "Trade release completed.",
            payload={"submitted": response.submitted, "orderId": response.order.orderId},
        )

    def _status_response(self, record: NotificationRequestRecord) -> NotificationStatusResponse:
        return NotificationStatusResponse(
            requestId=record.requestId,
            kind=record.kind,  # type: ignore[arg-type]
            status=record.status,  # type: ignore[arg-type]
            sourceRepo=record.sourceRepo,
            sourceSystem=record.sourceSystem,
            clientRequestId=record.clientRequestId,
            title=record.title,
            description=record.description,
            targetUrl=record.targetUrl,
            createdAt=record.createdAt,
            updatedAt=record.updatedAt,
            expiresAt=record.expiresAt,
            decisionStatus=record.decisionStatus,  # type: ignore[arg-type]
            decision=record.decision,  # type: ignore[arg-type]
            decidedAt=record.decidedAt,
            decidedBy=record.decidedBy,
            executionStatus=record.executionStatus,  # type: ignore[arg-type]
            executionOrderId=record.executionOrderId,
            executionMessage=record.executionMessage,
            deliveries=self._repo.list_delivery_results(record.requestId),
            tradeApproval=self._trade_display(record),
        )

    def _trade_display(self, record: NotificationRequestRecord) -> TradeApprovalDisplay | None:
        if not record.tradeApprovalPayload:
            return None
        payload = self._trade_payload(record)
        order = payload.order
        return TradeApprovalDisplay(
            accountId=payload.accountId,
            previewId=payload.previewId,
            orderHash=payload.orderHash,
            environment=order.environment,
            symbol=order.symbol,
            side=order.side,
            orderType=order.orderType,
            timeInForce=order.timeInForce,
            quantity=order.quantity,
            notional=order.notional,
            limitPrice=order.limitPrice,
            stopPrice=order.stopPrice,
        )

    def _resolve_available_token(
        self,
        raw_token: str,
        *,
        allow_used: bool,
    ) -> tuple[NotificationTokenRecord, NotificationRequestRecord]:
        token = self._repo.get_token_by_hash(self._token_hash(raw_token))
        if token is None:
            raise NotificationError(404, "Notification action is unavailable.")
        if token.usedAt is not None and not allow_used:
            raise NotificationError(404, "Notification action is unavailable.")
        record = self._repo.get_request(token.requestId)
        if record is None:
            raise NotificationError(404, "Notification action is unavailable.")
        record = self._expire_if_needed(record)
        if record.decisionStatus == "expired":
            raise NotificationError(404, "Notification action is unavailable.")
        return token, record

    def _expire_if_needed(self, record: NotificationRequestRecord) -> NotificationRequestRecord:
        if record.decisionStatus != "pending" or record.expiresAt is None or record.expiresAt > utc_now():
            return record
        now = utc_now()
        self._repo.update_decision(
            request_id=record.requestId,
            status="expired",
            decision_status="expired",
            decision=None,
            decided_at=now,
            decided_by="system:expiry",
        )
        self._audit(
            request_id=record.requestId,
            token_id=None,
            event_type="expired",
            actor="system:expiry",
            summary="Notification approval expired.",
            payload={},
        )
        return self._repo.get_request(record.requestId) or record

    def _trade_payload(self, record: NotificationRequestRecord) -> TradeApprovalPayload:
        payload = dict(record.tradeApprovalPayload or {})
        payload.pop("computedOrderHash", None)
        return TradeApprovalPayload.model_validate(payload)

    def _normalize_recipients(self, recipients: list[NotificationRecipient]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for index, recipient in enumerate(recipients, start=1):
            payload = recipient.model_dump(mode="json")
            if not payload.get("recipientId"):
                payload["recipientId"] = f"recipient-{index}"
            normalized.append(payload)
        return normalized

    def _initial_delivery_attempts(self, recipients: list[dict[str, Any]]) -> list[dict[str, Any]]:
        attempts: list[dict[str, Any]] = []
        for recipient in recipients:
            for channel in recipient.get("channels") or []:
                attempts.append(
                    {
                        "attemptId": new_trade_id("notification-attempt"),
                        "recipientId": recipient["recipientId"],
                        "channel": channel,
                        "address": recipient["email"] if channel == "email" else recipient["phoneNumber"],
                        "attemptNumber": 1,
                    }
                )
        return attempts

    def _action_url(self, raw_token: str) -> str:
        path = self._settings.action_path_template.replace("{token}", raw_token)
        base_url = (self._settings.app_base_url or "").rstrip("/")
        if not base_url:
            return path
        return f"{base_url}/{path.lstrip('/')}"

    def _message_body(self, record: NotificationRequestRecord, *, action_url: str | None, channel: str) -> str:
        if channel == "sms":
            suffix = f" {action_url}" if action_url else ""
            return f"{record.title}: {record.description}{suffix}"[:800]
        parts = [record.description]
        if action_url:
            parts.extend(["", f"Open: {action_url}"])
        return "\n".join(parts)

    def _token_hash(self, raw_token: str) -> str:
        secret = (self._settings.token_hash_secret or "").encode("utf-8")
        raw = raw_token.encode("utf-8")
        if secret:
            return hmac.new(secret, raw, hashlib.sha256).hexdigest()
        return hashlib.sha256(raw).hexdigest()

    def _audit(
        self,
        *,
        request_id: str,
        token_id: str | None,
        event_type: str,
        actor: str | None,
        summary: str,
        payload: dict[str, Any],
    ) -> None:
        self._repo.save_audit_event(
            event_id=new_trade_id("notification-audit"),
            request_id=request_id,
            token_id=token_id,
            event_type=event_type,
            actor=actor,
            summary=summary,
            payload=payload,
            occurred_at=utc_now(),
        )

    @staticmethod
    def _sanitize_error(exc: Exception) -> str:
        return str(exc or exc.__class__.__name__)[:500]
