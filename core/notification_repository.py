from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from asset_allocation_contracts.notifications import NotificationDeliveryResult
from asset_allocation_runtime_common.foundation.postgres import connect


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _json_payload(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


@dataclass(frozen=True)
class NotificationRequestRecord:
    requestId: str
    sourceRepo: str
    sourceSystem: str | None
    clientRequestId: str
    idempotencyKey: str
    requestHash: str
    kind: str
    status: str
    title: str
    description: str
    targetUrl: str | None
    requestPayload: dict[str, Any]
    tradeApprovalPayload: dict[str, Any] | None
    decisionStatus: str
    decision: str | None
    decidedAt: datetime | None
    decidedBy: str | None
    executionStatus: str
    executionOrderId: str | None
    executionMessage: str | None
    expiresAt: datetime | None
    createdAt: datetime
    updatedAt: datetime


@dataclass(frozen=True)
class NotificationTokenRecord:
    tokenId: str
    requestId: str
    recipientId: str
    tokenHash: str
    expiresAt: datetime | None
    viewedAt: datetime | None
    usedAt: datetime | None
    createdAt: datetime


@dataclass(frozen=True)
class NotificationDeliveryAttemptRecord:
    attemptId: str
    requestId: str
    recipientId: str
    channel: str
    address: str
    status: str
    provider: str | None
    providerMessageId: str | None
    attemptNumber: int
    sanitizedError: str | None
    attemptedAt: datetime


class NotificationRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def get_by_idempotency(self, source_repo: str, idempotency_key: str) -> NotificationRequestRecord | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        request_id,
                        source_repo,
                        source_system,
                        client_request_id,
                        idempotency_key,
                        request_hash,
                        kind,
                        status,
                        title,
                        description,
                        target_url,
                        request_payload,
                        trade_approval_payload,
                        decision_status,
                        decision,
                        decided_at,
                        decided_by,
                        execution_status,
                        execution_order_id,
                        execution_message,
                        expires_at,
                        created_at,
                        updated_at
                    FROM core.notification_requests
                    WHERE source_repo = %s AND idempotency_key = %s
                    """,
                    (source_repo, idempotency_key),
                )
                row = cur.fetchone()
        return self._request_record(row) if row else None

    def get_request(self, request_id: str) -> NotificationRequestRecord | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        request_id,
                        source_repo,
                        source_system,
                        client_request_id,
                        idempotency_key,
                        request_hash,
                        kind,
                        status,
                        title,
                        description,
                        target_url,
                        request_payload,
                        trade_approval_payload,
                        decision_status,
                        decision,
                        decided_at,
                        decided_by,
                        execution_status,
                        execution_order_id,
                        execution_message,
                        expires_at,
                        created_at,
                        updated_at
                    FROM core.notification_requests
                    WHERE request_id = %s
                    """,
                    (request_id,),
                )
                row = cur.fetchone()
        return self._request_record(row) if row else None

    def create_request(
        self,
        *,
        record: NotificationRequestRecord,
        recipients: list[dict[str, Any]],
        tokens: list[dict[str, Any]],
        delivery_attempts: list[dict[str, Any]],
    ) -> None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.notification_requests (
                        request_id,
                        source_repo,
                        source_system,
                        client_request_id,
                        idempotency_key,
                        request_hash,
                        kind,
                        status,
                        title,
                        description,
                        target_url,
                        request_payload,
                        trade_approval_payload,
                        decision_status,
                        execution_status,
                        expires_at,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        record.requestId,
                        record.sourceRepo,
                        record.sourceSystem,
                        record.clientRequestId,
                        record.idempotencyKey,
                        record.requestHash,
                        record.kind,
                        record.status,
                        record.title,
                        record.description,
                        record.targetUrl,
                        _json_dumps(record.requestPayload),
                        _json_dumps(record.tradeApprovalPayload) if record.tradeApprovalPayload is not None else None,
                        record.decisionStatus,
                        record.executionStatus,
                        record.expiresAt,
                        record.createdAt,
                        record.updatedAt,
                    ),
                )
                for recipient in recipients:
                    cur.execute(
                        """
                        INSERT INTO core.notification_recipients (
                            request_id,
                            recipient_id,
                            display_name,
                            email,
                            phone_number,
                            channels,
                            recipient_payload,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                        """,
                        (
                            record.requestId,
                            recipient["recipientId"],
                            recipient.get("displayName"),
                            recipient.get("email"),
                            recipient.get("phoneNumber"),
                            _json_dumps(recipient.get("channels") or []),
                            _json_dumps(recipient),
                            record.createdAt,
                        ),
                    )
                for token in tokens:
                    cur.execute(
                        """
                        INSERT INTO core.notification_action_tokens (
                            token_id,
                            request_id,
                            recipient_id,
                            token_hash,
                            expires_at,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            token["tokenId"],
                            record.requestId,
                            token["recipientId"],
                            token["tokenHash"],
                            token.get("expiresAt"),
                            record.createdAt,
                        ),
                    )
                for attempt in delivery_attempts:
                    cur.execute(
                        """
                        INSERT INTO core.notification_delivery_attempts (
                            attempt_id,
                            request_id,
                            recipient_id,
                            channel,
                            address,
                            status,
                            attempt_number,
                            attempted_at,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s)
                        """,
                        (
                            attempt["attemptId"],
                            record.requestId,
                            attempt["recipientId"],
                            attempt["channel"],
                            attempt["address"],
                            attempt.get("attemptNumber", 1),
                            record.createdAt,
                            record.createdAt,
                        ),
                    )

    def list_recipients(self, request_id: str) -> list[dict[str, Any]]:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT recipient_payload
                    FROM core.notification_recipients
                    WHERE request_id = %s
                    ORDER BY recipient_id
                    """,
                    (request_id,),
                )
                rows = cur.fetchall()
        return [_json_payload(row[0]) for row in rows]

    def get_token_for_recipient(self, request_id: str, recipient_id: str) -> NotificationTokenRecord | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT token_id, request_id, recipient_id, token_hash, expires_at, viewed_at, used_at, created_at
                    FROM core.notification_action_tokens
                    WHERE request_id = %s AND recipient_id = %s
                    """,
                    (request_id, recipient_id),
                )
                row = cur.fetchone()
        return self._token_record(row) if row else None

    def get_token_by_hash(self, token_hash: str) -> NotificationTokenRecord | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT token_id, request_id, recipient_id, token_hash, expires_at, viewed_at, used_at, created_at
                    FROM core.notification_action_tokens
                    WHERE token_hash = %s
                    """,
                    (token_hash,),
                )
                row = cur.fetchone()
        return self._token_record(row) if row else None

    def mark_token_viewed(self, token_id: str, viewed_at: datetime) -> None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.notification_action_tokens
                    SET viewed_at = COALESCE(viewed_at, %s)
                    WHERE token_id = %s
                    """,
                    (viewed_at, token_id),
                )

    def mark_token_used(self, token_id: str, used_at: datetime) -> bool:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.notification_action_tokens
                    SET used_at = %s
                    WHERE token_id = %s AND used_at IS NULL
                    RETURNING token_id
                    """,
                    (used_at, token_id),
                )
                row = cur.fetchone()
        return bool(row)

    def save_delivery_attempt(
        self,
        *,
        attempt_id: str,
        request_id: str,
        recipient_id: str,
        channel: str,
        address: str,
        status: str,
        provider: str | None,
        provider_message_id: str | None,
        attempt_number: int,
        sanitized_error: str | None,
        attempted_at: datetime,
    ) -> None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.notification_delivery_attempts (
                        attempt_id,
                        request_id,
                        recipient_id,
                        channel,
                        address,
                        status,
                        provider,
                        provider_message_id,
                        attempt_number,
                        sanitized_error,
                        attempted_at,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        attempt_id,
                        request_id,
                        recipient_id,
                        channel,
                        address,
                        status,
                        provider,
                        provider_message_id,
                        attempt_number,
                        sanitized_error,
                        attempted_at,
                        attempted_at,
                    ),
                )

    def list_pending_delivery_attempts(self, request_id: str) -> list[NotificationDeliveryAttemptRecord]:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        attempt_id,
                        request_id,
                        recipient_id,
                        channel,
                        address,
                        status,
                        provider,
                        provider_message_id,
                        attempt_number,
                        sanitized_error,
                        attempted_at
                    FROM core.notification_delivery_attempts
                    WHERE request_id = %s AND status = 'pending'
                    ORDER BY created_at, attempt_id
                    """,
                    (request_id,),
                )
                rows = cur.fetchall()
        return [self._delivery_attempt_record(row) for row in rows]

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
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.notification_delivery_attempts
                    SET
                        status = %s,
                        provider = %s,
                        provider_message_id = %s,
                        sanitized_error = %s,
                        attempted_at = %s
                    WHERE attempt_id = %s
                    """,
                    (status, provider, provider_message_id, sanitized_error, attempted_at, attempt_id),
                )

    def list_delivery_results(self, request_id: str) -> list[NotificationDeliveryResult]:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT recipient_id, channel, address, status, provider, provider_message_id, attempted_at, sanitized_error
                    FROM core.notification_delivery_attempts
                    WHERE request_id = %s
                    ORDER BY attempted_at, attempt_id
                    """,
                    (request_id,),
                )
                rows = cur.fetchall()
        return [
            NotificationDeliveryResult(
                recipientId=str(row[0]),
                channel=str(row[1]),
                address=str(row[2]),
                status=str(row[3]),
                provider=str(row[4]) if row[4] else None,
                providerMessageId=str(row[5]) if row[5] else None,
                attemptedAt=row[6],
                sanitizedError=str(row[7]) if row[7] else None,
            )
            for row in rows
        ]

    def update_request_status(self, request_id: str, status: str, updated_at: datetime) -> None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.notification_requests
                    SET status = %s, updated_at = %s
                    WHERE request_id = %s
                    """,
                    (status, updated_at, request_id),
                )

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
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.notification_requests
                    SET
                        status = %s,
                        decision_status = %s,
                        decision = %s,
                        decided_at = %s,
                        decided_by = %s,
                        updated_at = %s
                    WHERE request_id = %s
                    """,
                    (status, decision_status, decision, decided_at, decided_by, decided_at, request_id),
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
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.notification_requests
                    SET
                        execution_status = %s,
                        execution_order_id = %s,
                        execution_message = %s,
                        updated_at = %s
                    WHERE request_id = %s
                    """,
                    (execution_status, execution_order_id, execution_message, updated_at, request_id),
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
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.notification_audit_events (
                        event_id,
                        request_id,
                        token_id,
                        event_type,
                        actor,
                        summary,
                        event_payload,
                        occurred_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (event_id, request_id, token_id, event_type, actor, summary, _json_dumps(payload), occurred_at),
                )

    @staticmethod
    def _request_record(row: Any) -> NotificationRequestRecord:
        return NotificationRequestRecord(
            requestId=str(row[0]),
            sourceRepo=str(row[1]),
            sourceSystem=str(row[2]) if row[2] else None,
            clientRequestId=str(row[3]),
            idempotencyKey=str(row[4]),
            requestHash=str(row[5]),
            kind=str(row[6]),
            status=str(row[7]),
            title=str(row[8]),
            description=str(row[9]),
            targetUrl=str(row[10]) if row[10] else None,
            requestPayload=_json_payload(row[11]) or {},
            tradeApprovalPayload=_json_payload(row[12]),
            decisionStatus=str(row[13]),
            decision=str(row[14]) if row[14] else None,
            decidedAt=row[15],
            decidedBy=str(row[16]) if row[16] else None,
            executionStatus=str(row[17]),
            executionOrderId=str(row[18]) if row[18] else None,
            executionMessage=str(row[19]) if row[19] else None,
            expiresAt=row[20],
            createdAt=row[21],
            updatedAt=row[22],
        )

    @staticmethod
    def _token_record(row: Any) -> NotificationTokenRecord:
        return NotificationTokenRecord(
            tokenId=str(row[0]),
            requestId=str(row[1]),
            recipientId=str(row[2]),
            tokenHash=str(row[3]),
            expiresAt=row[4],
            viewedAt=row[5],
            usedAt=row[6],
            createdAt=row[7],
        )

    @staticmethod
    def _delivery_attempt_record(row: Any) -> NotificationDeliveryAttemptRecord:
        return NotificationDeliveryAttemptRecord(
            attemptId=str(row[0]),
            requestId=str(row[1]),
            recipientId=str(row[2]),
            channel=str(row[3]),
            address=str(row[4]),
            status=str(row[5]),
            provider=str(row[6]) if row[6] else None,
            providerMessageId=str(row[7]) if row[7] else None,
            attemptNumber=int(row[8]),
            sanitizedError=str(row[9]) if row[9] else None,
            attemptedAt=row[10],
        )
