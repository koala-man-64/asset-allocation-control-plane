from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountConfiguration,
    BrokerAccountConfigurationAuditRecord,
    BrokerStrategyAllocationSummary,
    BrokerTradingPolicy,
)
from asset_allocation_runtime_common.foundation.postgres import connect


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        return json.loads(text)
    return default


class BrokerAccountConfigurationRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def get_configuration(self, account_id: str) -> BrokerAccountConfiguration | None:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        account_id,
                        configuration_version,
                        requested_policy_json,
                        effective_policy_json,
                        allocation_summary_json,
                        warnings_json,
                        updated_at,
                        updated_by
                    FROM core.broker_account_configurations
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return BrokerAccountConfiguration(
            accountId=str(row[0]),
            configurationVersion=int(row[1] or 1),
            requestedPolicy=BrokerTradingPolicy.model_validate(_json_loads(row[2], {})),
            effectivePolicy=BrokerTradingPolicy.model_validate(_json_loads(row[3], {})),
            allocation=BrokerStrategyAllocationSummary.model_validate(_json_loads(row[4], {})),
            warnings=_json_loads(row[5], []),
            updatedAt=row[6],
            updatedBy=row[7],
        )

    def save_trading_policy(
        self,
        *,
        account_id: str,
        expected_configuration_version: int | None,
        requested_policy: BrokerTradingPolicy,
        effective_policy: BrokerTradingPolicy,
        warnings: list[str],
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountConfiguration:
        current = self.get_configuration(account_id)
        current_version = current.configurationVersion if current else 1
        if expected_configuration_version is not None and expected_configuration_version != current_version:
            raise ValueError(
                f"Configuration version conflict for account '{account_id}': "
                f"expected {expected_configuration_version}, found {current_version}."
            )

        next_version = current_version + 1 if current else 1
        allocation = current.allocation if current else BrokerStrategyAllocationSummary()
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.broker_account_configurations (
                        account_id,
                        configuration_version,
                        requested_policy_json,
                        effective_policy_json,
                        allocation_summary_json,
                        warnings_json,
                        updated_at,
                        updated_by
                    )
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, NOW(), %s)
                    ON CONFLICT (account_id)
                    DO UPDATE SET
                        configuration_version = EXCLUDED.configuration_version,
                        requested_policy_json = EXCLUDED.requested_policy_json,
                        effective_policy_json = EXCLUDED.effective_policy_json,
                        warnings_json = EXCLUDED.warnings_json,
                        updated_at = NOW(),
                        updated_by = EXCLUDED.updated_by
                    """,
                    (
                        account_id,
                        next_version,
                        _json_dumps(requested_policy.model_dump(mode="json")),
                        _json_dumps(effective_policy.model_dump(mode="json")),
                        _json_dumps(allocation.model_dump(mode="json")),
                        _json_dumps(warnings),
                        actor,
                    ),
                )
        self.save_audit(
            account_id=account_id,
            category="trading_policy",
            outcome="warning" if warnings else "saved",
            actor=actor,
            request_id=request_id,
            granted_roles=granted_roles,
            summary="Saved broker account trading policy.",
            before=(current.requestedPolicy.model_dump(mode="json") if current else {}),
            after=requested_policy.model_dump(mode="json"),
            denial_reason=None,
        )
        return BrokerAccountConfiguration(
            accountId=account_id,
            configurationVersion=next_version,
            requestedPolicy=requested_policy,
            effectivePolicy=effective_policy,
            allocation=allocation,
            warnings=warnings,
            updatedBy=actor,
        )

    def save_allocation_summary(
        self,
        *,
        account_id: str,
        expected_configuration_version: int | None,
        allocation: BrokerStrategyAllocationSummary,
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountConfiguration:
        current = self.get_configuration(account_id)
        current_version = current.configurationVersion if current else 1
        if expected_configuration_version is not None and expected_configuration_version != current_version:
            raise ValueError(
                f"Configuration version conflict for account '{account_id}': "
                f"expected {expected_configuration_version}, found {current_version}."
            )

        next_version = current_version + 1 if current else 1
        requested_policy = current.requestedPolicy if current else BrokerTradingPolicy()
        effective_policy = current.effectivePolicy if current else requested_policy
        warnings = current.warnings if current else []
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.broker_account_configurations (
                        account_id,
                        configuration_version,
                        requested_policy_json,
                        effective_policy_json,
                        allocation_summary_json,
                        warnings_json,
                        updated_at,
                        updated_by
                    )
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, NOW(), %s)
                    ON CONFLICT (account_id)
                    DO UPDATE SET
                        configuration_version = EXCLUDED.configuration_version,
                        requested_policy_json = EXCLUDED.requested_policy_json,
                        effective_policy_json = EXCLUDED.effective_policy_json,
                        allocation_summary_json = EXCLUDED.allocation_summary_json,
                        warnings_json = EXCLUDED.warnings_json,
                        updated_at = NOW(),
                        updated_by = EXCLUDED.updated_by
                    """,
                    (
                        account_id,
                        next_version,
                        _json_dumps(requested_policy.model_dump(mode="json")),
                        _json_dumps(effective_policy.model_dump(mode="json")),
                        _json_dumps(allocation.model_dump(mode="json")),
                        _json_dumps(warnings),
                        actor,
                    ),
                )
        self.save_audit(
            account_id=account_id,
            category="allocation",
            outcome="saved",
            actor=actor,
            request_id=request_id,
            granted_roles=granted_roles,
            summary="Saved broker account allocation.",
            before=(current.allocation.model_dump(mode="json") if current else {}),
            after=allocation.model_dump(mode="json"),
            denial_reason=None,
        )
        return BrokerAccountConfiguration(
            accountId=account_id,
            configurationVersion=next_version,
            requestedPolicy=requested_policy,
            effectivePolicy=effective_policy,
            allocation=allocation,
            warnings=warnings,
            updatedBy=actor,
        )

    def list_audit(self, account_id: str, *, limit: int = 25) -> list[BrokerAccountConfigurationAuditRecord]:
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT audit_payload
                    FROM core.broker_account_configuration_audit
                    WHERE account_id = %s
                    ORDER BY created_at DESC, audit_id DESC
                    LIMIT %s
                    """,
                    (account_id, max(1, int(limit))),
                )
                rows = cur.fetchall()
        return [
            BrokerAccountConfigurationAuditRecord.model_validate(_json_loads(row[0], {}))
            for row in rows
        ]

    def save_audit(
        self,
        *,
        account_id: str,
        category: str,
        outcome: str,
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
        summary: str,
        before: dict[str, Any],
        after: dict[str, Any],
        denial_reason: str | None,
    ) -> BrokerAccountConfigurationAuditRecord:
        audit = BrokerAccountConfigurationAuditRecord(
            auditId=f"audit-{uuid.uuid4().hex}",
            accountId=account_id,
            category=category,
            outcome=outcome,
            requestedAt=datetime.now(timezone.utc),
            actor=actor,
            requestId=request_id,
            grantedRoles=granted_roles,
            summary=summary,
            before=before,
            after=after,
            denialReason=denial_reason,
        )
        with connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.broker_account_configuration_audit (
                        audit_id,
                        account_id,
                        category,
                        outcome,
                        actor,
                        request_id,
                        granted_roles_json,
                        summary,
                        before_json,
                        after_json,
                        denial_reason,
                        audit_payload,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, NOW())
                    """,
                    (
                        audit.auditId,
                        account_id,
                        category,
                        outcome,
                        actor,
                        request_id,
                        _json_dumps(granted_roles),
                        summary,
                        _json_dumps(before),
                        _json_dumps(after),
                        denial_reason,
                        _json_dumps(audit.model_dump(mode="json")),
                    ),
                )
        return audit
