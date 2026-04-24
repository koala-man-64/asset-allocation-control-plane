from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountListResponse,
    TradeDeskAuditEvent,
    TradeOrder,
    TradeOrderCancelRequest,
    TradeOrderCancelResponse,
    TradeOrderHistoryResponse,
    TradeOrderPlaceRequest,
    TradeOrderPlaceResponse,
    TradeOrderPreviewRequest,
    TradeOrderPreviewResponse,
    TradePositionListResponse,
    TradeRiskCheck,
)

from api.service.settings import TradeDeskSettings
from core.trade_desk_repository import TradeDeskRepository, new_trade_id, stable_hash, utc_now

_TERMINAL_STATUSES = {"filled", "cancelled", "rejected", "expired"}


class TradeDeskError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class TradeDeskService:
    def __init__(self, repository: TradeDeskRepository, settings: TradeDeskSettings) -> None:
        self._repo = repository
        self._settings = settings

    def list_accounts(self) -> TradeAccountListResponse:
        response = self._repo.list_accounts()
        allowlist = {account_id.strip() for account_id in self._settings.account_allowlist if account_id.strip()}
        if not allowlist:
            return response
        return response.model_copy(
            update={"accounts": [account for account in response.accounts if account.accountId in allowlist]}
        )

    def get_account(self, account_id: str) -> TradeAccountDetail:
        record = self._repo.get_account_record(account_id)
        if record is None:
            raise TradeDeskError(404, f"Trade account '{account_id}' not found.")
        events = self._repo.list_audit_events(account_id, limit=25).events
        return record.detail.model_copy(update={"recentAuditEvents": events})

    def list_positions(self, account_id: str) -> TradePositionListResponse:
        try:
            return self._repo.list_positions(account_id)
        except LookupError as exc:
            raise TradeDeskError(404, str(exc)) from exc

    def list_orders(self, account_id: str) -> TradeOrderHistoryResponse:
        try:
            return self._repo.list_orders(account_id)
        except LookupError as exc:
            raise TradeDeskError(404, str(exc)) from exc

    def list_history(self, account_id: str) -> TradeOrderHistoryResponse:
        try:
            return self._repo.list_history(account_id)
        except LookupError as exc:
            raise TradeDeskError(404, str(exc)) from exc

    def preview_order(self, account_id: str, payload: TradeOrderPreviewRequest, *, actor: str | None) -> TradeOrderPreviewResponse:
        self._validate_path_account(account_id, payload.accountId)
        if payload.source != "manual":
            raise TradeDeskError(400, "Only manual trade desk orders can be previewed on this endpoint.")

        record = self._account_record(account_id)
        checks = self._risk_checks(record.account, payload)
        blocked = any(check.blocking for check in checks)
        now = utc_now()
        preview_id = new_trade_id("preview")
        order = self._build_order(
            order_id=preview_id,
            payload=payload,
            provider=record.account.provider,
            status="previewed",
            provider_order_id=None,
            risk_checks=checks,
            reconciliation_required=False,
            created_at=now,
        )
        estimated_cost = self._estimated_notional(payload)
        response = TradeOrderPreviewResponse(
            previewId=preview_id,
            accountId=account_id,
            provider=record.account.provider,
            environment=payload.environment,
            order=order,
            generatedAt=now,
            expiresAt=now + timedelta(minutes=5),
            estimatedCost=estimated_cost,
            cashAfter=None if estimated_cost is None else record.account.cash - estimated_cost,
            buyingPowerAfter=None if estimated_cost is None else record.account.buyingPower - estimated_cost,
            riskChecks=checks,
            warnings=[check.message for check in checks if check.status == "warning" and check.message],
            blocked=blocked,
            blockReason=self._blocking_message(checks),
            freshness=record.account.freshness,
        )
        self._repo.save_order(
            order,
            request_payload=payload.model_dump(mode="json"),
            response_payload=response.model_dump(mode="json"),
            request_hash=stable_hash(payload.model_dump(mode="json")),
        )
        self._audit(
            account_id=account_id,
            provider=record.account.provider,
            environment=payload.environment,
            event_type="preview",
            severity="critical" if blocked else "info",
            actor=actor,
            order_id=order.orderId,
            client_request_id=payload.clientRequestId,
            idempotency_key=None,
            status_before=None,
            status_after=order.status,
            summary="Preview blocked by risk checks." if blocked else "Manual order preview generated.",
            details={"blocked": blocked, "riskChecks": [check.model_dump(mode="json") for check in checks]},
        )
        return response

    def place_order(self, account_id: str, payload: TradeOrderPlaceRequest, *, actor: str | None) -> TradeOrderPlaceResponse:
        self._validate_path_account(account_id, payload.accountId)
        if payload.source != "manual":
            raise TradeDeskError(400, "Synthetic or system-generated proposals are not executable on this endpoint.")

        request_payload = payload.model_dump(mode="json")
        request_hash = stable_hash(request_payload)
        existing = self._repo.get_idempotency(account_id, "place", payload.idempotencyKey)
        if existing is not None:
            if existing.requestHash != request_hash:
                raise TradeDeskError(409, "Idempotency key was already used for a different place-order request.")
            response = TradeOrderPlaceResponse.model_validate(existing.responsePayload)
            return response.model_copy(update={"replayed": True})

        record = self._account_record(account_id)
        checks = self._risk_checks(record.account, payload)
        gate_check = self._execution_gate_check(record.account.accountId, record.account.provider, payload.environment)
        if gate_check is not None:
            checks.append(gate_check)

        blocked = any(check.blocking for check in checks)
        now = utc_now()
        status = "rejected" if blocked else "accepted"
        provider_order_id = None if blocked else new_trade_id(f"{record.account.provider}-paper")
        order = self._build_order(
            order_id=new_trade_id("order"),
            payload=payload,
            provider=record.account.provider,
            status=status,
            provider_order_id=provider_order_id,
            risk_checks=checks,
            reconciliation_required=False,
            created_at=now,
        )
        response = TradeOrderPlaceResponse(
            order=order,
            submitted=not blocked,
            replayed=False,
            reconciliationRequired=False,
            auditEventId=None,
            message=self._blocking_message(checks) if blocked else "Order accepted by trade desk execution gate.",
        )
        response_payload = response.model_dump(mode="json")
        self._repo.save_order(
            order,
            request_payload=request_payload,
            response_payload=response_payload,
            request_hash=request_hash,
        )
        self._repo.save_idempotency(
            account_id=account_id,
            action="place",
            idempotency_key=payload.idempotencyKey,
            request_hash=request_hash,
            actor=actor,
            response_payload=response_payload,
            provider_order_id=provider_order_id,
        )
        self._audit(
            account_id=account_id,
            provider=record.account.provider,
            environment=payload.environment,
            event_type="submit" if not blocked else "system_block",
            severity="critical" if blocked else "info",
            actor=actor,
            order_id=order.orderId,
            client_request_id=payload.clientRequestId,
            idempotency_key=payload.idempotencyKey,
            status_before="previewed",
            status_after=order.status,
            summary=response.message or "",
            details={"submitted": response.submitted, "riskChecks": [check.model_dump(mode="json") for check in checks]},
        )
        return response

    def cancel_order(
        self,
        account_id: str,
        order_id: str,
        payload: TradeOrderCancelRequest,
        *,
        actor: str | None,
    ) -> TradeOrderCancelResponse:
        self._validate_path_account(account_id, payload.accountId)
        self._validate_path_account(order_id, payload.orderId)
        request_payload = payload.model_dump(mode="json")
        request_hash = stable_hash(request_payload)
        existing = self._repo.get_idempotency(account_id, "cancel", payload.idempotencyKey)
        if existing is not None:
            if existing.requestHash != request_hash:
                raise TradeDeskError(409, "Idempotency key was already used for a different cancel-order request.")
            response = TradeOrderCancelResponse.model_validate(existing.responsePayload)
            return response.model_copy(update={"replayed": True})

        record = self._account_record(account_id)
        existing_order = self._repo.get_order(account_id, order_id)
        if existing_order is None:
            raise TradeDeskError(404, f"Trade order '{order_id}' not found.")

        checks: list[TradeRiskCheck] = []
        if not record.account.capabilities.canCancel:
            checks.append(self._fail_check("cancel_capability", "Cancel capability", "Account does not allow cancels."))
        if existing_order.status in _TERMINAL_STATUSES:
            checks.append(
                self._fail_check(
                    "terminal_order",
                    "Terminal order state",
                    f"Order is already {existing_order.status} and cannot be cancelled.",
                )
            )
        gate_check = self._execution_gate_check(record.account.accountId, record.account.provider, record.account.environment)
        if gate_check is not None:
            checks.append(gate_check)

        blocked = any(check.blocking for check in checks)
        updated_order = existing_order.model_copy(
            update={
                "status": "rejected" if blocked else "cancel_pending",
                "riskChecks": [*existing_order.riskChecks, *checks],
                "updatedAt": utc_now(),
            }
        )
        response = TradeOrderCancelResponse(
            order=updated_order,
            cancelAccepted=not blocked,
            replayed=False,
            reconciliationRequired=False,
            message=self._blocking_message(checks) if blocked else "Cancel accepted by trade desk execution gate.",
        )
        response_payload = response.model_dump(mode="json")
        self._repo.save_order(
            updated_order,
            request_payload=request_payload,
            response_payload=response_payload,
            request_hash=request_hash,
        )
        self._repo.save_idempotency(
            account_id=account_id,
            action="cancel",
            idempotency_key=payload.idempotencyKey,
            request_hash=request_hash,
            actor=actor,
            response_payload=response_payload,
            provider_order_id=updated_order.providerOrderId,
        )
        self._audit(
            account_id=account_id,
            provider=record.account.provider,
            environment=record.account.environment,
            event_type="cancel" if not blocked else "system_block",
            severity="critical" if blocked else "info",
            actor=actor,
            order_id=order_id,
            client_request_id=payload.clientRequestId,
            idempotency_key=payload.idempotencyKey,
            status_before=existing_order.status,
            status_after=updated_order.status,
            summary=response.message or "",
            details={"cancelAccepted": response.cancelAccepted, "riskChecks": [check.model_dump(mode="json") for check in checks]},
        )
        return response

    def _account_record(self, account_id: str):
        record = self._repo.get_account_record(account_id)
        if record is None:
            raise TradeDeskError(404, f"Trade account '{account_id}' not found.")
        allowlist = {value.strip() for value in self._settings.account_allowlist if value.strip()}
        if allowlist and account_id not in allowlist:
            raise TradeDeskError(403, f"Trade account '{account_id}' is not allowlisted for trade desk access.")
        return record

    def _risk_checks(self, account, payload: TradeOrderPreviewRequest) -> list[TradeRiskCheck]:
        checks: list[TradeRiskCheck] = []
        if payload.environment != account.environment:
            checks.append(
                self._fail_check(
                    "environment_mismatch",
                    "Environment match",
                    f"Request environment {payload.environment} does not match account environment {account.environment}.",
                )
            )
        if account.readiness != "ready":
            checks.append(
                self._fail_check(
                    "account_readiness",
                    "Account readiness",
                    account.readinessReason or "Account is not ready for trading.",
                )
            )
        if account.killSwitchActive:
            checks.append(self._fail_check("account_kill_switch", "Account kill switch", "Account kill switch is active."))
        if not account.capabilities.canPreview:
            checks.append(self._fail_check("preview_capability", "Preview capability", "Account cannot preview orders."))
        if payload.environment == "paper" and not account.capabilities.canSubmitPaper:
            checks.append(self._fail_check("paper_capability", "Paper trading", "Account cannot submit paper orders."))
        if payload.environment == "sandbox" and not account.capabilities.canSubmitSandbox:
            checks.append(self._fail_check("sandbox_capability", "Sandbox trading", "Account cannot submit sandbox orders."))
        if payload.environment == "live" and not account.capabilities.canSubmitLive:
            checks.append(self._fail_check("live_capability", "Live trading", "Account cannot submit live orders."))
        if payload.orderType == "market" and not account.capabilities.supportsMarketOrders:
            checks.append(self._fail_check("market_order_support", "Market orders", "Account does not support market orders."))
        if payload.orderType == "limit" and not account.capabilities.supportsLimitOrders:
            checks.append(self._fail_check("limit_order_support", "Limit orders", "Account does not support limit orders."))
        if payload.orderType in {"stop", "stop_limit"} and not account.capabilities.supportsStopOrders:
            checks.append(self._fail_check("stop_order_support", "Stop orders", "Account does not support stop orders."))
        if payload.notional is not None and not account.capabilities.supportsNotionalOrders:
            checks.append(self._fail_check("notional_order_support", "Notional orders", "Account does not support notional orders."))
        if payload.quantity is not None and payload.quantity % 1 != 0 and not account.capabilities.supportsFractionalQuantity:
            checks.append(
                self._fail_check("fractional_quantity_support", "Fractional quantity", "Account does not support fractional quantities.")
            )
        checks.extend(self._freshness_checks(account.freshness))
        estimated_notional = self._estimated_notional(payload)
        if estimated_notional is not None:
            max_notional = self._settings.max_order_notional
            if estimated_notional > max_notional:
                checks.append(
                    self._fail_check(
                        "max_order_notional",
                        "Maximum order notional",
                        f"Estimated notional {estimated_notional:.2f} exceeds max order notional {max_notional:.2f}.",
                    )
                )
            if payload.side == "buy" and estimated_notional > account.buyingPower:
                checks.append(
                    self._fail_check(
                        "buying_power",
                        "Buying power",
                        "Estimated buy notional exceeds current buying power.",
                    )
                )
        elif payload.orderType == "market":
            checks.append(
                TradeRiskCheck(
                    checkId="estimated_notional_unknown",
                    code="estimated_notional_unknown",
                    label="Estimated notional",
                    status="warning",
                    severity="warning",
                    blocking=False,
                    message="Market order notional is unknown until priced by the provider.",
                )
            )
        return checks

    def _freshness_checks(self, freshness) -> list[TradeRiskCheck]:
        checks: list[TradeRiskCheck] = []
        for field_name, label in (
            ("balancesState", "Cash and buying power freshness"),
            ("positionsState", "Position freshness"),
            ("ordersState", "Open order freshness"),
        ):
            state = getattr(freshness, field_name)
            if state != "fresh":
                checks.append(
                    self._fail_check(
                        field_name,
                        label,
                        freshness.staleReason or f"{label} is {state}; refresh before submitting orders.",
                    )
                )
        return checks

    def _execution_gate_check(self, account_id: str, provider: str, environment: str) -> TradeRiskCheck | None:
        if self._settings.global_kill_switch:
            return self._fail_check("global_kill_switch", "Global kill switch", "Global trade desk kill switch is active.")
        if provider in set(self._settings.provider_kill_switches):
            return self._fail_check("provider_kill_switch", "Provider kill switch", f"{provider} trading is disabled.")
        if account_id in set(self._settings.account_kill_switches):
            return self._fail_check("account_kill_switch", "Account kill switch", "Account kill switch is active.")
        if environment == "paper" and not self._settings.paper_execution_enabled:
            return self._fail_check("paper_execution_gate", "Paper execution gate", "Paper execution is disabled.")
        if environment == "sandbox" and not self._settings.sandbox_execution_enabled:
            return self._fail_check("sandbox_execution_gate", "Sandbox execution gate", "Sandbox execution is disabled.")
        if environment == "live":
            if self._settings.live_kill_switch:
                return self._fail_check("live_kill_switch", "Live kill switch", "Live trading kill switch is active.")
            if not self._settings.live_execution_enabled:
                return self._fail_check("live_execution_gate", "Live execution gate", "Live execution is disabled.")
            if account_id not in set(self._settings.live_account_allowlist):
                return self._fail_check("live_account_allowlist", "Live account allowlist", "Account is not allowlisted for live trading.")
        if not self._settings.simulated_execution_enabled:
            return self._fail_check(
                "provider_submission_gate",
                "Provider submission gate",
                "Provider submission is disabled; enable simulated or provider-backed execution before submitting.",
            )
        return None

    def _build_order(
        self,
        *,
        order_id: str,
        payload: TradeOrderPreviewRequest,
        provider: str,
        status: str,
        provider_order_id: str | None,
        risk_checks: list[TradeRiskCheck],
        reconciliation_required: bool,
        created_at: datetime,
    ) -> TradeOrder:
        estimated_notional = self._estimated_notional(payload)
        return TradeOrder(
            orderId=order_id,
            accountId=payload.accountId,
            provider=provider,
            environment=payload.environment,
            status=status,
            symbol=payload.symbol,
            side=payload.side,
            orderType=payload.orderType,
            timeInForce=payload.timeInForce,
            assetClass=payload.assetClass,
            clientRequestId=payload.clientRequestId,
            idempotencyKey=getattr(payload, "idempotencyKey", None),
            providerOrderId=provider_order_id,
            quantity=payload.quantity,
            notional=payload.notional,
            limitPrice=payload.limitPrice,
            stopPrice=payload.stopPrice,
            estimatedNotional=estimated_notional,
            submittedAt=created_at if status in {"submitted", "accepted"} else None,
            acceptedAt=created_at if status == "accepted" else None,
            createdAt=created_at,
            updatedAt=created_at,
            riskChecks=risk_checks,
            reconciliationRequired=reconciliation_required,
        )

    def _audit(
        self,
        *,
        account_id: str,
        provider: str,
        environment: str,
        event_type: str,
        severity: str,
        actor: str | None,
        order_id: str | None,
        client_request_id: str | None,
        idempotency_key: str | None,
        status_before: str | None,
        status_after: str | None,
        summary: str,
        details: dict[str, Any],
    ) -> None:
        self._repo.save_audit_event(
            TradeDeskAuditEvent(
                eventId=new_trade_id("audit"),
                accountId=account_id,
                provider=provider,
                environment=environment,
                eventType=event_type,
                severity=severity,
                occurredAt=utc_now(),
                actor=actor,
                orderId=order_id,
                clientRequestId=client_request_id,
                idempotencyKey=idempotency_key,
                statusBefore=status_before,
                statusAfter=status_after,
                summary=summary,
                details=details,
            )
        )

    @staticmethod
    def _validate_path_account(path_value: str, payload_value: str) -> None:
        if str(path_value).strip() != str(payload_value).strip():
            raise TradeDeskError(400, "Path identifier does not match request payload.")

    @staticmethod
    def _estimated_notional(payload: TradeOrderPreviewRequest) -> float | None:
        if payload.notional is not None:
            return float(payload.notional)
        if payload.quantity is not None and payload.limitPrice is not None:
            return float(payload.quantity) * float(payload.limitPrice)
        return None

    @staticmethod
    def _fail_check(code: str, label: str, message: str) -> TradeRiskCheck:
        return TradeRiskCheck(
            checkId=code,
            code=code,
            label=label,
            status="fail",
            severity="critical",
            blocking=True,
            message=message,
        )

    @staticmethod
    def _blocking_message(checks: list[TradeRiskCheck]) -> str | None:
        for check in checks:
            if check.blocking and check.message:
                return check.message
        return None
