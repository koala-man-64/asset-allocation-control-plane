from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from asset_allocation_contracts.broker_accounts import (
    BrokerAccountConfigurationAuditRecord,
    BrokerAccountOnboardingRequest,
    BrokerAccountSummary,
    BrokerConnectionHealth,
)
from asset_allocation_contracts.trade_desk import TradeAccountSummary

from api.service.broker_account_onboarding_service import (
    BrokerAccountOnboardingError,
    BrokerAccountOnboardingService,
)
from api.service.settings import TradeDeskSettings
from core.trade_desk_repository import TradeAccountSeedState


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


class FakeTradeRepo:
    def __init__(self, states: dict[str, TradeAccountSeedState] | None = None) -> None:
        self.states = states or {}
        self.saved_account: TradeAccountSummary | None = None

    def get_account_seed_state(self, account_id: str) -> TradeAccountSeedState | None:
        return self.states.get(account_id)

    def upsert_account_seed(self, *, account, detail, provider_account_key, live_trading_allowed, kill_switch_active):
        current = self.states.get(account.accountId)
        created = current is None
        reenabled = current is not None and not current.enabled
        self.saved_account = account
        self.states[account.accountId] = TradeAccountSeedState(
            accountId=account.accountId,
            enabled=True,
            provider=account.provider,
            environment=account.environment,
            providerAccountKey=provider_account_key,
        )
        return created, reenabled


class FakeConfigurationRepo:
    def __init__(self) -> None:
        self.audit_payload: dict[str, Any] | None = None

    def save_audit(self, **kwargs):
        self.audit_payload = kwargs
        return BrokerAccountConfigurationAuditRecord(
            auditId="audit-1",
            accountId=kwargs["account_id"],
            category=kwargs["category"],
            outcome=kwargs["outcome"],
            requestedAt=_now(),
            actor=kwargs["actor"],
            requestId=kwargs["request_id"],
            grantedRoles=kwargs["granted_roles"],
            summary=kwargs["summary"],
            before=kwargs["before"],
            after=kwargs["after"],
            denialReason=kwargs["denial_reason"],
        )


class FakeConfigurationService:
    def get_configuration(self, account_id: str):
        return None


class FakeOperationsService:
    def __init__(self, repo: FakeTradeRepo) -> None:
        self.repo = repo

    def get_account(self, account_id: str):
        account = self.repo.saved_account
        assert account is not None
        assert account.accountId == account_id
        return type(
            "Detail",
            (),
            {
                "account": BrokerAccountSummary(
                    accountId=account.accountId,
                    broker=account.provider,
                    name=account.name,
                    accountNumberMasked=account.accountNumberMasked,
                    baseCurrency=account.baseCurrency,
                    overallStatus="warning",
                    tradeReadiness=account.readiness,
                    connectionHealth=BrokerConnectionHealth(
                        overallStatus="warning",
                        authStatus="authenticated",
                        connectionState="degraded",
                        syncStatus="never_synced",
                        lastCheckedAt=_now(),
                    ),
                    equity=account.equity,
                    cash=account.cash,
                    buyingPower=account.buyingPower,
                    openPositionCount=account.positionCount,
                    openOrderCount=account.openOrderCount,
                ),
                "configuration": None,
            },
        )()


class FakeRefreshService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def action_response(self, *, account_id: str, action: str, trigger: str):
        self.calls.append((account_id, action, trigger))
        return None


class FakeAlpacaGateway:
    def get_account(self, *, environment: str, subject: str | None):
        return {
            "account_number": "PA123456789",
            "currency": "USD",
            "status": "ACTIVE",
        }


class FakeETradeGateway:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def get_session_state(self, *, environment: str):
        return {"configured": True, "connected": True}

    def list_accounts(self, *, environment: str, subject: str | None):
        return self.payload


def _service(
    *,
    repo: FakeTradeRepo | None = None,
    settings: TradeDeskSettings | None = None,
    alpaca_gateway: FakeAlpacaGateway | None = None,
    etrade_gateway: FakeETradeGateway | None = None,
    refresh_service: FakeRefreshService | None = None,
) -> BrokerAccountOnboardingService:
    trade_repo = repo or FakeTradeRepo()
    config_repo = FakeConfigurationRepo()
    return BrokerAccountOnboardingService(
        trade_repo,  # type: ignore[arg-type]
        config_repo,  # type: ignore[arg-type]
        FakeConfigurationService(),  # type: ignore[arg-type]
        FakeOperationsService(trade_repo),  # type: ignore[arg-type]
        settings or TradeDeskSettings(),
        refresh_service=refresh_service or FakeRefreshService(),  # type: ignore[arg-type]
        alpaca_gateway=alpaca_gateway,
        etrade_gateway=etrade_gateway,
    )


def test_alpaca_discovery_returns_onboardable_paper_candidate_with_posture() -> None:
    service = _service(
        settings=TradeDeskSettings(paper_execution_enabled=True),
        alpaca_gateway=FakeAlpacaGateway(),
    )

    response = service.list_candidates(
        provider="alpaca",
        environment="paper",
        actor="desk@example.com",
        granted_roles=[],
    )

    assert response.discoveryStatus == "completed"
    candidate = response.candidates[0]
    assert candidate.suggestedAccountId == "alpaca-paper"
    assert candidate.accountNumberMasked == "***6789"
    assert candidate.canOnboard is True
    assert candidate.allowedExecutionPostures == ["monitor_only", "paper"]


def test_existing_enabled_account_is_not_onboardable() -> None:
    repo = FakeTradeRepo(
        {
            "alpaca-paper": TradeAccountSeedState(
                accountId="alpaca-paper",
                enabled=True,
                provider="alpaca",
                environment="paper",
                providerAccountKey="provider-key",
            )
        }
    )
    service = _service(repo=repo, alpaca_gateway=FakeAlpacaGateway())

    response = service.list_candidates(
        provider="alpaca",
        environment="paper",
        actor=None,
        granted_roles=[],
    )

    assert response.candidates[0].state == "already_configured"
    assert response.candidates[0].canOnboard is False


def test_etrade_discovery_requires_provider_account_key() -> None:
    service = _service(
        etrade_gateway=FakeETradeGateway(
            {
                "AccountListResponse": {
                    "Accounts": {
                        "Account": [
                            {
                                "accountId": "12345678",
                                "accountName": "Taxable Brokerage",
                            }
                        ]
                    }
                }
            }
        )
    )

    response = service.list_candidates(
        provider="etrade",
        environment="sandbox",
        actor=None,
        granted_roles=[],
    )

    assert response.candidates[0].state == "unavailable"
    assert response.candidates[0].canOnboard is False
    assert "provider account key" in str(response.candidates[0].stateReason)


def test_onboard_disabled_account_reenables_and_audits_operator_reason() -> None:
    repo = FakeTradeRepo(
        {
            "alpaca-paper": TradeAccountSeedState(
                accountId="alpaca-paper",
                enabled=False,
                provider="alpaca",
                environment="paper",
                providerAccountKey="provider-key",
            )
        }
    )
    refresh_service = FakeRefreshService()
    service = _service(
        repo=repo,
        settings=TradeDeskSettings(paper_execution_enabled=True),
        alpaca_gateway=FakeAlpacaGateway(),
        refresh_service=refresh_service,
    )
    candidate = service.list_candidates(
        provider="alpaca",
        environment="paper",
        actor="desk@example.com",
        granted_roles=[],
    ).candidates[0]

    response = service.onboard_account(
        BrokerAccountOnboardingRequest(
            candidateId=candidate.candidateId,
            provider="alpaca",
            environment="paper",
            displayName="Core Paper",
            readiness="review",
            executionPosture="paper",
            initialRefresh=True,
            reason="Create monitored paper account for strategy dry runs.",
        ),
        actor="desk@example.com",
        request_id="request-1",
        granted_roles=["AssetAllocation.AccountPolicy.Write"],
    )

    assert response.created is False
    assert response.reenabled is True
    assert response.audit is not None
    assert response.audit.category == "onboarding"
    assert response.audit.after["operatorReason"] == "Create monitored paper account for strategy dry runs."
    assert repo.saved_account is not None
    assert repo.saved_account.capabilities.canSubmitPaper is True
    assert refresh_service.calls == [("alpaca-paper", "refresh", "manual")]


def test_live_posture_requires_live_role_and_live_allowlist() -> None:
    service = _service(
        settings=TradeDeskSettings(
            live_execution_enabled=True,
            live_kill_switch=False,
            live_account_allowlist=["alpaca-live"],
            live_required_roles=["AssetAllocation.TradeDesk.Live"],
        ),
        alpaca_gateway=FakeAlpacaGateway(),
    )
    candidate = service.list_candidates(
        provider="alpaca",
        environment="live",
        actor="desk@example.com",
        granted_roles=[],
    ).candidates[0]

    assert candidate.allowedExecutionPostures == ["monitor_only"]
    assert "Missing required live-trade roles" in candidate.blockedExecutionPostureReasons["live"]

    with pytest.raises(BrokerAccountOnboardingError) as exc_info:
        service.onboard_account(
            BrokerAccountOnboardingRequest(
                candidateId=candidate.candidateId,
                provider="alpaca",
                environment="live",
                displayName="Alpaca Live",
                readiness="review",
                executionPosture="live",
                initialRefresh=False,
                reason="Enable live account after approval.",
            ),
            actor="desk@example.com",
            request_id="request-1",
            granted_roles=[],
        )

    assert exc_info.value.status_code == 403
