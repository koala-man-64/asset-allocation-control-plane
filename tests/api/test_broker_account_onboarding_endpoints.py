from __future__ import annotations

from datetime import datetime, timezone

import pytest
from asset_allocation_contracts.broker_accounts import (
    BrokerAccountOnboardingCandidate,
    BrokerAccountOnboardingCandidateListResponse,
    BrokerAccountOnboardingResponse,
    BrokerAccountSummary,
    BrokerConnectionHealth,
)

from api.service.app import create_app
from api.service.broker_account_onboarding_service import (
    BrokerAccountOnboardingError,
    BrokerAccountOnboardingService,
)
from tests.api._client import get_test_client


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _summary() -> BrokerAccountSummary:
    return BrokerAccountSummary(
        accountId="alpaca-paper",
        broker="alpaca",
        name="Alpaca Paper",
        accountNumberMasked="***6789",
        baseCurrency="USD",
        overallStatus="warning",
        tradeReadiness="review",
        connectionHealth=BrokerConnectionHealth(
            overallStatus="warning",
            authStatus="authenticated",
            connectionState="degraded",
            syncStatus="never_synced",
            lastCheckedAt=_now(),
        ),
        equity=0,
        cash=0,
        buyingPower=0,
        openPositionCount=0,
        openOrderCount=0,
    )


@pytest.mark.asyncio
async def test_broker_account_onboarding_endpoints_return_contract_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def list_candidates(self, *, provider, environment, actor, granted_roles):
        return BrokerAccountOnboardingCandidateListResponse(
            candidates=[
                BrokerAccountOnboardingCandidate(
                    candidateId="alpaca:paper:123",
                    provider=provider,
                    environment=environment,
                    suggestedAccountId="alpaca-paper",
                    displayName="Alpaca Paper",
                    accountNumberMasked="***6789",
                    baseCurrency="USD",
                    allowedExecutionPostures=["monitor_only", "paper"],
                    canOnboard=True,
                )
            ],
            generatedAt=_now(),
        )

    def onboard_account(self, payload, *, actor, request_id, granted_roles):
        return BrokerAccountOnboardingResponse(
            account=_summary(),
            configuration=None,
            created=True,
            reenabled=False,
            refreshAction=None,
            audit=None,
            message="Broker account onboarded.",
            generatedAt=_now(),
        )

    monkeypatch.setattr(BrokerAccountOnboardingService, "list_candidates", list_candidates)
    monkeypatch.setattr(BrokerAccountOnboardingService, "onboard_account", onboard_account)

    app = create_app()
    async with get_test_client(app) as client:
        candidates_response = await client.get(
            "/api/broker-accounts/onboarding/candidates",
            params={"provider": "alpaca", "environment": "paper"},
        )
        onboard_response = await client.post(
            "/api/broker-accounts/onboarding",
            json={
                "candidateId": "alpaca:paper:123",
                "provider": "alpaca",
                "environment": "paper",
                "displayName": "Alpaca Paper",
                "readiness": "review",
                "executionPosture": "paper",
                "initialRefresh": True,
                "reason": "Create monitored paper account.",
            },
        )

    assert candidates_response.status_code == 200
    assert candidates_response.headers["cache-control"] == "no-store"
    assert candidates_response.json()["candidates"][0]["suggestedAccountId"] == "alpaca-paper"

    assert onboard_response.status_code == 200
    assert onboard_response.headers["cache-control"] == "no-store"
    assert onboard_response.json()["account"]["accountId"] == "alpaca-paper"
    assert onboard_response.json()["created"] is True


@pytest.mark.asyncio
async def test_broker_account_onboarding_endpoint_maps_service_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def onboard_account(self, payload, *, actor, request_id, granted_roles):
        raise BrokerAccountOnboardingError(409, "Account 'alpaca-paper' is already enabled.")

    monkeypatch.setattr(BrokerAccountOnboardingService, "onboard_account", onboard_account)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/broker-accounts/onboarding",
            json={
                "candidateId": "alpaca:paper:123",
                "provider": "alpaca",
                "environment": "paper",
                "displayName": "Alpaca Paper",
                "readiness": "review",
                "executionPosture": "paper",
                "initialRefresh": True,
                "reason": "Create monitored paper account.",
            },
        )

    assert response.status_code == 409
    assert response.headers["cache-control"] == "no-store"
    assert "already enabled" in response.json()["detail"]


def test_broker_account_onboarding_openapi_includes_static_routes_before_account_route() -> None:
    schema = create_app().openapi()
    paths = list(schema["paths"])

    assert "/api/broker-accounts/onboarding/candidates" in paths
    assert "/api/broker-accounts/onboarding" in paths
    assert paths.index("/api/broker-accounts/onboarding/candidates") < paths.index("/api/broker-accounts/{account_id}")
