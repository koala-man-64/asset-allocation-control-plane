from __future__ import annotations

from datetime import date

import pytest
from asset_allocation_contracts.portfolio import (
    FreshnessStatus,
    PortfolioAccount,
    PortfolioAccountDetailResponse,
    PortfolioAccountRevision,
    PortfolioAssignment,
    PortfolioForecastResponse,
    PortfolioHistoryResponse,
    PortfolioSnapshot,
    PortfolioHistoryPoint,
    PortfolioNextRebalanceResponse,
    RebalanceProposal,
)

from api.service.app import create_app
from core.portfolio_repository import PortfolioRepository
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


def _sample_account_detail() -> PortfolioAccountDetailResponse:
    return PortfolioAccountDetailResponse(
        account=PortfolioAccount(
            accountId="acct-core",
            name="Core Internal",
            description="Desk book",
            status="active",
            mode="internal_model_managed",
            accountingDepth="position_level",
            cadenceMode="strategy_native",
            baseCurrency="USD",
            benchmarkSymbol="SPY",
            inceptionDate=date(2026, 1, 2),
            mandate="Compound capital",
            latestRevision=1,
            activeRevision=1,
            activePortfolioName="core-book",
            activePortfolioVersion=2,
        ),
        revision=PortfolioAccountRevision(
            accountId="acct-core",
            version=1,
            name="Core Internal",
            description="Desk book",
            mandate="Compound capital",
            status="active",
            mode="internal_model_managed",
            accountingDepth="position_level",
            cadenceMode="strategy_native",
            baseCurrency="USD",
            benchmarkSymbol="SPY",
            inceptionDate=date(2026, 1, 2),
        ),
        activeAssignment=PortfolioAssignment(
            assignmentId="asn-1",
            accountId="acct-core",
            accountVersion=1,
            portfolioName="core-book",
            portfolioVersion=2,
            effectiveFrom=date(2026, 1, 2),
            status="active",
        ),
        recentLedgerEvents=[],
    )


async def test_create_portfolio_account_returns_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    expected = _sample_account_detail()

    monkeypatch.setattr(
        PortfolioRepository,
        "save_account",
        lambda self, *, account_id=None, payload, created_by=None: expected,
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/portfolio-accounts",
            json={
                "name": "Core Internal",
                "description": "Desk book",
                "mandate": "Compound capital",
                "baseCurrency": "USD",
                "benchmarkSymbol": "SPY",
                "inceptionDate": "2026-01-02",
                "openingCash": 250000,
            },
        )

    assert response.status_code == 200
    assert response.json()["account"]["accountId"] == "acct-core"
    assert response.json()["activeAssignment"]["portfolioName"] == "core-book"


async def test_preview_portfolio_rebalance_returns_contract_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    proposal = RebalanceProposal(
        proposalId="reb-1",
        accountId="acct-core",
        asOf=date(2026, 4, 1),
        portfolioName="core-book",
        portfolioVersion=2,
        blocked=False,
        estimatedCashImpact=0.0,
        estimatedTurnover=0.18,
        trades=[],
    )
    monkeypatch.setattr(
        PortfolioRepository,
        "create_rebalance_preview",
        lambda self, account_id, *, as_of, notes="": proposal,
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/portfolio-accounts/acct-core/rebalances/preview",
            json={"asOf": "2026-04-01", "notes": "rebalance"},
        )

    assert response.status_code == 200
    assert response.json()["proposalId"] == "reb-1"
    assert response.json()["estimatedTurnover"] == 0.18


async def test_get_portfolio_snapshot_round_trips_materialized_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    snapshot = PortfolioSnapshot(
        accountId="acct-core",
        accountName="Core Internal",
        asOf=date(2026, 4, 18),
        nav=280000.0,
        cash=25000.0,
        grossExposure=0.91,
        netExposure=0.91,
        sinceInceptionPnl=30000.0,
        sinceInceptionReturn=0.12,
        currentDrawdown=-0.03,
        maxDrawdown=-0.08,
        openAlertCount=1,
        freshness=[FreshnessStatus(domain="valuation", state="fresh")],
        slices=[],
    )
    monkeypatch.setattr(PortfolioRepository, "get_snapshot", lambda self, account_id: snapshot)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/portfolio-accounts/acct-core/snapshot")

    assert response.status_code == 200
    assert response.json()["accountName"] == "Core Internal"
    assert response.json()["freshness"][0]["domain"] == "valuation"


async def test_get_portfolio_history_uses_repository_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    history = [
        PortfolioHistoryPoint(
            asOf=date(2026, 4, 17),
            nav=275000.0,
            cash=30000.0,
            grossExposure=0.88,
            netExposure=0.88,
            cumulativeReturn=0.1,
        )
    ]
    monkeypatch.setattr(
        PortfolioRepository,
        "list_history",
        lambda self, account_id, *, limit: PortfolioHistoryResponse(points=history, totalPoints=1, truncated=False),
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/portfolio-accounts/acct-core/history")

    assert response.status_code == 200
    assert response.json()["totalPoints"] == 1
    assert response.json()["points"][0]["nav"] == 275000.0


async def test_get_portfolio_forecast_uses_repository_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    forecast = PortfolioForecastResponse(
        accountId="acct-core",
        asOf=date(2026, 4, 18),
        modelName="default-regime",
        modelVersion=3,
        benchmarkSymbol="SPY",
        horizon="3M",
        assumption="current",
        costDragOverrideBps=12,
        expectedReturnPct=4.2,
        expectedActiveReturnPct=1.1,
        downsidePct=-2.3,
        upsidePct=7.6,
        confidence="medium",
        confidenceLabel="Medium confidence",
        sampleSize=11,
        sampleMode="regime-conditioned",
        appliedRegimeCode="trending_up",
        notes=["Regime sample is moderately deep."],
    )
    monkeypatch.setattr(
        PortfolioRepository,
        "get_forecast",
        lambda self, account_id, **kwargs: forecast,
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/portfolio-accounts/acct-core/forecast",
            params={
                "modelName": "default-regime",
                "horizon": "3M",
                "assumption": "current",
                "costDragOverrideBps": 12,
            },
        )

    assert response.status_code == 200
    assert response.json()["modelName"] == "default-regime"
    assert response.json()["expectedReturnPct"] == 4.2


async def test_get_portfolio_next_rebalance_uses_repository_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    next_rebalance = PortfolioNextRebalanceResponse(
        accountId="acct-core",
        asOf=date(2026, 4, 18),
        rebalanceCadence="weekly",
        anchorText="Monday close",
        nextDate=date(2026, 4, 20),
        inferred=False,
        basis="anchor",
        reason="Weekly cadence is anchored to the parsed weekday in the rebalance anchor.",
    )
    monkeypatch.setattr(
        PortfolioRepository,
        "get_next_rebalance",
        lambda self, account_id, *, as_of=None: next_rebalance,
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/portfolio-accounts/acct-core/next-rebalance",
            params={"asOf": "2026-04-18"},
        )

    assert response.status_code == 200
    assert response.json()["anchorText"] == "Monday close"
    assert response.json()["nextDate"] == "2026-04-20"
