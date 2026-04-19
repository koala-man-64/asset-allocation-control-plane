from __future__ import annotations

import pytest

from api.endpoints import portfolio_internal as internal_routes
from api.service.app import create_app
from core.portfolio_repository import PortfolioRepository
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


async def test_internal_portfolio_materialization_claim_delegates_to_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        PortfolioRepository,
        "claim_next_materialization",
        lambda self, execution_name=None: {"accountId": "acct-core", "claimToken": "claim-1"},
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/internal/portfolios/materializations/claim", json={"executionName": "job-1"})

    assert response.status_code == 200
    assert response.json()["work"]["accountId"] == "acct-core"


async def test_internal_portfolio_materialization_ready_probes_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    calls: list[str] = []
    monkeypatch.setattr(internal_routes, "_probe_postgres", lambda dsn: calls.append(dsn))

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/internal/portfolios/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert calls == ["postgresql://test:test@localhost:5432/asset_allocation"]


async def test_internal_portfolio_materialization_fail_maps_lookup_errors_to_409(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def _raise_missing(self, account_id: str, *, claim_token: str, error: str):  # type: ignore[no-untyped-def]
        raise LookupError("claim missing")

    monkeypatch.setattr(PortfolioRepository, "fail_materialization", _raise_missing)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/internal/portfolios/materializations/acct-core/fail",
            json={"claimToken": "claim-1", "error": "boom"},
        )

    assert response.status_code == 409


async def test_internal_portfolio_materialization_bundle_compat_path_returns_repository_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        PortfolioRepository,
        "get_materialization_bundle",
        lambda self, account_id, *, claim_token=None: {
            "account": {
                "accountId": account_id,
                "name": "Core",
                "description": "",
                "status": "active",
                "mode": "internal_model_managed",
                "accountingDepth": "position_level",
                "cadenceMode": "strategy_native",
                "baseCurrency": "USD",
                "inceptionDate": "2026-01-02",
            },
            "ledgerEvents": [],
            "alerts": [],
            "freshness": [],
            "claimToken": claim_token,
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/internal/portfolio-materializations/accounts/acct-core/bundle",
            params={"claimToken": "claim-1"},
        )

    assert response.status_code == 200
    assert response.json()["account"]["accountId"] == "acct-core"
    assert response.json()["claimToken"] == "claim-1"
