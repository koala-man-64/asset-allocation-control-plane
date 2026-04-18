from __future__ import annotations

import pytest
from psycopg import OperationalError

from api.endpoints import internal as internal_routes
from api.service.app import create_app
from api.service.auth import AuthContext
from core.backtest_repository import BacktestRepository, BacktestResultsNotReadyError
from core.regime_repository import RegimeRepository
from core.strategy_repository import StrategyRepository
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


async def test_internal_strategies_list_returns_repository_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        StrategyRepository,
        "list_strategies",
        lambda self: [
            {
                "name": "momentum",
                "type": "configured",
                "description": "Monthly momentum",
                "updated_at": "2026-03-08T00:00:00Z",
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/internal/strategies")

    assert response.status_code == 200
    assert response.json()[0]["name"] == "momentum"


async def test_internal_regime_current_uses_query_params(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    calls: list[tuple[str, int | None]] = []

    def _get_regime_latest(self, *, model_name: str, model_version: int | None = None):
        calls.append((model_name, model_version))
        return {
            "as_of_date": "2026-03-07",
            "effective_from_date": "2026-03-10",
            "model_name": model_name,
            "model_version": model_version or 1,
            "regime_code": "trending_bull",
            "regime_status": "confirmed",
            "halt_flag": False,
        }

    monkeypatch.setattr(RegimeRepository, "get_regime_latest", _get_regime_latest)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/internal/regimes/current", params={"modelName": "fast-regime", "modelVersion": 3})

    assert response.status_code == 200
    assert response.json()["model_name"] == "fast-regime"
    assert calls == [("fast-regime", 3)]


async def test_internal_backtest_claim_and_missing_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "claim_next_run",
        lambda self, execution_name=None: {"run_id": "run-123", "execution_name": execution_name},
    )
    monkeypatch.setattr(BacktestRepository, "get_run", lambda self, run_id: None)

    app = create_app()
    async with get_test_client(app) as client:
        claim = await client.post("/api/internal/backtests/runs/claim", json={"executionName": "job-run-7"})
        missing = await client.get("/api/internal/backtests/runs/missing-run")

    assert claim.status_code == 200
    assert claim.json()["run"]["run_id"] == "run-123"
    assert missing.status_code == 404



async def test_internal_complete_backtest_run_returns_409_when_results_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {"run_id": run_id, "status": "running", "results_ready_at": None},
    )

    def _raise_not_ready(self, run_id: str, *, summary=None):  # type: ignore[no-untyped-def]
        raise BacktestResultsNotReadyError("results not ready")

    monkeypatch.setattr(BacktestRepository, "complete_run", _raise_not_ready)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/internal/backtests/runs/run-1/complete", json={"summary": {"sharpe": 1.2}})

    assert response.status_code == 409


async def test_internal_backtest_ready_checks_auth_and_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    auth_calls: list[str] = []
    probe_calls: list[str] = []

    def _validate_auth(request):  # type: ignore[no-untyped-def]
        auth_calls.append(request.url.path)
        return AuthContext(mode="anonymous", subject=None, claims={})

    monkeypatch.setattr(internal_routes, "validate_auth", _validate_auth)
    monkeypatch.setattr(internal_routes, "_probe_postgres", lambda dsn: probe_calls.append(dsn))

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/internal/backtests/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert auth_calls == ["/api/internal/backtests/ready"]
    assert probe_calls == ["postgresql://test:test@localhost:5432/asset_allocation"]


async def test_internal_backtest_ready_returns_503_when_postgres_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        internal_routes,
        "validate_auth",
        lambda request: AuthContext(mode="anonymous", subject=None, claims={}),  # type: ignore[arg-type]
    )

    def _raise_probe(_dsn: str) -> None:
        raise OperationalError("db unavailable")

    monkeypatch.setattr(internal_routes, "_probe_postgres", _raise_probe)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/internal/backtests/ready")

    assert response.status_code == 503


async def test_internal_results_reconcile_forwards_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    calls: list[tuple[str, bool]] = []

    def _reconcile(dsn: str, *, dry_run: bool = False) -> dict[str, object]:
        calls.append((dsn, dry_run))
        return {"dryRun": dry_run, "rankingDirtyCount": 1, "errorCount": 0}

    monkeypatch.setattr(internal_routes, "reconcile_results_freshness", _reconcile)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/internal/results/reconcile", json={"dryRun": True})

    assert response.status_code == 200
    assert response.json()["dryRun"] is True
    assert calls == [("postgresql://test:test@localhost:5432/asset_allocation", True)]


async def test_internal_ranking_refresh_routes_delegate_to_freshness_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    claim_calls: list[str | None] = []
    complete_calls: list[tuple[str, str, str | None]] = []
    fail_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        internal_routes,
        "claim_next_ranking_refresh",
        lambda dsn, execution_name=None: claim_calls.append(execution_name)
        or {"strategyName": "alpha", "claimToken": "claim-1", "startDate": "2026-03-01", "endDate": "2026-03-02"},
    )
    monkeypatch.setattr(
        internal_routes,
        "complete_ranking_refresh",
        lambda dsn, *, strategy_name, claim_token, run_id=None, dependency_fingerprint=None, dependency_state=None: complete_calls.append(
            (strategy_name, claim_token, run_id)
        )
        or {"status": "ok"},
    )
    monkeypatch.setattr(
        internal_routes,
        "fail_ranking_refresh",
        lambda dsn, *, strategy_name, claim_token, error: fail_calls.append((strategy_name, error)) or {"status": "ok"},
    )

    app = create_app()
    async with get_test_client(app) as client:
        claim_response = await client.post("/api/internal/rankings/refresh/claim", json={"executionName": "job-7"})
        complete_response = await client.post(
            "/api/internal/rankings/refresh/alpha/complete",
            json={"claimToken": "claim-1", "runId": "run-1", "dependencyFingerprint": "fp-1", "dependencyState": {}},
        )
        fail_response = await client.post(
            "/api/internal/rankings/refresh/alpha/fail",
            json={"claimToken": "claim-1", "error": "boom"},
        )

    assert claim_response.status_code == 200
    assert claim_response.json()["work"]["strategyName"] == "alpha"
    assert complete_response.status_code == 200
    assert fail_response.status_code == 200
    assert claim_calls == ["job-7"]
    assert complete_calls == [("alpha", "claim-1", "run-1")]
    assert fail_calls == [("alpha", "boom")]
