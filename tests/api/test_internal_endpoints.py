from __future__ import annotations

import pytest
from psycopg import OperationalError
from asset_allocation_contracts.symbol_enrichment import (
    SymbolCleanupRunSummary,
    SymbolCleanupWorkItem,
    SymbolEnrichmentResolveResponse,
    SymbolProfileValues,
)
from api.service.intraday_contracts_compat import (
    IntradayMonitorRunSummary,
    IntradaySymbolStatus,
    IntradayRefreshBatchSummary,
    IntradayWatchlistDetail,
)

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
            "active_regimes": ["trending_up"],
            "signals": [
                {
                    "regime_code": "trending_up",
                    "display_name": "Trending (Up)",
                    "signal_state": "active",
                    "score": 0.9,
                    "activation_threshold": 0.6,
                    "is_active": True,
                    "matched_rule_id": "trending_up",
                    "evidence": {},
                }
            ],
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


async def test_internal_symbol_cleanup_routes_delegate_to_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ENABLED", "true")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ALLOWED_JOBS", "symbol-cleanup-job")

    claim_calls: list[str | None] = []
    complete_calls: list[tuple[str, dict[str, object] | None]] = []
    fail_calls: list[tuple[str, str]] = []
    get_run_calls: list[str] = []

    work_item = SymbolCleanupWorkItem(
        workId="work-1",
        runId="run-1",
        symbol="AAPL",
        status="claimed",
        requestedFields=["sector_norm"],
        attemptCount=1,
        executionName="exec-1",
    )
    run_summary = SymbolCleanupRunSummary(
        runId="run-1",
        status="running",
        mode="full_reconcile",
        queuedCount=0,
        claimedCount=1,
        completedCount=0,
        failedCount=0,
    )

    monkeypatch.setattr(
        internal_routes,
        "claim_next_symbol_cleanup_work",
        lambda dsn, execution_name=None: claim_calls.append(execution_name) or work_item,
    )
    monkeypatch.setattr(
        internal_routes,
        "complete_symbol_cleanup_work",
        lambda dsn, *, work_id, result: complete_calls.append((work_id, result)) or run_summary,
    )
    monkeypatch.setattr(
        internal_routes,
        "fail_symbol_cleanup_work",
        lambda dsn, *, work_id, error: fail_calls.append((work_id, error)) or run_summary,
    )
    monkeypatch.setattr(
        internal_routes,
        "get_symbol_cleanup_run",
        lambda dsn, run_id: get_run_calls.append(run_id) or run_summary,
    )

    app = create_app()
    headers = {"X-Caller-Job": "symbol-cleanup-job"}
    async with get_test_client(app) as client:
        claim_response = await client.post("/api/internal/symbol-cleanup/claim", json={"executionName": "exec-1"}, headers=headers)
        complete_response = await client.post(
            "/api/internal/symbol-cleanup/work-1/complete",
            json={
                "result": {
                    "symbol": "AAPL",
                    "profile": {"sector_norm": "Technology"},
                    "model": "gpt-5.4-mini",
                    "confidence": 0.91,
                    "sourceFingerprint": "fp-1",
                    "warnings": [],
                }
            },
            headers=headers,
        )
        fail_response = await client.post(
            "/api/internal/symbol-cleanup/work-1/fail",
            json={"error": "boom"},
            headers=headers,
        )
        run_response = await client.get("/api/internal/symbol-cleanup/runs/run-1", headers=headers)

    assert claim_response.status_code == 200
    assert claim_response.json()["work"]["symbol"] == "AAPL"
    assert complete_response.status_code == 200
    assert fail_response.status_code == 200
    assert run_response.status_code == 200
    assert claim_calls == ["exec-1"]
    assert complete_calls == [
        (
            "work-1",
            {
                "symbol": "AAPL",
                "profile": {
                    "security_type_norm": None,
                    "exchange_mic": None,
                    "country_of_risk": None,
                    "sector_norm": "Technology",
                    "industry_group_norm": None,
                    "industry_norm": None,
                    "is_adr": None,
                    "is_etf": None,
                    "is_cef": None,
                    "is_preferred": None,
                    "share_class": None,
                    "listing_status_norm": None,
                    "issuer_summary_short": None,
                },
                "model": "gpt-5.4-mini",
                "confidence": 0.91,
                "sourceFingerprint": "fp-1",
                "warnings": [],
            },
        )
    ]
    assert fail_calls == [("work-1", "boom")]
    assert get_run_calls == ["run-1"]


async def test_internal_symbol_enrichment_resolve_checks_confidence_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ENABLED", "true")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ALLOWED_JOBS", "symbol-cleanup-job")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_CONFIDENCE_MIN", "0.8")

    async def _resolve(**_kwargs):  # type: ignore[no-untyped-def]
        return SymbolEnrichmentResolveResponse(
            symbol="AAPL",
            profile=SymbolProfileValues(sector_norm="Technology"),
            model="gpt-5.4-mini",
            confidence=0.91,
            sourceFingerprint="fp-1",
            warnings=[],
        )

    monkeypatch.setattr(internal_routes, "resolve_symbol_profile_via_ai", _resolve)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/internal/symbol-enrichment/resolve",
            headers={"X-Caller-Job": "symbol-cleanup-job"},
            json={
                "symbol": "AAPL",
                "overwriteMode": "fill_missing",
                "requestedFields": ["sector_norm"],
                "providerFacts": {"symbol": "AAPL"},
                "currentProfile": {},
            },
        )

    assert response.status_code == 200
    assert response.json()["profile"]["sector_norm"] == "Technology"


async def test_internal_intraday_routes_delegate_to_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("INTRADAY_MONITOR_ENABLED", "true")
    monkeypatch.setenv("INTRADAY_MONITOR_ALLOWED_JOBS", "intraday-monitor-job,intraday-refresh-job")

    run = IntradayMonitorRunSummary(
        runId="run-1",
        watchlistId="watch-1",
        watchlistName="Tech Momentum",
        triggerKind="manual",
        status="claimed",
        forceRefresh=True,
        symbolCount=2,
        observedSymbolCount=1,
        eligibleRefreshCount=1,
        refreshBatchCount=1,
    )
    watchlist = IntradayWatchlistDetail(
        watchlistId="watch-1",
        name="Tech Momentum",
        description="Core intraday list",
        enabled=True,
        symbolCount=2,
        pollIntervalMinutes=5,
        refreshCooldownMinutes=15,
        autoRefreshEnabled=True,
        marketSession="us_equities_regular",
        symbols=["AAPL", "MSFT"],
    )
    batch = IntradayRefreshBatchSummary(
        batchId="batch-1",
        runId="run-1",
        watchlistId="watch-1",
        watchlistName="Tech Momentum",
        domain="market",
        bucketLetter="A",
        status="claimed",
        symbols=["AAPL"],
        symbolCount=1,
    )

    monitor_claim_calls: list[str | None] = []
    refresh_claim_calls: list[str | None] = []
    realtime_events: list[tuple[str, str, dict[str, object] | None]] = []

    monkeypatch.setattr(
        internal_routes,
        "claim_next_intraday_monitor_run",
        lambda dsn, execution_name=None: monitor_claim_calls.append(execution_name) or (run, watchlist, "claim-1"),
    )
    monkeypatch.setattr(
        internal_routes,
        "list_intraday_symbol_status",
        lambda dsn, *, watchlist_id=None, q=None, limit=100, offset=0: (
            2,
            [
                IntradaySymbolStatus(
                    watchlistId="watch-1",
                    symbol="AAPL",
                    monitorStatus="observed",
                    lastObservedPrice=213.42,
                )
            ],
        ),
    )
    monkeypatch.setattr(
        internal_routes,
        "complete_intraday_monitor_run",
        lambda dsn, *, run_id, claim_token, symbol_statuses, events, refresh_symbols: run,
    )
    monkeypatch.setattr(
        internal_routes,
        "fail_intraday_monitor_run",
        lambda dsn, *, run_id, claim_token, error: run,
    )
    monkeypatch.setattr(
        internal_routes,
        "claim_next_intraday_refresh_batch",
        lambda dsn, execution_name=None: refresh_claim_calls.append(execution_name) or (batch, "refresh-claim-1"),
    )
    monkeypatch.setattr(
        internal_routes,
        "complete_intraday_refresh_batch",
        lambda dsn, *, batch_id, claim_token: batch,
    )
    monkeypatch.setattr(
        internal_routes,
        "fail_intraday_refresh_batch",
        lambda dsn, *, batch_id, claim_token, error: batch,
    )
    monkeypatch.setattr(
        internal_routes,
        "_emit_intraday_realtime",
        lambda topic, event_type, payload=None: realtime_events.append((topic, event_type, payload)),
    )

    app = create_app()
    monitor_headers = {"X-Caller-Job": "intraday-monitor-job"}
    refresh_headers = {"X-Caller-Job": "intraday-refresh-job"}
    async with get_test_client(app) as client:
        claim_response = await client.post(
            "/api/internal/intraday-monitor/claim",
            json={"executionName": "monitor-exec-1"},
            headers=monitor_headers,
        )
        complete_response = await client.post(
            "/api/internal/intraday-monitor/runs/run-1/complete",
            json={
                "claimToken": "claim-1",
                "symbolStatuses": [
                    {
                        "symbol": "AAPL",
                        "monitorStatus": "refresh_queued",
                        "lastObservedPrice": 213.42,
                    }
                ],
                "events": [
                    {
                        "eventType": "snapshot_polled",
                        "severity": "info",
                        "message": "Fetched latest snapshot.",
                        "details": {"source": "massive"},
                    }
                ],
                "refreshSymbols": ["AAPL"],
            },
            headers=monitor_headers,
        )
        fail_response = await client.post(
            "/api/internal/intraday-monitor/runs/run-1/fail",
            json={"claimToken": "claim-1", "error": "boom"},
            headers=monitor_headers,
        )
        refresh_claim_response = await client.post(
            "/api/internal/intraday-refresh/claim",
            json={"executionName": "refresh-exec-1"},
            headers=refresh_headers,
        )
        refresh_complete_response = await client.post(
            "/api/internal/intraday-refresh/batches/batch-1/complete",
            json={"claimToken": "refresh-claim-1"},
            headers=refresh_headers,
        )
        refresh_fail_response = await client.post(
            "/api/internal/intraday-refresh/batches/batch-1/fail",
            json={"claimToken": "refresh-claim-1", "error": "boom"},
            headers=refresh_headers,
        )

    assert claim_response.status_code == 200
    assert claim_response.json()["claimToken"] == "claim-1"
    assert claim_response.json()["watchlist"]["symbols"] == ["AAPL", "MSFT"]
    assert claim_response.json()["currentSymbolStatuses"][0]["symbol"] == "AAPL"
    assert complete_response.status_code == 200
    assert fail_response.status_code == 200
    assert refresh_claim_response.status_code == 200
    assert refresh_claim_response.json()["claimToken"] == "refresh-claim-1"
    assert refresh_complete_response.status_code == 200
    assert refresh_fail_response.status_code == 200
    assert monitor_claim_calls == ["monitor-exec-1"]
    assert refresh_claim_calls == ["refresh-exec-1"]
    assert realtime_events == [
        (
            "intraday-monitor",
            "run.claimed",
            {"run": run.model_dump(mode="json")},
        ),
        (
            "intraday-monitor",
            "run.completed",
            {"run": run.model_dump(mode="json")},
        ),
        (
            "intraday-monitor",
            "run.failed",
            {"run": run.model_dump(mode="json")},
        ),
        (
            "intraday-refresh",
            "refresh.claimed",
            {"batch": batch.model_dump(mode="json")},
        ),
        (
            "intraday-refresh",
            "refresh.completed",
            {"batch": batch.model_dump(mode="json")},
        ),
        (
            "intraday-refresh",
            "refresh.failed",
            {"batch": batch.model_dump(mode="json")},
        ),
    ]


async def test_internal_intraday_ready_checks_job_access_and_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("INTRADAY_MONITOR_ENABLED", "true")
    monkeypatch.setenv("INTRADAY_MONITOR_ALLOWED_JOBS", "intraday-monitor-job")

    probe_calls: list[str] = []
    monkeypatch.setattr(internal_routes, "_probe_postgres", lambda dsn: probe_calls.append(dsn))

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/internal/intraday/ready",
            headers={"X-Caller-Job": "intraday-monitor-job"},
        )

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert probe_calls == ["postgresql://test:test@localhost:5432/asset_allocation"]
