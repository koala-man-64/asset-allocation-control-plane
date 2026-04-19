from __future__ import annotations

import pytest
from asset_allocation_contracts.symbol_enrichment import (
    SymbolCleanupRunSummary,
    SymbolEnrichmentSummaryResponse,
    SymbolEnrichmentSymbolDetailResponse,
    SymbolEnrichmentSymbolListItem,
    SymbolProfileOverride,
    SymbolProviderFacts,
)

from api.endpoints import system as system_routes
from api.service.app import create_app
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_symbol_enrichment_system_routes_return_operator_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ENABLED", "true")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_ALLOWED_JOBS", "symbol-cleanup-job")
    monkeypatch.setenv("SYMBOL_ENRICHMENT_MAX_SYMBOLS_PER_RUN", "250")

    summary = SymbolEnrichmentSummaryResponse(
        backlogCount=3,
        lastRun=SymbolCleanupRunSummary(runId="run-1", status="completed", mode="full_reconcile"),
        activeRun=None,
        validationFailureCount=1,
        lockCount=2,
    )
    runs = [
        SymbolCleanupRunSummary(
            runId="run-1",
            status="completed",
            mode="full_reconcile",
            completedCount=2,
        )
    ]
    symbols = [
        SymbolEnrichmentSymbolListItem(
            symbol="AAPL",
            name="Apple Inc.",
            status="accepted",
            sourceKind="ai",
            missingFieldCount=1,
            lockedFieldCount=0,
            dataCompletenessScore=0.92,
        )
    ]
    detail = SymbolEnrichmentSymbolDetailResponse(
        providerFacts=SymbolProviderFacts(symbol="AAPL", name="Apple Inc."),
        currentProfile=None,
        overrides=[],
        history=[],
    )
    override = SymbolProfileOverride(
        symbol="AAPL",
        fieldName="sector_norm",
        value="Technology",
        isLocked=True,
        updatedBy="tester@example.com",
    )

    enqueue_calls: list[dict[str, object]] = []

    monkeypatch.setattr(system_routes, "get_symbol_enrichment_summary", lambda dsn: summary)
    monkeypatch.setattr(system_routes, "list_symbol_cleanup_runs", lambda dsn, limit=50, offset=0: runs)
    monkeypatch.setattr(system_routes, "list_symbol_enrichment_symbols", lambda dsn, q=None, limit=100, offset=0: (1, symbols))
    monkeypatch.setattr(system_routes, "get_symbol_enrichment_symbol_detail", lambda dsn, symbol: detail)
    monkeypatch.setattr(
        system_routes,
        "enqueue_symbol_cleanup_run",
        lambda dsn, *, symbols, full_scan, overwrite_mode, max_symbols: enqueue_calls.append(
            {
                "symbols": symbols,
                "full_scan": full_scan,
                "overwrite_mode": overwrite_mode,
                "max_symbols": max_symbols,
            }
        )
        or runs[0],
    )
    monkeypatch.setattr(system_routes, "upsert_symbol_profile_overrides", lambda dsn, *, symbol, overrides: overrides)

    app = create_app()
    async with get_test_client(app) as client:
        summary_response = await client.get("/api/system/symbol-enrichment/summary")
        runs_response = await client.get("/api/system/symbol-enrichment/runs")
        symbols_response = await client.get("/api/system/symbol-enrichment/symbols")
        detail_response = await client.get("/api/system/symbol-enrichment/symbols/AAPL")
        enqueue_response = await client.post(
            "/api/system/symbol-enrichment/enqueue",
            json={
                "symbols": ["AAPL", "MSFT"],
                "fullScan": False,
                "overwriteMode": "full_reconcile",
                "maxSymbols": 500,
            },
        )
        overrides_response = await client.put(
            "/api/system/symbol-enrichment/overrides/AAPL",
            json=[override.model_dump(mode="json")],
        )

    assert summary_response.status_code == 200
    assert summary_response.json()["backlogCount"] == 3
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["runId"] == "run-1"
    assert symbols_response.status_code == 200
    assert symbols_response.headers["X-Total-Count"] == "1"
    assert symbols_response.json()[0]["symbol"] == "AAPL"
    assert detail_response.status_code == 200
    assert detail_response.json()["providerFacts"]["symbol"] == "AAPL"
    assert enqueue_response.status_code == 200
    assert overrides_response.status_code == 200
    assert overrides_response.json()[0]["fieldName"] == "sector_norm"
    assert enqueue_calls == [
        {
            "symbols": ["AAPL", "MSFT"],
            "full_scan": False,
            "overwrite_mode": "full_reconcile",
            "max_symbols": 250,
        }
    ]
