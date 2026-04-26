from __future__ import annotations

import pytest

from api.endpoints import intraday as intraday_routes
from api.service.app import create_app
from api.service.intraday_contracts_compat import (
    IntradayMonitorEvent,
    IntradayMonitorRunSummary,
    IntradayRefreshBatchSummary,
    IntradaySymbolStatus,
    IntradayWatchlistDetail,
    IntradayWatchlistSymbolAppendResponse,
    IntradayWatchlistSummary,
)
from tests.api._client import get_test_client


pytestmark = pytest.mark.asyncio


async def test_intraday_public_routes_return_operator_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("INTRADAY_MONITOR_ENABLED", "true")
    monkeypatch.setenv("INTRADAY_MONITOR_ALLOWED_JOBS", "intraday-monitor-job,intraday-refresh-job")

    watchlist_summary = IntradayWatchlistSummary(
        watchlistId="watch-1",
        name="Tech Momentum",
        description="Core intraday list",
        enabled=True,
        symbolCount=2,
        pollIntervalMinutes=5,
        refreshCooldownMinutes=15,
        autoRefreshEnabled=True,
        marketSession="us_equities_regular",
    )
    watchlist_detail = IntradayWatchlistDetail(
        **watchlist_summary.model_dump(mode="json"),
        symbols=["AAPL", "MSFT"],
    )
    run = IntradayMonitorRunSummary(
        runId="run-1",
        watchlistId="watch-1",
        watchlistName="Tech Momentum",
        triggerKind="manual",
        status="queued",
        forceRefresh=True,
        symbolCount=2,
        observedSymbolCount=0,
        eligibleRefreshCount=0,
        refreshBatchCount=0,
    )
    append_run = IntradayMonitorRunSummary(
        runId="run-append",
        watchlistId="watch-1",
        watchlistName="Tech Momentum",
        triggerKind="manual",
        status="queued",
        forceRefresh=False,
        symbolCount=3,
        observedSymbolCount=0,
        eligibleRefreshCount=0,
        refreshBatchCount=0,
    )
    append_watchlist_payload = watchlist_summary.model_dump(mode="json")
    append_watchlist_payload["symbolCount"] = 3
    append_response = IntradayWatchlistSymbolAppendResponse(
        watchlist=IntradayWatchlistDetail(
            **append_watchlist_payload,
            symbols=["AAPL", "MSFT", "NVDA"],
        ),
        addedSymbols=["NVDA"],
        alreadyPresentSymbols=["AAPL"],
        queuedRun=append_run,
    )
    event = IntradayMonitorEvent(
        eventId="event-1",
        runId="run-1",
        watchlistId="watch-1",
        symbol="AAPL",
        eventType="snapshot_polled",
        severity="info",
        message="Fetched latest snapshot.",
        details={"source": "massive"},
    )
    status_item = IntradaySymbolStatus(
        watchlistId="watch-1",
        symbol="AAPL",
        monitorStatus="refresh_queued",
        lastObservedPrice=213.42,
        lastRunId="run-1",
    )
    batch = IntradayRefreshBatchSummary(
        batchId="batch-1",
        runId="run-1",
        watchlistId="watch-1",
        watchlistName="Tech Momentum",
        domain="market",
        bucketLetter="A",
        status="queued",
        symbols=["AAPL"],
        symbolCount=1,
    )

    emitted: list[tuple[str, str]] = []
    deleted_watchlists: list[str] = []
    append_calls: list[dict[str, object]] = []

    monkeypatch.setattr(intraday_routes, "list_intraday_watchlists", lambda dsn, limit=100, offset=0: [watchlist_summary])
    monkeypatch.setattr(intraday_routes, "upsert_intraday_watchlist", lambda dsn, watchlist_id=None, payload=None: watchlist_detail)
    monkeypatch.setattr(intraday_routes, "get_intraday_watchlist", lambda dsn, watchlist_id: watchlist_detail)
    monkeypatch.setattr(intraday_routes, "enqueue_intraday_watchlist_run", lambda dsn, watchlist_id: run)
    monkeypatch.setattr(
        intraday_routes,
        "append_intraday_watchlist_symbols",
        lambda dsn, watchlist_id, payload, actor=None, request_id=None: append_calls.append(
            {
                "watchlist_id": watchlist_id,
                "symbols": payload.symbols,
                "queueRun": payload.queueRun,
                "reason": payload.reason,
                "actor": actor,
                "request_id": request_id,
            }
        )
        or append_response,
    )
    monkeypatch.setattr(intraday_routes, "list_intraday_symbol_status", lambda dsn, watchlist_id=None, q=None, limit=100, offset=0: (1, [status_item]))
    monkeypatch.setattr(
        intraday_routes,
        "get_intraday_health_summary",
        lambda dsn: {
            "watchlistCount": 1,
            "enabledWatchlistCount": 1,
            "dueRunBacklogCount": 1,
            "failedRunCount": 0,
            "staleSymbolCount": 1,
            "refreshBatchBacklogAgeSeconds": 120.0,
            "latestMonitorRun": run.model_dump(mode="json"),
            "latestRefreshBatch": batch.model_dump(mode="json"),
        },
    )
    monkeypatch.setattr(intraday_routes, "list_intraday_monitor_runs", lambda dsn, watchlist_id=None, limit=100, offset=0: [run])
    monkeypatch.setattr(
        intraday_routes,
        "list_intraday_monitor_events",
        lambda dsn, watchlist_id=None, run_id=None, limit=100, offset=0: [event],
    )
    monkeypatch.setattr(
        intraday_routes,
        "list_intraday_refresh_batches",
        lambda dsn, watchlist_id=None, limit=100, offset=0: [batch],
    )
    monkeypatch.setattr(
        intraday_routes,
        "delete_intraday_watchlist",
        lambda dsn, watchlist_id: deleted_watchlists.append(watchlist_id),
    )
    monkeypatch.setattr(
        intraday_routes,
        "_emit_realtime",
        lambda topic, event_type, payload=None: emitted.append((topic, event_type)),
    )

    app = create_app()
    async with get_test_client(app) as client:
        list_response = await client.get("/api/intraday/watchlists")
        create_response = await client.post(
            "/api/intraday/watchlists",
            json={
                "name": "Tech Momentum",
                "description": "Core intraday list",
                "enabled": True,
                "symbols": ["AAPL", "MSFT"],
                "pollIntervalMinutes": 5,
                "refreshCooldownMinutes": 15,
                "autoRefreshEnabled": True,
                "marketSession": "us_equities_regular",
            },
        )
        detail_response = await client.get("/api/intraday/watchlists/watch-1")
        update_response = await client.put(
            "/api/intraday/watchlists/watch-1",
            json={
                "name": "Tech Momentum",
                "description": "Core intraday list",
                "enabled": True,
                "symbols": ["AAPL", "MSFT"],
                "pollIntervalMinutes": 5,
                "refreshCooldownMinutes": 15,
                "autoRefreshEnabled": True,
                "marketSession": "us_equities_regular",
            },
        )
        append_symbols_response = await client.post(
            "/api/intraday/watchlists/watch-1/symbols",
            headers={"x-request-id": "req-append-1"},
            json={"symbols": ["nvda", " aapl "], "queueRun": True, "reason": " desk add "},
        )
        run_response = await client.post("/api/intraday/watchlists/watch-1/run")
        status_response = await client.get("/api/intraday/status")
        runs_response = await client.get("/api/intraday/runs")
        events_response = await client.get("/api/intraday/events")
        batches_response = await client.get("/api/intraday/refresh-batches")
        delete_response = await client.delete("/api/intraday/watchlists/watch-1")

    assert list_response.status_code == 200
    assert list_response.json()[0]["watchlistId"] == "watch-1"
    assert create_response.status_code == 200
    assert create_response.json()["symbols"] == ["AAPL", "MSFT"]
    assert detail_response.status_code == 200
    assert update_response.status_code == 200
    assert append_symbols_response.status_code == 200
    assert append_symbols_response.json()["addedSymbols"] == ["NVDA"]
    assert append_symbols_response.json()["queuedRun"]["forceRefresh"] is False
    assert run_response.status_code == 200
    assert run_response.json()["runId"] == "run-1"
    assert status_response.status_code == 200
    assert status_response.json()["counts"]["watchlistCount"] == 1
    assert status_response.json()["items"][0]["symbol"] == "AAPL"
    assert runs_response.status_code == 200
    assert events_response.status_code == 200
    assert batches_response.status_code == 200
    assert delete_response.status_code == 200
    assert deleted_watchlists == ["watch-1"]
    assert append_calls == [
        {
            "watchlist_id": "watch-1",
            "symbols": ["NVDA", "AAPL"],
            "queueRun": True,
            "reason": "desk add",
            "actor": None,
            "request_id": "req-append-1",
        }
    ]
    assert emitted == [
        (intraday_routes.REALTIME_TOPIC_INTRADAY_MONITOR, "watchlist.created"),
        (intraday_routes.REALTIME_TOPIC_INTRADAY_MONITOR, "watchlist.updated"),
        (intraday_routes.REALTIME_TOPIC_INTRADAY_MONITOR, "watchlist.symbols_added"),
        (intraday_routes.REALTIME_TOPIC_INTRADAY_MONITOR, "run.enqueued"),
        (intraday_routes.REALTIME_TOPIC_INTRADAY_MONITOR, "watchlist.deleted"),
    ]


async def test_intraday_routes_return_503_when_feature_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.delenv("INTRADAY_MONITOR_ENABLED", raising=False)
    monkeypatch.delenv("INTRADAY_MONITOR_ALLOWED_JOBS", raising=False)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/intraday/watchlists")

    assert response.status_code == 503
