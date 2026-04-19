from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from anyio import from_thread
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from api.service.dependencies import get_settings, require_intraday_operator_access
from api.service.intraday_contracts_compat import (
    IntradayMonitorEvent,
    IntradayMonitorRunSummary,
    IntradayRefreshBatchSummary,
    IntradaySymbolStatus,
    IntradayWatchlistDetail,
    IntradayWatchlistSummary,
    IntradayWatchlistUpsertRequest,
)
from api.service.realtime import manager as realtime_manager
from core.intraday_monitor_repository import (
    delete_intraday_watchlist,
    enqueue_intraday_watchlist_run,
    get_intraday_health_summary,
    get_intraday_watchlist,
    list_intraday_monitor_events,
    list_intraday_monitor_runs,
    list_intraday_refresh_batches,
    list_intraday_symbol_status,
    list_intraday_watchlists,
    upsert_intraday_watchlist,
)

REALTIME_TOPIC_INTRADAY_MONITOR = "intraday-monitor"
REALTIME_TOPIC_INTRADAY_REFRESH = "intraday-refresh"

router = APIRouter()


class IntradayStatusCounts(BaseModel):
    model_config = ConfigDict(extra="forbid")

    watchlistCount: int = Field(default=0, ge=0)
    enabledWatchlistCount: int = Field(default=0, ge=0)
    dueRunBacklogCount: int = Field(default=0, ge=0)
    failedRunCount: int = Field(default=0, ge=0)
    staleSymbolCount: int = Field(default=0, ge=0)
    refreshBatchBacklogAgeSeconds: float = Field(default=0.0, ge=0.0)


class IntradayStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    counts: IntradayStatusCounts
    latestMonitorRun: IntradayMonitorRunSummary | None = None
    latestRefreshBatch: IntradayRefreshBatchSummary | None = None
    total: int = Field(default=0, ge=0)
    items: list[IntradaySymbolStatus] = Field(default_factory=list)


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(get_settings(request).postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for intraday monitoring.")
    return dsn


def _emit_realtime(topic: str, event_type: str, payload: dict[str, Any] | None = None) -> None:
    message = {
        "type": event_type,
        "payload": payload or {},
        "emittedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        from_thread.run(realtime_manager.broadcast, topic, message)
    except RuntimeError:
        return
    except Exception:
        return


def _require_watchlist(dsn: str, watchlist_id: str) -> IntradayWatchlistDetail:
    watchlist = get_intraday_watchlist(dsn, watchlist_id)
    if watchlist is None:
        raise HTTPException(status_code=404, detail=f"Intraday watchlist '{watchlist_id}' not found.")
    return watchlist


@router.get("/watchlists", response_model=list[IntradayWatchlistSummary])
def list_intraday_watchlists_endpoint(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[IntradayWatchlistSummary]:
    require_intraday_operator_access(request)
    return list_intraday_watchlists(_require_postgres_dsn(request), limit=limit, offset=offset)


@router.post("/watchlists", response_model=IntradayWatchlistDetail)
def create_intraday_watchlist_endpoint(
    payload: IntradayWatchlistUpsertRequest,
    request: Request,
) -> IntradayWatchlistDetail:
    require_intraday_operator_access(request)
    try:
        watchlist = upsert_intraday_watchlist(_require_postgres_dsn(request), watchlist_id=None, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _emit_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "watchlist.created",
        {"watchlist": watchlist.model_dump(mode="json")},
    )
    return watchlist


@router.get("/watchlists/{watchlist_id}", response_model=IntradayWatchlistDetail)
def get_intraday_watchlist_endpoint(watchlist_id: str, request: Request) -> IntradayWatchlistDetail:
    require_intraday_operator_access(request)
    return _require_watchlist(_require_postgres_dsn(request), watchlist_id)


@router.put("/watchlists/{watchlist_id}", response_model=IntradayWatchlistDetail)
def update_intraday_watchlist_endpoint(
    watchlist_id: str,
    payload: IntradayWatchlistUpsertRequest,
    request: Request,
) -> IntradayWatchlistDetail:
    require_intraday_operator_access(request)
    try:
        watchlist = upsert_intraday_watchlist(_require_postgres_dsn(request), watchlist_id=watchlist_id, payload=payload)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _emit_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "watchlist.updated",
        {"watchlist": watchlist.model_dump(mode="json")},
    )
    return watchlist


@router.delete("/watchlists/{watchlist_id}")
def delete_intraday_watchlist_endpoint(watchlist_id: str, request: Request) -> dict[str, str]:
    require_intraday_operator_access(request)
    try:
        delete_intraday_watchlist(_require_postgres_dsn(request), watchlist_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _emit_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "watchlist.deleted",
        {"watchlistId": watchlist_id},
    )
    return {"status": "deleted"}


@router.post("/watchlists/{watchlist_id}/run", response_model=IntradayMonitorRunSummary)
def enqueue_intraday_watchlist_run_endpoint(watchlist_id: str, request: Request) -> IntradayMonitorRunSummary:
    require_intraday_operator_access(request)
    try:
        run = enqueue_intraday_watchlist_run(_require_postgres_dsn(request), watchlist_id=watchlist_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _emit_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "run.enqueued",
        {"run": run.model_dump(mode="json")},
    )
    return run


@router.get("/status", response_model=IntradayStatusResponse)
def get_intraday_status_endpoint(
    request: Request,
    watchlistId: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> IntradayStatusResponse:
    require_intraday_operator_access(request)
    dsn = _require_postgres_dsn(request)
    total, items = list_intraday_symbol_status(dsn, watchlist_id=watchlistId, q=q, limit=limit, offset=offset)
    summary = get_intraday_health_summary(dsn)
    return IntradayStatusResponse(
        counts=IntradayStatusCounts(
            watchlistCount=int(summary.get("watchlistCount") or 0),
            enabledWatchlistCount=int(summary.get("enabledWatchlistCount") or 0),
            dueRunBacklogCount=int(summary.get("dueRunBacklogCount") or 0),
            failedRunCount=int(summary.get("failedRunCount") or 0),
            staleSymbolCount=int(summary.get("staleSymbolCount") or 0),
            refreshBatchBacklogAgeSeconds=float(summary.get("refreshBatchBacklogAgeSeconds") or 0.0),
        ),
        latestMonitorRun=IntradayMonitorRunSummary.model_validate(summary["latestMonitorRun"])
        if summary.get("latestMonitorRun")
        else None,
        latestRefreshBatch=IntradayRefreshBatchSummary.model_validate(summary["latestRefreshBatch"])
        if summary.get("latestRefreshBatch")
        else None,
        total=total,
        items=items,
    )


@router.get("/runs", response_model=list[IntradayMonitorRunSummary])
def list_intraday_runs_endpoint(
    request: Request,
    watchlistId: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[IntradayMonitorRunSummary]:
    require_intraday_operator_access(request)
    return list_intraday_monitor_runs(_require_postgres_dsn(request), watchlist_id=watchlistId, limit=limit, offset=offset)


@router.get("/events", response_model=list[IntradayMonitorEvent])
def list_intraday_events_endpoint(
    request: Request,
    watchlistId: str | None = Query(default=None),
    runId: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[IntradayMonitorEvent]:
    require_intraday_operator_access(request)
    return list_intraday_monitor_events(
        _require_postgres_dsn(request),
        watchlist_id=watchlistId,
        run_id=runId,
        limit=limit,
        offset=offset,
    )


@router.get("/refresh-batches", response_model=list[IntradayRefreshBatchSummary])
def list_intraday_refresh_batches_endpoint(
    request: Request,
    watchlistId: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[IntradayRefreshBatchSummary]:
    require_intraday_operator_access(request)
    return list_intraday_refresh_batches(
        _require_postgres_dsn(request),
        watchlist_id=watchlistId,
        limit=limit,
        offset=offset,
    )
