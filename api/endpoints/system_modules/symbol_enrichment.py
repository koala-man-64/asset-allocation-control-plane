from __future__ import annotations

from types import ModuleType
from typing import Any

from asset_allocation_contracts.symbol_enrichment import (
    SymbolCleanupRunSummary,
    SymbolEnrichmentEnqueueRequest,
    SymbolEnrichmentSummaryResponse,
    SymbolEnrichmentSymbolDetailResponse,
    SymbolEnrichmentSymbolListItem,
    SymbolProfileOverride,
)
from fastapi import APIRouter, HTTPException, Query, Request, Response


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def _require_postgres_dsn(runtime: ModuleType, request: Request) -> str:
    get_settings = _runtime_attr(runtime, "get_settings")
    os_module = _runtime_attr(runtime, "os")

    settings = get_settings(request)
    dsn = str(settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")
    return dsn


def build_router(*, runtime: ModuleType) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/symbol-enrichment/summary", response_model=SymbolEnrichmentSummaryResponse)
    def get_symbol_enrichment_summary_endpoint(request: Request) -> SymbolEnrichmentSummaryResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_symbol_enrichment_summary = _runtime_attr(runtime, "get_symbol_enrichment_summary")

        validate_auth(request)
        return get_symbol_enrichment_summary(_require_postgres_dsn(runtime, request))

    @router.get("/symbol-enrichment/runs", response_model=list[SymbolCleanupRunSummary])
    def list_symbol_enrichment_runs_endpoint(
        request: Request,
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> list[SymbolCleanupRunSummary]:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        list_symbol_cleanup_runs = _runtime_attr(runtime, "list_symbol_cleanup_runs")

        validate_auth(request)
        return list_symbol_cleanup_runs(_require_postgres_dsn(runtime, request), limit=limit, offset=offset)

    @router.get("/symbol-enrichment/symbols", response_model=list[SymbolEnrichmentSymbolListItem])
    def list_symbol_enrichment_symbols_endpoint(
        request: Request,
        response: Response,
        q: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> list[SymbolEnrichmentSymbolListItem]:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        list_symbol_enrichment_symbols = _runtime_attr(runtime, "list_symbol_enrichment_symbols")

        validate_auth(request)
        total, items = list_symbol_enrichment_symbols(
            _require_postgres_dsn(runtime, request),
            q=q,
            limit=limit,
            offset=offset,
        )
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Total-Count"] = str(total)
        return items

    @router.get("/symbol-enrichment/symbols/{symbol}", response_model=SymbolEnrichmentSymbolDetailResponse)
    def get_symbol_enrichment_symbol_detail_endpoint(
        symbol: str,
        request: Request,
    ) -> SymbolEnrichmentSymbolDetailResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_symbol_enrichment_symbol_detail = _runtime_attr(runtime, "get_symbol_enrichment_symbol_detail")

        validate_auth(request)
        try:
            return get_symbol_enrichment_symbol_detail(_require_postgres_dsn(runtime, request), symbol)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/symbol-enrichment/enqueue", response_model=SymbolCleanupRunSummary)
    def enqueue_symbol_enrichment_endpoint(
        payload: SymbolEnrichmentEnqueueRequest,
        request: Request,
    ) -> SymbolCleanupRunSummary:
        require_system_operate_access = _runtime_attr(runtime, "require_system_operate_access")
        get_settings = _runtime_attr(runtime, "get_settings")
        enqueue_symbol_cleanup_run = _runtime_attr(runtime, "enqueue_symbol_cleanup_run")

        require_system_operate_access(request)
        settings = get_settings(request).symbol_enrichment
        if not settings.enabled:
            raise HTTPException(status_code=503, detail="Symbol enrichment is disabled.")

        requested_max_symbols = payload.maxSymbols or settings.max_symbols_per_run
        max_symbols = min(requested_max_symbols, settings.max_symbols_per_run)
        try:
            return enqueue_symbol_cleanup_run(
                _require_postgres_dsn(runtime, request),
                symbols=payload.symbols,
                full_scan=payload.fullScan,
                overwrite_mode=payload.overwriteMode,
                max_symbols=max_symbols,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.put("/symbol-enrichment/overrides/{symbol}", response_model=list[SymbolProfileOverride])
    def update_symbol_profile_overrides_endpoint(
        symbol: str,
        payload: list[SymbolProfileOverride],
        request: Request,
    ) -> list[SymbolProfileOverride]:
        require_system_operate_access = _runtime_attr(runtime, "require_system_operate_access")
        get_actor = _runtime_attr(runtime, "_get_actor")
        upsert_symbol_profile_overrides = _runtime_attr(runtime, "upsert_symbol_profile_overrides")

        require_system_operate_access(request)
        actor = get_actor(request)
        resolved_payload = [
            override.model_copy(
                update={
                    "symbol": symbol,
                    "updatedBy": override.updatedBy or actor,
                }
            )
            for override in payload
        ]
        try:
            return upsert_symbol_profile_overrides(
                _require_postgres_dsn(runtime, request),
                symbol=symbol,
                overrides=resolved_payload,
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return router, {
        "get_symbol_enrichment_summary_endpoint": get_symbol_enrichment_summary_endpoint,
        "list_symbol_enrichment_runs_endpoint": list_symbol_enrichment_runs_endpoint,
        "list_symbol_enrichment_symbols_endpoint": list_symbol_enrichment_symbols_endpoint,
        "get_symbol_enrichment_symbol_detail_endpoint": get_symbol_enrichment_symbol_detail_endpoint,
        "enqueue_symbol_enrichment_endpoint": enqueue_symbol_enrichment_endpoint,
        "update_symbol_profile_overrides_endpoint": update_symbol_profile_overrides_endpoint,
    }
