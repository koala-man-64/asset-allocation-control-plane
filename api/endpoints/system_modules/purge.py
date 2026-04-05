from types import ModuleType
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


class PurgeRequest(BaseModel):
    scope: Literal["layer-domain", "layer", "domain"]
    layer: Optional[str] = None
    domain: Optional[str] = None
    confirm: bool = False


class DomainListResetRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    confirm: bool = False


class DomainCheckpointResetRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    confirm: bool = False


class DomainListFileResponse(BaseModel):
    listType: Literal["whitelist", "blacklist"]
    path: str
    exists: bool
    symbolCount: int
    symbols: List[str] = Field(default_factory=list)
    truncated: bool = False
    warning: Optional[str] = None


class DomainListsResponse(BaseModel):
    layer: str
    domain: str
    container: str
    limit: int
    files: List[DomainListFileResponse] = Field(default_factory=list)
    loadedAt: str


class DomainCheckpointTargetResponse(BaseModel):
    operation: str
    path: str
    status: Literal["reset"]
    existed: bool
    deleted: bool


class DomainCheckpointResetResponse(BaseModel):
    layer: str
    domain: str
    container: Optional[str] = None
    resetCount: int
    deletedCount: int
    targets: List[DomainCheckpointTargetResponse] = Field(default_factory=list)
    updatedAt: str
    note: Optional[str] = None


class PurgeCandidatesRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    column: str = Field(..., min_length=1, max_length=128)
    operator: str = Field(..., min_length=1, max_length=24)
    value: Optional[float] = None
    percentile: Optional[float] = None
    as_of: Optional[str] = None
    recent_rows: int = Field(default=1, ge=1, le=5000)
    aggregation: str = Field(default="avg", min_length=1, max_length=24)
    limit: Optional[int] = Field(default=None, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)
    min_rows: int = Field(default=1, ge=1)


class PurgeSymbolRequest(BaseModel):
    symbol: str
    confirm: bool = False


class PurgeRuleAuditRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    column_name: str = Field(..., min_length=1, max_length=128)
    operator: str = Field(..., min_length=1, max_length=24)
    threshold: float
    aggregation: Optional[str] = Field(default=None, min_length=1, max_length=24)
    recent_rows: Optional[int] = Field(default=None, ge=1, le=5000)
    expression: Optional[str] = Field(default=None, max_length=512)
    selected_symbol_count: Optional[int] = Field(default=None, ge=0)
    matched_symbol_count: Optional[int] = Field(default=None, ge=0)


class PurgeSymbolsBatchRequest(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    confirm: bool = False
    scope_note: Optional[str] = None
    dry_run: bool = False
    audit_rule: Optional[PurgeRuleAuditRequest] = None


class PurgeRuleCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    column_name: str = Field(..., min_length=1, max_length=128)
    operator: str = Field(..., min_length=1, max_length=24)
    threshold: float
    run_interval_minutes: int = Field(..., ge=1)


class PurgeRuleUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    layer: Optional[str] = Field(default=None, min_length=1, max_length=32)
    domain: Optional[str] = Field(default=None, min_length=1, max_length=64)
    column_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    operator: Optional[str] = Field(default=None, min_length=1, max_length=24)
    threshold: Optional[float] = None
    run_interval_minutes: Optional[int] = Field(default=None, ge=1)


class PurgeRulePreviewRequest(BaseModel):
    max_symbols: int = Field(default=200, ge=1, le=1000)


def build_router(
    *,
    runtime: ModuleType,
    domain_lists_response_model: Any,
    purge_request_model: Any,
    domain_list_reset_request_model: Any,
    domain_checkpoint_reset_request_model: Any,
    purge_candidates_request_model: Any,
    purge_symbol_request_model: Any,
    purge_symbols_batch_request_model: Any,
    purge_rule_create_request_model: Any,
    purge_rule_update_request_model: Any,
    purge_rule_preview_request_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/purge-rules/operators")
    def list_purge_rule_operators(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        supported_purge_rule_operators = _runtime_attr(runtime, "supported_purge_rule_operators")

        validate_auth(request)
        return JSONResponse({"operators": supported_purge_rule_operators()}, headers={"Cache-Control": "no-store"})

    @router.get("/purge-rules")
    def list_purge_rules_endpoint(
        request: Request,
        layer: Optional[str] = Query(default=None),
        domain: Optional[str] = Query(default=None),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        list_purge_rules = _runtime_attr(runtime, "list_purge_rules")
        logger = _runtime_attr(runtime, "logger")
        serialize_purge_rule = _runtime_attr(runtime, "_serialize_purge_rule")

        validate_auth(request)
        dsn = require_postgres_dsn(request)
        try:
            normalized_layer = normalize_layer(layer)
            normalized_domain = normalize_domain(domain)
            rules = list_purge_rules(dsn=dsn, layer=normalized_layer, domain=normalized_domain)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid purge-rule query: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to list purge rules.")
            raise HTTPException(status_code=503, detail=f"Failed to list purge rules: {exc}") from exc

        return JSONResponse(
            {"items": [serialize_purge_rule(rule) for rule in rules]},
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/purge-rules")
    def create_purge_rule_endpoint(payload: purge_rule_create_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        get_actor = _runtime_attr(runtime, "_get_actor")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        normalize_purge_rule_operator = _runtime_attr(runtime, "normalize_purge_rule_operator")
        create_purge_rule = _runtime_attr(runtime, "create_purge_rule")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        serialize_purge_rule = _runtime_attr(runtime, "_serialize_purge_rule")

        validate_auth(request)
        dsn = require_postgres_dsn(request)
        actor = get_actor(request)
        normalized_layer = normalize_layer(payload.layer)
        normalized_domain = normalize_domain(payload.domain)
        if not normalized_layer or not normalized_domain:
            raise HTTPException(status_code=400, detail="layer and domain are required.")
        try:
            operator = normalize_purge_rule_operator(payload.operator)
            rule = create_purge_rule(
                dsn=dsn,
                name=payload.name,
                layer=normalized_layer,
                domain=normalized_domain,
                column_name=payload.column_name,
                operator=operator,
                threshold=payload.threshold,
                run_interval_minutes=payload.run_interval_minutes,
                actor=actor,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid purge rule: {exc}") from exc
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to create purge rule: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to create purge rule.")
            raise HTTPException(status_code=500, detail=f"Failed to create purge rule: {exc}") from exc

        return JSONResponse(serialize_purge_rule(rule), headers={"Cache-Control": "no-store"}, status_code=201)

    @router.patch("/purge-rules/{rule_id}")
    def update_purge_rule_endpoint(
        rule_id: int,
        payload: purge_rule_update_request_model,
        request: Request,
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        get_actor = _runtime_attr(runtime, "_get_actor")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        normalize_purge_rule_operator = _runtime_attr(runtime, "normalize_purge_rule_operator")
        update_purge_rule = _runtime_attr(runtime, "update_purge_rule")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        serialize_purge_rule = _runtime_attr(runtime, "_serialize_purge_rule")

        validate_auth(request)
        dsn = require_postgres_dsn(request)
        actor = get_actor(request)
        if all(
            value is None
            for value in (
                payload.name,
                payload.layer,
                payload.domain,
                payload.column_name,
                payload.operator,
                payload.threshold,
                payload.run_interval_minutes,
            )
        ):
            raise HTTPException(status_code=400, detail="No fields supplied for purge rule update.")

        try:
            rule = update_purge_rule(
                dsn=dsn,
                rule_id=rule_id,
                name=payload.name,
                layer=(normalize_layer(payload.layer) if payload.layer is not None else None),
                domain=(normalize_domain(payload.domain) if payload.domain is not None else None),
                column_name=payload.column_name,
                operator=normalize_purge_rule_operator(payload.operator) if payload.operator is not None else None,
                threshold=payload.threshold,
                run_interval_minutes=payload.run_interval_minutes,
                actor=actor,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid purge-rule update: {exc}") from exc
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to update purge rule: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to update purge rule id=%s.", rule_id)
            raise HTTPException(status_code=500, detail=f"Failed to update purge rule: {exc}") from exc

        return JSONResponse(serialize_purge_rule(rule), headers={"Cache-Control": "no-store"})

    @router.delete("/purge-rules/{rule_id}", status_code=200)
    def delete_purge_rule_endpoint(rule_id: int, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        delete_purge_rule_row = _runtime_attr(runtime, "delete_purge_rule_row")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        dsn = require_postgres_dsn(request)
        deleted = False
        try:
            deleted = delete_purge_rule_row(dsn=dsn, rule_id=rule_id)
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to delete purge rule: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to delete purge rule id=%s.", rule_id)
            raise HTTPException(status_code=500, detail=f"Failed to delete purge rule: {exc}") from exc
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Purge rule id={rule_id} not found.")
        return JSONResponse({"deleted": True, "id": rule_id}, headers={"Cache-Control": "no-store"})

    @router.post("/purge-rules/{rule_id}/preview")
    def preview_purge_rule(
        rule_id: int,
        request: Request,
        payload: purge_rule_preview_request_model,
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        get_purge_rule = _runtime_attr(runtime, "get_purge_rule")
        collect_rule_symbol_values = _runtime_attr(runtime, "_collect_rule_symbol_values")
        logger = _runtime_attr(runtime, "logger")
        serialize_purge_rule = _runtime_attr(runtime, "_serialize_purge_rule")

        validate_auth(request)
        dsn = require_postgres_dsn(request)
        rule = get_purge_rule(dsn=dsn, rule_id=rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail=f"Purge rule id={rule_id} not found.")

        try:
            matches = collect_rule_symbol_values(rule)
            matches = sorted(matches, key=lambda pair: str(pair[0]).strip().upper())
            preview = [
                {
                    "symbol": symbol,
                    "value": metric,
                }
                for symbol, metric in matches[: payload.max_symbols]
            ]
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Failed to preview purge rule id=%s.", rule_id)
            raise HTTPException(status_code=500, detail=f"Failed to preview purge rule: {exc}") from exc

        return JSONResponse(
            {
                "rule": serialize_purge_rule(rule),
                "matchCount": len(matches),
                "previewCount": len(preview),
                "matches": preview,
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/purge-rules/{rule_id}/run")
    def run_purge_rule_now(rule_id: int, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        get_actor = _runtime_attr(runtime, "_get_actor")
        get_purge_rule = _runtime_attr(runtime, "get_purge_rule")
        claim_purge_rule_for_run = _runtime_attr(runtime, "claim_purge_rule_for_run")
        execute_purge_rule = _runtime_attr(runtime, "_execute_purge_rule")
        complete_purge_rule_execution = _runtime_attr(runtime, "complete_purge_rule_execution")
        logger = _runtime_attr(runtime, "logger")
        serialize_purge_rule = _runtime_attr(runtime, "_serialize_purge_rule")
        datetime_cls = _runtime_attr(runtime, "datetime")
        timezone_obj = _runtime_attr(runtime, "timezone")

        validate_auth(request)
        dsn = require_postgres_dsn(request)
        actor = get_actor(request)
        now = datetime_cls.now(timezone_obj.utc)
        rule = get_purge_rule(dsn=dsn, rule_id=rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail=f"Purge rule id={rule_id} not found.")

        if not claim_purge_rule_for_run(
            dsn=dsn,
            rule_id=rule.id,
            now=now,
            require_due=False,
            actor=actor,
        ):
            raise HTTPException(status_code=409, detail="Purge rule is already running.")

        try:
            execution = execute_purge_rule(rule=rule, actor=actor)
            failed_symbols = execution.get("failedSymbols") or []
            status = "failed" if failed_symbols else "succeeded"
            complete_purge_rule_execution(
                dsn=dsn,
                rule_id=rule.id,
                status=status,
                error=None if not failed_symbols else "; ".join(failed_symbols),
                matched_count=int(execution.get("matchedCount") or 0),
                purged_count=int(execution.get("purgedCount") or 0),
                run_interval_minutes=rule.run_interval_minutes,
                actor=actor,
                now=now,
            )
        except Exception as exc:
            logger.exception("Failed to run purge rule id=%s now.", rule_id)
            try:
                complete_purge_rule_execution(
                    dsn=dsn,
                    rule_id=rule.id,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    matched_count=None,
                    purged_count=None,
                    run_interval_minutes=rule.run_interval_minutes,
                    actor=actor,
                    now=now,
                )
            except Exception:
                logger.exception("Failed to persist purge-rule manual failure: id=%s", rule_id)
            raise HTTPException(status_code=500, detail=f"Failed to run purge rule: {exc}") from exc

        return JSONResponse(
            {
                "rule": serialize_purge_rule(get_purge_rule(dsn=dsn, rule_id=rule_id) or rule),
                "execution": execution,
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/purge")
    def purge_data(payload: purge_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_actor = _runtime_attr(runtime, "_get_actor")
        logger = _runtime_attr(runtime, "logger")
        create_purge_operation = _runtime_attr(runtime, "_create_purge_operation")
        execute_purge_operation = _runtime_attr(runtime, "_execute_purge_operation")
        threading = _runtime_attr(runtime, "threading")
        utc_timestamp = _runtime_attr(runtime, "_utc_timestamp")

        validate_auth(request)
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="Confirmation required to purge data.")

        actor = get_actor(request)
        logger.info(
            "Purge request received: actor=%s scope=%s layer=%s domain=%s",
            actor or "-",
            payload.scope,
            payload.layer,
            payload.domain,
        )
        operation_id = create_purge_operation(payload, actor)
        thread = threading.Thread(target=execute_purge_operation, args=(operation_id, payload), daemon=True)
        thread.start()
        logger.info(
            "Purge operation queued: operation=%s actor=%s scope=%s layer=%s domain=%s",
            operation_id,
            actor or "-",
            payload.scope,
            payload.layer,
            payload.domain,
        )

        return JSONResponse(
            {
                "operationId": operation_id,
                "status": "running",
                "scope": payload.scope,
                "layer": payload.layer,
                "domain": payload.domain,
                "createdAt": utc_timestamp(),
                "updatedAt": utc_timestamp(),
                "startedAt": utc_timestamp(),
                "completedAt": None,
                "result": None,
                "error": None,
            },
            status_code=202,
        )

    @router.get("/domain-lists", response_model=domain_lists_response_model)
    def get_domain_lists(
        request: Request,
        layer: str = Query(..., description="Layer key (bronze|silver|gold|platinum)"),
        domain: str = Query(..., description="Domain key (market|finance|earnings|price-target|platinum)"),
        limit: int = Query(default=5000, ge=1, le=50000, description="Max symbols returned per list file."),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        resolve_container = _runtime_attr(runtime, "_resolve_container")
        blob_storage_client = _runtime_attr(runtime, "BlobStorageClient")
        resolve_domain_list_paths = _runtime_attr(runtime, "_resolve_domain_list_paths")
        load_domain_list_file_preview = _runtime_attr(runtime, "_load_domain_list_file_preview")
        utc_timestamp = _runtime_attr(runtime, "_utc_timestamp")

        validate_auth(request)
        layer_norm = normalize_layer(layer)
        domain_norm = normalize_domain(domain)
        if not layer_norm:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not domain_norm:
            raise HTTPException(status_code=400, detail="domain is required.")

        container = resolve_container(layer_norm)
        client = blob_storage_client(container_name=container, ensure_container_exists=False)
        list_paths = resolve_domain_list_paths(layer_norm, domain_norm)

        files: List[Dict[str, Any]] = []
        for item in list_paths:
            list_type = str(item.get("listType") or "").strip().lower()
            path = str(item.get("path") or "").strip()
            if list_type not in {"whitelist", "blacklist"} or not path:
                continue
            files.append(
                load_domain_list_file_preview(
                    client,
                    list_type=list_type,
                    path=path,
                    limit=limit,
                )
            )

        payload_out = {
            "layer": layer_norm,
            "domain": domain_norm,
            "container": container,
            "limit": limit,
            "files": files,
            "loadedAt": utc_timestamp(),
        }
        return JSONResponse(payload_out, headers={"Cache-Control": "no-store"})

    @router.post("/domain-lists/reset")
    def reset_domain_lists(payload: domain_list_reset_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        resolve_container = _runtime_attr(runtime, "_resolve_container")
        blob_storage_client = _runtime_attr(runtime, "BlobStorageClient")
        reset_domain_lists_impl = _runtime_attr(runtime, "_reset_domain_lists")
        get_actor = _runtime_attr(runtime, "_get_actor")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="Confirmation required to reset blacklist/whitelist lists.")

        layer_norm = normalize_layer(payload.layer)
        domain_norm = normalize_domain(payload.domain)
        if not layer_norm:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not domain_norm:
            raise HTTPException(status_code=400, detail="domain is required.")

        container = resolve_container(layer_norm)
        client = blob_storage_client(container_name=container, ensure_container_exists=False)
        result = reset_domain_lists_impl(client, layer=layer_norm, domain=domain_norm)
        actor = get_actor(request)
        logger.warning(
            "Domain lists reset: actor=%s layer=%s domain=%s container=%s reset=%s",
            actor or "-",
            layer_norm,
            domain_norm,
            container,
            result.get("resetCount"),
        )
        return JSONResponse(result, headers={"Cache-Control": "no-store"})

    @router.post("/domain-checkpoints/reset")
    def reset_domain_checkpoints(payload: domain_checkpoint_reset_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        reset_domain_checkpoints_impl = _runtime_attr(runtime, "_reset_domain_checkpoints")
        get_actor = _runtime_attr(runtime, "_get_actor")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="Confirmation required to reset checkpoint gates.")

        result = reset_domain_checkpoints_impl(layer=payload.layer, domain=payload.domain)
        actor = get_actor(request)
        logger.warning(
            "Domain checkpoints reset: actor=%s layer=%s domain=%s container=%s reset=%s deleted=%s",
            actor or "-",
            result.get("layer"),
            result.get("domain"),
            result.get("container") or "-",
            result.get("resetCount"),
            result.get("deletedCount"),
        )
        return JSONResponse(result, headers={"Cache-Control": "no-store"})

    @router.get("/purge-candidates")
    def get_purge_candidates(
        request: Request,
        layer: str = Query(..., description="Layer key (bronze/silver/gold)"),
        domain: str = Query(..., description="Domain key (market/finance/earnings/price-target)"),
        column: str = Query(..., description="Column to evaluate"),
        operator: str = Query(..., description="Supported operators: gt, gte, lt, lte, top_percent, bottom_percent"),
        value: Optional[float] = Query(default=None, description="Numeric threshold (required for numeric operators)"),
        percentile: Optional[float] = Query(default=None, description="Required for percent operators"),
        as_of: Optional[str] = Query(default=None, description="Optional date limit (YYYY-MM-DD)"),
        recent_rows: int = Query(default=1, ge=1, le=5000, description="Recent rows per symbol used for aggregation"),
        aggregation: str = Query(default="avg", description="Aggregation over recent rows: min|max|avg|stddev"),
        limit: Optional[int] = Query(default=None, ge=1, le=5000, description="Deprecated: optional max candidate rows"),
        offset: int = Query(default=0, ge=0, description="Candidate result offset"),
        min_rows: int = Query(default=1, ge=1, description="Minimum rows contributing per symbol"),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        build_purge_candidates_response = _runtime_attr(runtime, "_build_purge_candidates_response")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        try:
            response_payload = build_purge_candidates_response(
                layer=layer,
                domain=domain,
                column=column,
                operator=operator,
                value=value,
                percentile=percentile,
                as_of=as_of,
                recent_rows=recent_rows,
                aggregation=aggregation,
                limit=limit,
                offset=offset,
                min_rows=min_rows,
            )
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception(
                "Failed to collect purge candidates: layer=%s domain=%s column=%s",
                layer,
                domain,
                column,
            )
            raise HTTPException(status_code=500, detail=f"Failed to collect purge candidates: {exc}") from exc

        return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})

    @router.post("/purge-candidates")
    def create_purge_candidates_operation(payload: purge_candidates_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_actor = _runtime_attr(runtime, "_get_actor")
        create_purge_candidates_operation_impl = _runtime_attr(runtime, "_create_purge_candidates_operation")
        execute_purge_candidates_operation = _runtime_attr(runtime, "_execute_purge_candidates_operation")
        get_purge_operation_state = _runtime_attr(runtime, "_get_purge_operation")
        threading = _runtime_attr(runtime, "threading")

        validate_auth(request)
        actor = get_actor(request)
        operation_id = create_purge_candidates_operation_impl(payload, actor)
        thread = threading.Thread(
            target=execute_purge_candidates_operation,
            args=(operation_id, payload),
            daemon=True,
        )
        thread.start()

        operation = get_purge_operation_state(operation_id)
        if not operation:
            raise HTTPException(status_code=500, detail="Failed to initialize purge-candidates operation.")

        return JSONResponse(operation, status_code=202)

    @router.get("/purge-symbols/blacklist")
    def get_blacklist_symbols_for_purge(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        resolve_container = _runtime_attr(runtime, "_resolve_container")
        blob_storage_client = _runtime_attr(runtime, "BlobStorageClient")
        load_symbols_from_bronze_blacklists = _runtime_attr(runtime, "_load_symbols_from_bronze_blacklists")
        utc_timestamp = _runtime_attr(runtime, "_utc_timestamp")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)

        container_bronze = resolve_container("bronze")
        bronze_client = blob_storage_client(container_name=container_bronze, ensure_container_exists=False)
        payload_out = load_symbols_from_bronze_blacklists(bronze_client)
        payload_out["loadedAt"] = utc_timestamp()

        logger.info(
            "Loaded blacklist symbols for purge: container=%s symbols=%s sources=%s",
            container_bronze,
            payload_out.get("symbolCount"),
            len(payload_out.get("sources") or []),
        )
        return JSONResponse(payload_out, headers={"Cache-Control": "no-store"})

    @router.post("/purge-symbols")
    def purge_symbols(payload: purge_symbols_batch_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_actor = _runtime_attr(runtime, "_get_actor")
        normalize_candidate_symbols = _runtime_attr(runtime, "_normalize_candidate_symbols")
        require_postgres_dsn = _runtime_attr(runtime, "_require_postgres_dsn")
        persist_purge_symbols_audit_rule = _runtime_attr(runtime, "_persist_purge_symbols_audit_rule")
        create_purge_symbols_operation = _runtime_attr(runtime, "_create_purge_symbols_operation")
        execute_purge_symbols_operation = _runtime_attr(runtime, "_execute_purge_symbols_operation")
        get_purge_operation_state = _runtime_attr(runtime, "_get_purge_operation")
        threading = _runtime_attr(runtime, "threading")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="Confirmation required to purge symbols.")

        actor = get_actor(request)
        normalized_symbols = normalize_candidate_symbols(payload.symbols)
        if not normalized_symbols:
            raise HTTPException(status_code=400, detail="At least one symbol is required.")

        audit_rule = None
        if payload.audit_rule and not payload.dry_run:
            dsn = require_postgres_dsn(request)
            audit_rule = persist_purge_symbols_audit_rule(
                dsn=dsn,
                audit_rule=payload.audit_rule,
                actor=actor,
            )

        operation_id = create_purge_symbols_operation(
            normalized_symbols,
            actor,
            scope_note=payload.scope_note,
            dry_run=bool(payload.dry_run),
            audit_rule_id=(audit_rule.id if audit_rule else None),
        )
        logger.info(
            "Purge-symbols requested: operation=%s actor=%s symbols=%s dry_run=%s audit_rule_id=%s",
            operation_id,
            actor or "-",
            len(normalized_symbols),
            bool(payload.dry_run),
            (audit_rule.id if audit_rule else None),
        )
        thread = threading.Thread(
            target=execute_purge_symbols_operation,
            args=(operation_id, normalized_symbols),
            kwargs={"dry_run": bool(payload.dry_run), "scope_note": payload.scope_note},
            daemon=True,
        )
        thread.start()

        operation = get_purge_operation_state(operation_id) or {}
        if not isinstance(operation, dict):
            raise HTTPException(status_code=500, detail="Failed to initialize purge-symbols operation.")

        return JSONResponse(operation, status_code=202)

    @router.post("/purge-symbol")
    def purge_symbol(payload: purge_symbol_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_actor = _runtime_attr(runtime, "_get_actor")
        normalize_purge_symbol = _runtime_attr(runtime, "_normalize_purge_symbol")
        create_purge_symbol_operation = _runtime_attr(runtime, "_create_purge_symbol_operation")
        execute_purge_symbol_operation = _runtime_attr(runtime, "_execute_purge_symbol_operation")
        threading = _runtime_attr(runtime, "threading")
        utc_timestamp = _runtime_attr(runtime, "_utc_timestamp")

        validate_auth(request)
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="Confirmation required to purge a symbol.")

        actor = get_actor(request)
        normalized_symbol = normalize_purge_symbol(payload.symbol)
        symbol_payload = purge_symbol_request_model(symbol=normalized_symbol, confirm=payload.confirm)
        operation_id = create_purge_symbol_operation(symbol_payload, actor)
        thread = threading.Thread(target=execute_purge_symbol_operation, args=(operation_id, symbol_payload), daemon=True)
        thread.start()

        return JSONResponse(
            {
                "operationId": operation_id,
                "status": "running",
                "scope": "symbol",
                "symbol": normalized_symbol,
                "createdAt": utc_timestamp(),
                "updatedAt": utc_timestamp(),
                "startedAt": utc_timestamp(),
                "completedAt": None,
                "result": None,
                "error": None,
            },
            status_code=202,
        )

    @router.get("/purge/{operation_id}")
    def get_purge_operation(operation_id: str, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_purge_operation_state = _runtime_attr(runtime, "_get_purge_operation")

        validate_auth(request)
        operation = get_purge_operation_state(operation_id)
        if not operation:
            raise HTTPException(status_code=404, detail="Purge operation not found.")
        return JSONResponse(operation)

    return router, {
        "list_purge_rule_operators": list_purge_rule_operators,
        "list_purge_rules_endpoint": list_purge_rules_endpoint,
        "create_purge_rule_endpoint": create_purge_rule_endpoint,
        "update_purge_rule_endpoint": update_purge_rule_endpoint,
        "delete_purge_rule_endpoint": delete_purge_rule_endpoint,
        "preview_purge_rule": preview_purge_rule,
        "run_purge_rule_now": run_purge_rule_now,
        "purge_data": purge_data,
        "get_domain_lists": get_domain_lists,
        "reset_domain_lists": reset_domain_lists,
        "reset_domain_checkpoints": reset_domain_checkpoints,
        "get_purge_candidates": get_purge_candidates,
        "create_purge_candidates_operation": create_purge_candidates_operation,
        "get_blacklist_symbols_for_purge": get_blacklist_symbols_for_purge,
        "purge_symbols": purge_symbols,
        "purge_symbol": purge_symbol,
        "get_purge_operation": get_purge_operation,
    }
