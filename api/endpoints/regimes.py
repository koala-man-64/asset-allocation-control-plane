from __future__ import annotations

import os
import re
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from api.endpoints.backtests import _actor_from_request, _trigger_backtest_job
from api.service.dependencies import validate_auth
from asset_allocation_runtime_common.domain.regime import (
    DEFAULT_REGIME_MODEL_NAME,
    RegimeModelConfig,
    canonical_default_regime_config_errors,
)
from core.regime_repository import RegimeRepository

router = APIRouter()
_JOB_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?")


class CreateRegimeModelRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str = Field(default="", max_length=1000)
    config: dict[str, Any] = Field(default_factory=dict)


class ActivateRegimeModelRequest(BaseModel):
    version: int | None = Field(default=None, ge=1)


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for regime features.")
    return dsn


def _trigger_regime_job_if_configured() -> dict[str, Any] | None:
    job_name = str(os.environ.get("REGIME_ACA_JOB_NAME") or "").strip()
    if not job_name:
        return None
    if not _JOB_NAME_RE.fullmatch(job_name):
        raise HTTPException(status_code=500, detail="REGIME_ACA_JOB_NAME is invalid.")
    try:
        return _trigger_backtest_job(job_name)
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to trigger regime job: {exc}") from exc


def _validate_regime_config(*, model_name: str, config: dict[str, Any]) -> dict[str, Any]:
    validated = RegimeModelConfig.model_validate(config or {})
    if model_name != DEFAULT_REGIME_MODEL_NAME:
        return validated.model_dump(mode="json")

    errors = canonical_default_regime_config_errors(validated)
    if errors:
        raise HTTPException(
            status_code=422,
            detail="default-regime must use canonical v3 semantics: " + "; ".join(errors),
        )
    return validated.model_dump(mode="json")


def _require_activatable_regime_revision(*, model_name: str, revision: dict[str, Any] | None) -> None:
    if revision is None:
        raise HTTPException(status_code=404, detail=f"Regime model revision not found for '{model_name}'.")
    if model_name != DEFAULT_REGIME_MODEL_NAME:
        return

    validated = RegimeModelConfig.model_validate(revision.get("config") or {})
    errors = canonical_default_regime_config_errors(validated)
    if errors:
        raise HTTPException(
            status_code=409,
            detail="default-regime revision cannot be activated because it does not use canonical v3 semantics: "
            + "; ".join(errors),
        )


@router.get("/current")
async def get_current_regime(
    request: Request,
    modelName: str = Query(default=DEFAULT_REGIME_MODEL_NAME, min_length=1),
    modelVersion: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    validate_auth(request)
    repo = RegimeRepository(_require_postgres_dsn(request))
    payload = repo.get_regime_latest(model_name=modelName, model_version=modelVersion)
    if not payload:
        raise HTTPException(status_code=404, detail=f"Regime current snapshot not found for '{modelName}'.")
    return payload


@router.get("/history")
async def get_regime_history(
    request: Request,
    modelName: str = Query(default=DEFAULT_REGIME_MODEL_NAME, min_length=1),
    modelVersion: int | None = Query(default=None, ge=1),
    startDate: date | None = Query(default=None),
    endDate: date | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    validate_auth(request)
    repo = RegimeRepository(_require_postgres_dsn(request))
    rows = repo.list_regime_history(
        model_name=modelName,
        model_version=modelVersion,
        start_date=startDate,
        end_date=endDate,
        limit=limit,
    )
    return {
        "modelName": modelName,
        "modelVersion": modelVersion,
        "rows": rows,
        "limit": limit,
    }


@router.get("/models")
async def list_regime_models(request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = RegimeRepository(_require_postgres_dsn(request))
    return {"models": repo.list_regime_models()}


@router.get("/models/{model_name}")
async def get_regime_model_detail(model_name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = RegimeRepository(_require_postgres_dsn(request))
    model = repo.get_regime_model(model_name)
    if not model:
        raise HTTPException(status_code=404, detail=f"Regime model '{model_name}' not found.")
    return {
        "model": model,
        "activeRevision": repo.get_active_regime_model_revision(model_name),
        "revisions": repo.list_regime_model_revisions(model_name),
        "latest": repo.get_regime_latest(model_name=model_name),
    }


@router.post("/models")
async def create_regime_model(payload: CreateRegimeModelRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = RegimeRepository(_require_postgres_dsn(request))
    model_name = payload.name.strip()
    validated = _validate_regime_config(model_name=model_name, config=payload.config or {})
    created = repo.save_regime_model(
        name=model_name,
        description=payload.description.strip(),
        config=validated,
    )
    return {
        "model": created,
        "activeRevision": repo.get_active_regime_model_revision(created["name"]),
    }


@router.post("/models/{model_name}/activate")
async def activate_regime_model(
    model_name: str,
    payload: ActivateRegimeModelRequest,
    request: Request,
) -> dict[str, Any]:
    validate_auth(request)
    repo = RegimeRepository(_require_postgres_dsn(request))
    resolved_revision = repo.get_regime_model_revision(model_name, version=payload.version)
    _require_activatable_regime_revision(model_name=model_name, revision=resolved_revision)
    activated = repo.activate_regime_model(
        name=model_name,
        version=payload.version,
        activated_by=_actor_from_request(request),
    )
    trigger = _trigger_regime_job_if_configured()
    return {
        "model": model_name,
        "activatedRevision": activated,
        "jobTrigger": trigger,
    }
