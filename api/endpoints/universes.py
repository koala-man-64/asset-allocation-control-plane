from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from api.openapi_models import UniverseDefinitionOutput, UniversePreviewResponse
from api.service.dependencies import validate_auth
from core.strategy_engine.universe import (
    list_gold_universe_catalog,
    preview_gold_universe,
    _publicize_universe_definition,
    validate_universe_definition_support,
)
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

router = APIRouter()


class UniverseConfigSummaryResponse(BaseModel):
    name: str
    description: str = ""
    version: int = 1
    updated_at: datetime | None = None


class UniverseConfigDetailResponse(UniverseConfigSummaryResponse):
    config: UniverseDefinitionOutput


class UniverseConfigUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str = ""
    config: dict[str, Any]


class UniverseCatalogFieldResponse(BaseModel):
    id: str
    label: str
    valueKind: str
    operators: list[str]


class UniverseCatalogResponse(BaseModel):
    source: str
    fields: list[UniverseCatalogFieldResponse]


class UniversePreviewRequest(BaseModel):
    universeName: str | None = Field(default=None, min_length=1, max_length=128)
    universe: dict[str, Any] | None = None
    sampleLimit: int = Field(default=25, ge=1, le=100)

    @model_validator(mode="after")
    def validate_source(self) -> "UniversePreviewRequest":
        if not self.universeName and self.universe is None:
            raise ValueError("universeName or universe is required.")
        return self


def _require_postgres_dsn(request: Request) -> str:
    settings = request.app.state.settings
    dsn = str(settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for universe features.")
    return dsn


def _build_detail_response(universe: dict[str, Any]) -> UniverseConfigDetailResponse:
    return UniverseConfigDetailResponse(
        name=str(universe.get("name") or ""),
        description=str(universe.get("description") or ""),
        version=int(universe.get("version") or 1),
        updated_at=universe.get("updated_at"),
        config=UniverseDefinitionOutput.model_validate(_publicize_universe_definition(universe.get("config") or {})),
    )


@router.get("/", response_model=List[UniverseConfigSummaryResponse])
async def list_universe_configs(request: Request) -> List[dict[str, Any]]:
    validate_auth(request)
    repo = UniverseRepository(_require_postgres_dsn(request))
    return repo.list_universe_configs()


@router.get("/catalog", response_model=UniverseCatalogResponse)
async def get_universe_catalog(request: Request) -> UniverseCatalogResponse:
    validate_auth(request)
    try:
        return UniverseCatalogResponse.model_validate(list_gold_universe_catalog(_require_postgres_dsn(request)))
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Universe catalog load failed.")
        raise HTTPException(status_code=500, detail=f"Failed to load universe catalog: {exc}") from exc


@router.post("/preview", response_model=UniversePreviewResponse)
async def preview_universe(payload: UniversePreviewRequest, request: Request) -> UniversePreviewResponse:
    validate_auth(request)
    repo = UniverseRepository(_require_postgres_dsn(request))
    try:
        universe = payload.universe
        if universe is None:
            record = repo.get_universe_config(str(payload.universeName))
            if not record:
                raise HTTPException(status_code=404, detail=f"Universe config '{payload.universeName}' not found")
            universe = record.get("config") or {}
        return UniversePreviewResponse.model_validate(
            preview_gold_universe(_require_postgres_dsn(request), universe, sample_limit=payload.sampleLimit)
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Universe preview failed.")
        raise HTTPException(status_code=500, detail=f"Failed to preview universe: {exc}") from exc


@router.get("/{name}/detail", response_model=UniverseConfigDetailResponse)
async def get_universe_config_detail(name: str, request: Request) -> UniverseConfigDetailResponse:
    validate_auth(request)
    repo = UniverseRepository(_require_postgres_dsn(request))
    universe = repo.get_universe_config(name)
    if not universe:
        raise HTTPException(status_code=404, detail=f"Universe config '{name}' not found")
    return _build_detail_response(universe)


@router.get("/{name}/revisions/{version}", response_model=UniverseConfigDetailResponse)
async def get_universe_config_revision(name: str, version: int, request: Request) -> UniverseConfigDetailResponse:
    validate_auth(request)
    repo = UniverseRepository(_require_postgres_dsn(request))
    universe = repo.get_universe_config_revision(name, version=version)
    if not universe:
        raise HTTPException(status_code=404, detail=f"Universe config '{name}' version {version} not found")
    return _build_detail_response(universe)


@router.post("/")
async def save_universe_config(payload: UniverseConfigUpsertRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = UniverseRepository(dsn)
    try:
        normalized_universe = validate_universe_definition_support(dsn, payload.config)
        saved = repo.save_universe_config(
            name=payload.name,
            description=payload.description,
            config=normalized_universe.model_dump(exclude_none=True),
        )
        return {
            "status": "success",
            "message": f"Universe config '{payload.name}' saved successfully",
            "version": int(saved["version"]),
        }
    except Exception as exc:
        logger.exception("Failed to save universe config '%s'.", payload.name)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/{name}")
async def delete_universe_config(name: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    repo = UniverseRepository(_require_postgres_dsn(request))
    try:
        references = repo.get_universe_config_references(name)
        if references["strategies"] or references["rankingSchemas"]:
            strategy_refs = ", ".join(references["strategies"]) or "none"
            ranking_refs = ", ".join(references["rankingSchemas"]) or "none"
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Universe config '{name}' is still referenced by strategies [{strategy_refs}] "
                    f"and ranking schemas [{ranking_refs}]."
                ),
            )
        deleted = repo.delete_universe_config(name)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Universe config '{name}' not found")
        return {"status": "success", "message": f"Universe config '{name}' deleted successfully"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to delete universe config '%s'.", name)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
