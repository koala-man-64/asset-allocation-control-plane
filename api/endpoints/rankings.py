from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from api.service.dependencies import validate_auth
from core.ranking_engine.catalog import list_gold_ranking_catalog
from core.ranking_engine.contracts import RankingMaterializationSummary, RankingSchemaConfig
from core.ranking_engine.service import materialize_strategy_rankings, preview_strategy_rankings
from core.ranking_repository import RankingRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

router = APIRouter()


class RankingCatalogColumnResponse(BaseModel):
    name: str
    dataType: str
    valueKind: str


class RankingCatalogTableResponse(BaseModel):
    name: str
    asOfColumn: str
    columns: list[RankingCatalogColumnResponse]


class RankingCatalogResponse(BaseModel):
    source: str
    tables: list[RankingCatalogTableResponse]


class RankingSchemaSummaryResponse(BaseModel):
    name: str
    description: str = ""
    version: int = 1
    updated_at: datetime | None = None


class RankingSchemaDetailResponse(RankingSchemaSummaryResponse):
    config: RankingSchemaConfig


class RankingSchemaUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str = ""
    config: RankingSchemaConfig


class RankingPreviewRequest(BaseModel):
    strategyName: str = Field(..., min_length=1, max_length=128)
    asOfDate: date
    limit: int = Field(default=25, ge=1, le=100)
    schemaName: str | None = Field(default=None, min_length=1, max_length=128)
    schemaPayload: RankingSchemaConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_schema_alias(cls, payload: Any) -> Any:
        if isinstance(payload, dict) and "schema" in payload and "schemaPayload" not in payload:
            normalized = dict(payload)
            normalized["schemaPayload"] = normalized.pop("schema")
            return normalized
        return payload

    @model_validator(mode="after")
    def validate_schema_source(self) -> "RankingPreviewRequest":
        if not self.schemaName and self.schemaPayload is None:
            raise ValueError("schemaName or schema is required.")
        return self


class RankingPreviewRowResponse(BaseModel):
    symbol: str
    rank: int
    score: float


class RankingPreviewResponse(BaseModel):
    strategyName: str
    asOfDate: date
    rowCount: int
    rows: list[RankingPreviewRowResponse]
    warnings: list[str] = Field(default_factory=list)


class RankingMaterializeRequest(BaseModel):
    strategyName: str = Field(..., min_length=1, max_length=128)
    startDate: date | None = None
    endDate: date | None = None


def _require_postgres_dsn(request: Request) -> str:
    settings = request.app.state.settings
    dsn = str(settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for ranking features.")
    return dsn


def _build_detail_response(schema: dict[str, Any]) -> RankingSchemaDetailResponse:
    return RankingSchemaDetailResponse(
        name=str(schema.get("name") or ""),
        description=str(schema.get("description") or ""),
        version=int(schema.get("version") or 1),
        updated_at=schema.get("updated_at"),
        config=RankingSchemaConfig.model_validate(schema.get("config") or {}),
    )


@router.get("/", response_model=List[RankingSchemaSummaryResponse])
async def list_ranking_schemas(request: Request) -> List[dict[str, Any]]:
    validate_auth(request)
    repo = RankingRepository(_require_postgres_dsn(request))
    return repo.list_ranking_schemas()


@router.get("/catalog", response_model=RankingCatalogResponse)
async def get_ranking_catalog(request: Request) -> RankingCatalogResponse:
    validate_auth(request)
    try:
        return RankingCatalogResponse.model_validate(list_gold_ranking_catalog(_require_postgres_dsn(request)))
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Ranking catalog load failed.")
        raise HTTPException(status_code=500, detail=f"Failed to load ranking catalog: {exc}") from exc


@router.get("/{name}/detail", response_model=RankingSchemaDetailResponse)
async def get_ranking_schema_detail(name: str, request: Request) -> RankingSchemaDetailResponse:
    validate_auth(request)
    repo = RankingRepository(_require_postgres_dsn(request))
    schema = repo.get_ranking_schema(name)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Ranking schema '{name}' not found")
    return _build_detail_response(schema)


@router.post("/")
async def save_ranking_schema(payload: RankingSchemaUpsertRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    if not payload.config.universeConfigName:
        raise HTTPException(status_code=400, detail="Ranking schema config must reference universeConfigName.")
    dsn = _require_postgres_dsn(request)
    universe_repo = UniverseRepository(dsn)
    if not universe_repo.get_universe_config(payload.config.universeConfigName):
        raise HTTPException(
            status_code=400,
            detail=f"Universe config '{payload.config.universeConfigName}' not found.",
        )
    repo = RankingRepository(dsn)
    try:
        saved = repo.save_ranking_schema(
            name=payload.name,
            description=payload.description,
            config=payload.config.model_dump(exclude_none=True),
        )
        return {
            "status": "success",
            "message": f"Ranking schema '{payload.name}' saved successfully",
            "version": int(saved["version"]),
        }
    except Exception as exc:
        logger.exception("Failed to save ranking schema '%s'.", payload.name)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/{name}")
async def delete_ranking_schema(name: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    repo = RankingRepository(_require_postgres_dsn(request))
    try:
        deleted = repo.delete_ranking_schema(name)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Ranking schema '{name}' not found")
        return {"status": "success", "message": f"Ranking schema '{name}' deleted successfully"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to delete ranking schema '%s'.", name)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/preview", response_model=RankingPreviewResponse)
async def preview_rankings(payload: RankingPreviewRequest, request: Request) -> RankingPreviewResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = RankingRepository(dsn)
    try:
        ranking_schema = payload.schemaPayload
        if ranking_schema is None:
            schema_record = repo.get_ranking_schema(str(payload.schemaName))
            if not schema_record:
                raise HTTPException(status_code=404, detail=f"Ranking schema '{payload.schemaName}' not found")
            ranking_schema = RankingSchemaConfig.model_validate(schema_record.get("config") or {})
        preview = preview_strategy_rankings(
            dsn,
            strategy_name=payload.strategyName,
            schema=ranking_schema,
            as_of_date=payload.asOfDate,
            limit=payload.limit,
        )
        return RankingPreviewResponse.model_validate(preview)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Ranking preview failed.")
        raise HTTPException(status_code=500, detail=f"Failed to preview rankings: {exc}") from exc


@router.post("/materialize", response_model=RankingMaterializationSummary)
async def materialize_rankings(payload: RankingMaterializeRequest, request: Request) -> RankingMaterializationSummary:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    try:
        result = materialize_strategy_rankings(
            dsn,
            strategy_name=payload.strategyName,
            start_date=payload.startDate,
            end_date=payload.endDate,
            triggered_by="api",
        )
        return RankingMaterializationSummary.model_validate(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Ranking materialization failed.")
        raise HTTPException(status_code=500, detail=f"Failed to materialize rankings: {exc}") from exc
