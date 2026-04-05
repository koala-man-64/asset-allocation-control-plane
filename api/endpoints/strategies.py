import logging
from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from api.service.dependencies import validate_auth

from core.strategy_engine import StrategyConfig, UniverseDefinition
from core.strategy_engine.universe import list_gold_universe_catalog, preview_gold_universe
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

router = APIRouter()


class StrategySummaryResponse(BaseModel):
    name: str
    type: str = "configured"
    description: str = ""
    output_table_name: str | None = None
    updated_at: datetime | None = None


class StrategyDetailResponse(StrategySummaryResponse):
    config: StrategyConfig


class StrategyUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    config: StrategyConfig
    description: str = ""
    type: str = "configured"


class UniverseCatalogColumnResponse(BaseModel):
    name: str
    dataType: str
    valueKind: str
    operators: list[str]


class UniverseCatalogTableResponse(BaseModel):
    name: str
    asOfColumn: str
    columns: list[UniverseCatalogColumnResponse]


class UniverseCatalogResponse(BaseModel):
    source: str
    tables: list[UniverseCatalogTableResponse]


class UniversePreviewRequest(BaseModel):
    universe: UniverseDefinition
    sampleLimit: int = Field(default=25, ge=1, le=100)


class UniversePreviewResponse(BaseModel):
    source: str
    symbolCount: int
    sampleSymbols: list[str]
    tablesUsed: list[str]
    warnings: list[str] = Field(default_factory=list)


def _normalize_strategy_config(dsn: str, config: Any) -> dict[str, Any]:
    normalized = StrategyConfig.model_validate(config or {})
    if not normalized.universeConfigName:
        raise ValueError("Strategy config must reference universeConfigName.")
    universe_repo = UniverseRepository(dsn)
    if not universe_repo.get_universe_config(normalized.universeConfigName):
        raise ValueError(f"Universe config '{normalized.universeConfigName}' not found.")
    return normalized.model_dump(exclude_none=True)


def _build_strategy_detail_response(strategy: dict[str, Any]) -> StrategyDetailResponse:
    return StrategyDetailResponse(
        name=str(strategy.get("name") or ""),
        type=str(strategy.get("type") or "configured"),
        description=str(strategy.get("description") or ""),
        output_table_name=strategy.get("output_table_name"),
        updated_at=strategy.get("updated_at"),
        config=StrategyConfig.model_validate(strategy.get("config") or {}),
    )


def _require_postgres_dsn(request: Request) -> str:
    settings = request.app.state.settings
    dsn = str(settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for strategy universe features.")
    return dsn


@router.get("/", response_model=List[StrategySummaryResponse])
async def list_strategies(request: Request) -> List[dict[str, Any]]:
    """
    List all available strategies.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    return repo.list_strategies()


@router.get("/universe/catalog", response_model=UniverseCatalogResponse)
async def get_universe_catalog(request: Request) -> UniverseCatalogResponse:
    """
    Return eligible Postgres gold tables and columns for strategy universe authoring.
    """
    validate_auth(request)
    try:
        return UniverseCatalogResponse.model_validate(
            list_gold_universe_catalog(_require_postgres_dsn(request))
        )
    except HTTPException:
        raise
    except ValueError as exc:
        logger.warning("Universe catalog validation failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Universe catalog load failed.")
        raise HTTPException(status_code=500, detail=f"Failed to load universe catalog: {exc}") from exc


@router.post("/universe/preview", response_model=UniversePreviewResponse)
async def preview_universe(payload: UniversePreviewRequest, request: Request) -> UniversePreviewResponse:
    """
    Preview the current matching symbol set for a draft strategy universe.
    """
    validate_auth(request)
    try:
        return UniversePreviewResponse.model_validate(
            preview_gold_universe(
                _require_postgres_dsn(request),
                payload.universe,
                sample_limit=payload.sampleLimit,
            )
        )
    except HTTPException:
        raise
    except ValueError as exc:
        logger.warning("Universe preview validation failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Universe preview failed.")
        raise HTTPException(status_code=500, detail=f"Failed to preview universe: {exc}") from exc


@router.get("/{name}/detail", response_model=StrategyDetailResponse)
async def get_strategy_detail(name: str, request: Request) -> StrategyDetailResponse:
    """
    Get normalized metadata and configuration for a specific strategy.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    strategy = repo.get_strategy(name)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")
    return _build_strategy_detail_response(strategy)


@router.get("/{name}", response_model=StrategyConfig)
async def get_strategy(name: str, request: Request) -> StrategyConfig:
    """
    Get configuration for a specific strategy by name.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    config = repo.get_strategy_config(name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' configuration not found")
    return StrategyConfig.model_validate(config)


@router.post("/")
async def save_strategy(strategy: StrategyUpsertRequest, request: Request) -> dict[str, str]:
    """
    Create or update a strategy configuration.
    Requires authentication.
    """
    validate_auth(request)
    settings = request.app.state.settings
    dsn = _require_postgres_dsn(request)
    repo = StrategyRepository(settings.postgres_dsn)

    try:
        repo.save_strategy(
            name=strategy.name,
            config=_normalize_strategy_config(dsn, strategy.config),
            strategy_type=strategy.type or "configured",
            description=strategy.description or "",
        )
        return {"status": "success", "message": f"Strategy '{strategy.name}' saved successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error saving strategy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{name}")
async def delete_strategy(name: str, request: Request) -> dict[str, str]:
    """
    Delete a strategy configuration by name.
    Requires authentication.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)

    try:
        deleted = repo.delete_strategy(name)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found")
        return {"status": "success", "message": f"Strategy '{name}' deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting strategy '{name}': {e}")
        raise HTTPException(status_code=500, detail=str(e))
