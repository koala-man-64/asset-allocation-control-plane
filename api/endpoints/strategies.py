import logging
from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from api.openapi_models import StrategyConfigOutput, UniversePreviewResponse
from api.service.dependencies import validate_auth

from core.strategy_engine import StrategyConfig
from core.strategy_engine.universe import (
    list_gold_universe_catalog,
    preview_gold_universe,
    _normalize_universe_definition,
    _publicize_universe_definition,
)
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
    config: StrategyConfigOutput


class StrategyUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    config: dict[str, Any]
    description: str = ""
    type: str = "configured"


class UniverseCatalogFieldResponse(BaseModel):
    id: str
    label: str
    valueKind: str
    operators: list[str]


class UniverseCatalogResponse(BaseModel):
    source: str
    fields: list[UniverseCatalogFieldResponse]


class UniversePreviewRequest(BaseModel):
    universe: dict[str, Any]
    sampleLimit: int = Field(default=25, ge=1, le=100)


def _normalize_strategy_config(dsn: str, config: Any) -> dict[str, Any]:
    payload = config.model_dump(exclude_none=True) if hasattr(config, "model_dump") else dict(config or {})
    universe_payload = payload.get("universe")
    if universe_payload is not None:
        normalized_universe = _normalize_universe_definition(universe_payload)
        payload["universe"] = normalized_universe.model_dump(exclude_none=True)
    normalized = StrategyConfig.model_validate(payload)
    universe_ref = normalized.componentRefs.universe if normalized.componentRefs else None
    universe_name = normalized.universeConfigName or (universe_ref.name if universe_ref else None)
    if not universe_name and normalized.universe is None:
        raise ValueError("Strategy config must reference componentRefs.universe, universeConfigName, or inline universe.")
    if universe_name and not UniverseRepository(dsn).get_universe_config(universe_name):
        raise ValueError(f"Universe config '{universe_name}' not found.")
    return normalized.model_dump(exclude_none=True)


def _publicize_strategy_config(config: Any) -> dict[str, Any]:
    payload = config.model_dump(exclude_none=True) if hasattr(config, "model_dump") else dict(config or {})
    universe_payload = payload.get("universe")
    if universe_payload is not None:
        payload["universe"] = _publicize_universe_definition(universe_payload)
    return payload


def _build_strategy_detail_response(strategy: dict[str, Any]) -> StrategyDetailResponse:
    return StrategyDetailResponse(
        name=str(strategy.get("name") or ""),
        type=str(strategy.get("type") or "configured"),
        description=str(strategy.get("description") or ""),
        output_table_name=strategy.get("output_table_name"),
        updated_at=strategy.get("updated_at"),
        config=StrategyConfigOutput.model_validate(_publicize_strategy_config(strategy.get("config") or {})),
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
    Return eligible public universe fields for strategy universe authoring.
    """
    validate_auth(request)
    try:
        return UniverseCatalogResponse.model_validate(list_gold_universe_catalog(_require_postgres_dsn(request)))
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


@router.get("/{name}", response_model=StrategyConfigOutput)
async def get_strategy(name: str, request: Request) -> StrategyConfigOutput:
    """
    Get configuration for a specific strategy by name.
    """
    validate_auth(request)
    settings = request.app.state.settings
    repo = StrategyRepository(settings.postgres_dsn)
    config = repo.get_strategy_config(name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' configuration not found")
    return StrategyConfigOutput.model_validate(config)


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
