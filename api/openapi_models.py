from __future__ import annotations

from asset_allocation_contracts.strategy import UniverseDefinition as SharedUniverseDefinition
from pydantic import BaseModel, ConfigDict, Field

from core.strategy_engine import StrategyConfig as SharedStrategyConfig


class StrategyConfigOutput(SharedStrategyConfig):
    model_config = ConfigDict(title="StrategyConfig-Output")


class UniverseDefinitionOutput(SharedUniverseDefinition):
    model_config = ConfigDict(title="UniverseDefinition-Output")


class UniversePreviewResponse(BaseModel):
    source: str
    symbolCount: int
    sampleSymbols: list[str]
    fieldsUsed: list[str]
    warnings: list[str] = Field(default_factory=list)
