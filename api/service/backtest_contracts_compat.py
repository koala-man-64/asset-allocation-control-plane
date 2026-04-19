from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

try:  # pragma: no cover - exercised once the published shared package includes these types.
    from asset_allocation_contracts.backtest import (  # type: ignore[attr-defined]
        BacktestLookupRequest,
        BacktestLookupResponse,
        BacktestResultLinks,
        BacktestRunRequest,
        BacktestRunResponse,
        BacktestStreamEvent,
        BacktestSummary,
        ClosedPositionListResponse,
        RunListResponse,
        RunPinsResponse,
        RunRecordResponse,
        RunStatusResponse,
        StrategyReferenceInput,
        TradeRole,
    )
except Exception:  # pragma: no cover - default path while control-plane depends on unpublished backtest route models.
    from asset_allocation_contracts.backtest import (
        BacktestResultMetadata,
        BacktestSummary,
        ClosedPositionListResponse,
        RunListResponse,
        RunPinsResponse,
        RunRecordResponse,
        RunStatusResponse,
        TradeRole,
    )
    from asset_allocation_contracts.strategy import StrategyConfig

    BacktestLookupState = Literal["not_run", "queued", "running", "completed", "failed"]
    BacktestStreamEventType = Literal["accepted", "status", "heartbeat", "completed", "failed"]

    class StrategyReferenceInput(BaseModel):
        model_config = ConfigDict(extra="forbid")

        strategyName: str = Field(..., min_length=1, max_length=128)
        strategyVersion: int | None = Field(default=None, ge=1)


    class BacktestResultLinks(BaseModel):
        model_config = ConfigDict(extra="forbid")

        summaryUrl: str = Field(..., min_length=1)
        metricsTimeseriesUrl: str = Field(..., min_length=1)
        metricsRollingUrl: str = Field(..., min_length=1)
        tradesUrl: str = Field(..., min_length=1)
        closedPositionsUrl: str = Field(..., min_length=1)


    class _BacktestRequestBase(BaseModel):
        model_config = ConfigDict(
            extra="forbid",
            json_schema_extra={
                "oneOf": [
                    {"required": ["strategyRef"]},
                    {"required": ["strategyConfig"]},
                ]
            },
        )

        strategyRef: StrategyReferenceInput | None = None
        strategyConfig: StrategyConfig | None = None
        startTs: datetime
        endTs: datetime
        barSize: str = Field(..., min_length=1, max_length=32)
        runName: str | None = Field(default=None, max_length=255)

        @model_validator(mode="after")
        def validate_strategy_input(self) -> "_BacktestRequestBase":
            has_strategy_ref = self.strategyRef is not None
            has_strategy_config = self.strategyConfig is not None
            if has_strategy_ref == has_strategy_config:
                raise ValueError("Exactly one of strategyRef or strategyConfig must be provided.")
            return self


    class BacktestLookupRequest(_BacktestRequestBase):
        pass


    class BacktestRunRequest(_BacktestRequestBase):
        pass


    class BacktestLookupResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        found: bool
        state: BacktestLookupState
        run: RunStatusResponse | None = None
        result: BacktestSummary | None = None
        links: BacktestResultLinks | None = None


    class BacktestRunResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        run: RunStatusResponse
        created: bool
        reusedInflight: bool
        streamUrl: str = Field(..., min_length=1)


    class BacktestStreamEvent(BaseModel):
        model_config = ConfigDict(extra="forbid")

        event: BacktestStreamEventType
        run: RunStatusResponse
        summary: BacktestSummary | None = None
        metadata: BacktestResultMetadata | None = None
        links: BacktestResultLinks | None = None


__all__ = [
    "BacktestLookupRequest",
    "BacktestLookupResponse",
    "BacktestResultLinks",
    "BacktestRunRequest",
    "BacktestRunResponse",
    "BacktestStreamEvent",
    "BacktestSummary",
    "ClosedPositionListResponse",
    "RunListResponse",
    "RunPinsResponse",
    "RunRecordResponse",
    "RunStatusResponse",
    "StrategyReferenceInput",
    "TradeRole",
]
