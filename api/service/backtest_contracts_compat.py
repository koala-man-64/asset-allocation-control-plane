from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

try:  # pragma: no cover - exercised once the published shared package includes these types.
    from asset_allocation_contracts.backtest import (  # type: ignore[attr-defined]
        BacktestLookupRequest,
        BacktestLookupResponse,
        BacktestAttributionExposureResponse,
        BacktestDataProvenance,
        BacktestExecutionAssumptions,
        BacktestResultLinks,
        BacktestReplayTimelineResponse,
        BacktestRunComparisonRequest,
        BacktestRunComparisonResponse,
        BacktestRunDetailResponse,
        BacktestRunRequest,
        BacktestRunResponse,
        BacktestStreamEvent,
        BacktestSummary,
        BacktestValidationReport,
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
    BacktestValidationVerdict = Literal["pass", "warn", "block"]
    BacktestValidationSeverity = Literal["info", "warning", "critical"]
    BacktestProvenanceQuality = Literal["complete", "partial", "missing", "contradictory"]
    BacktestReplayEventType = Literal[
        "signal",
        "order_decision",
        "fill_assumption",
        "position_update",
        "risk_limit",
        "exit",
        "corporate_action",
        "data_event",
        "cash",
    ]
    BacktestReplayExecutionSource = Literal["simulated", "broker_fill", "portfolio_ledger", "unknown"]
    BacktestAttributionSliceKind = Literal[
        "selection",
        "sizing",
        "timing",
        "implementation",
        "sector",
        "factor",
        "regime",
        "symbol",
        "outlier",
    ]
    BacktestComparisonAlignment = Literal["aligned", "caveated", "blocked"]

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


    class BacktestExecutionAssumptions(BaseModel):
        model_config = ConfigDict(extra="forbid")

        initialCapital: float | None = Field(default=None, gt=0)
        benchmarkSymbol: str | None = Field(default=None, min_length=1, max_length=32)
        costModel: str = Field(default="default", min_length=1, max_length=128)
        commissionBps: float | None = Field(default=None, ge=0)
        commissionPerShare: float | None = Field(default=None, ge=0)
        slippageBps: float | None = Field(default=None, ge=0)
        spreadBps: float | None = Field(default=None, ge=0)
        marketImpactBps: float | None = Field(default=None, ge=0)
        borrowCostBps: float | None = Field(default=None, ge=0)
        financingCostBps: float | None = Field(default=None, ge=0)
        participationCapPct: float | None = Field(default=None, ge=0, le=1)
        latencyBars: int | None = Field(default=None, ge=0)
        liquidityFilters: dict[str, Any] = Field(default_factory=dict)
        notes: str = Field(default="", max_length=2000)


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
        assumptions: BacktestExecutionAssumptions | None = None

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


    class BacktestValidationCheck(BaseModel):
        model_config = ConfigDict(extra="forbid")

        code: str = Field(..., min_length=1, max_length=128)
        label: str = Field(..., min_length=1, max_length=255)
        verdict: BacktestValidationVerdict
        severity: BacktestValidationSeverity = "info"
        message: str = Field(default="", max_length=2000)
        evidence: dict[str, Any] = Field(default_factory=dict)


    class BacktestValidationReport(BaseModel):
        model_config = ConfigDict(extra="forbid")

        verdict: BacktestValidationVerdict
        checks: list[BacktestValidationCheck] = Field(default_factory=list)
        blockedReasons: list[str] = Field(default_factory=list)
        warnings: list[str] = Field(default_factory=list)
        duplicateRun: RunStatusResponse | None = None
        reusedInflightRun: RunStatusResponse | None = None
        generatedAt: datetime | None = None


    class BacktestDataProvenance(BaseModel):
        model_config = ConfigDict(extra="forbid")

        quality: BacktestProvenanceQuality
        dataSnapshotId: str | None = Field(default=None, min_length=1, max_length=255)
        vendor: str | None = Field(default=None, min_length=1, max_length=128)
        source: str | None = Field(default=None, min_length=1, max_length=128)
        loadId: str | None = Field(default=None, min_length=1, max_length=255)
        schemaVersion: str | None = Field(default=None, min_length=1, max_length=64)
        adjustmentPolicy: str | None = Field(default=None, min_length=1, max_length=128)
        symbolMapVersion: str | None = Field(default=None, min_length=1, max_length=128)
        corporateActionState: str | None = Field(default=None, min_length=1, max_length=128)
        coveragePct: float | None = Field(default=None, ge=0, le=1)
        nullCount: int | None = Field(default=None, ge=0)
        gapCount: int | None = Field(default=None, ge=0)
        staleCount: int | None = Field(default=None, ge=0)
        quarantined: bool = False
        warnings: list[str] = Field(default_factory=list)


    class BacktestRunDetailResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        run: RunStatusResponse
        request: BacktestRunRequest | None = None
        effectiveConfig: dict[str, Any] = Field(default_factory=dict)
        configHash: str | None = Field(default=None, min_length=1, max_length=255)
        requestHash: str | None = Field(default=None, min_length=1, max_length=255)
        owner: str | None = Field(default=None, min_length=1, max_length=255)
        assumptions: BacktestExecutionAssumptions | None = None
        validation: BacktestValidationReport | None = None
        provenance: BacktestDataProvenance | None = None
        links: BacktestResultLinks | None = None
        warnings: list[str] = Field(default_factory=list)


    class BacktestReplayPositionState(BaseModel):
        model_config = ConfigDict(extra="forbid")

        symbol: str = Field(..., min_length=1, max_length=32)
        quantity: float
        marketValue: float | None = None
        weight: float | None = None
        averageCost: float | None = None
        unrealizedPnl: float | None = None


    class BacktestReplayEvent(BaseModel):
        model_config = ConfigDict(extra="forbid")

        eventId: str = Field(..., min_length=1, max_length=255)
        sequence: int = Field(..., ge=0)
        timestamp: datetime
        eventType: BacktestReplayEventType
        symbol: str | None = Field(default=None, min_length=1, max_length=32)
        ruleId: str | None = Field(default=None, min_length=1, max_length=128)
        source: BacktestReplayExecutionSource = "simulated"
        summary: str = Field(default="", max_length=1000)
        beforeCash: float | None = None
        afterCash: float | None = None
        beforeGrossExposure: float | None = None
        afterGrossExposure: float | None = None
        beforeNetExposure: float | None = None
        afterNetExposure: float | None = None
        beforePositions: list[BacktestReplayPositionState] = Field(default_factory=list)
        afterPositions: list[BacktestReplayPositionState] = Field(default_factory=list)
        transactionCost: float | None = None
        benchmarkPrice: float | None = None
        evidence: dict[str, Any] = Field(default_factory=dict)
        warnings: list[str] = Field(default_factory=list)


    class BacktestReplayTimelineResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        runId: str = Field(..., min_length=1, max_length=128)
        events: list[BacktestReplayEvent]
        total: int = Field(..., ge=0)
        limit: int = Field(..., ge=1)
        offset: int = Field(..., ge=0)
        nextOffset: int | None = Field(default=None, ge=0)
        warnings: list[str] = Field(default_factory=list)


    class BacktestGrossToNetBridge(BaseModel):
        model_config = ConfigDict(extra="forbid")

        grossReturn: float | None = None
        commissionDrag: float | None = None
        slippageDrag: float | None = None
        spreadDrag: float | None = None
        marketImpactDrag: float | None = None
        borrowFinancingDrag: float | None = None
        netReturn: float | None = None
        costDragBps: float | None = None


    class BacktestAttributionSlice(BaseModel):
        model_config = ConfigDict(extra="forbid")

        kind: BacktestAttributionSliceKind
        name: str = Field(..., min_length=1, max_length=255)
        contributionReturn: float | None = None
        contributionPnl: float | None = None
        exposureAvg: float | None = None
        tradeCount: int | None = Field(default=None, ge=0)
        notes: list[str] = Field(default_factory=list)


    class BacktestAttributionExposureResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        runId: str = Field(..., min_length=1, max_length=128)
        asOf: datetime | None = None
        grossToNet: BacktestGrossToNetBridge | None = None
        slices: list[BacktestAttributionSlice] = Field(default_factory=list)
        concentration: list[BacktestAttributionSlice] = Field(default_factory=list)
        grossExposureAvg: float | None = None
        netExposureAvg: float | None = None
        turnover: float | None = None
        warnings: list[str] = Field(default_factory=list)


    class BacktestRunComparisonRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        baselineRunId: str = Field(..., min_length=1, max_length=128)
        challengerRunIds: list[str] = Field(..., min_length=1, max_length=10)
        metricKeys: list[str] = Field(default_factory=list)

        @model_validator(mode="after")
        def validate_distinct_runs(self) -> "BacktestRunComparisonRequest":
            all_run_ids = [self.baselineRunId, *self.challengerRunIds]
            if len(set(all_run_ids)) != len(all_run_ids):
                raise ValueError("Backtest run comparison requires distinct run ids.")
            return self


    class BacktestRunComparisonMetric(BaseModel):
        model_config = ConfigDict(extra="forbid")

        metric: str = Field(..., min_length=1, max_length=128)
        label: str = Field(..., min_length=1, max_length=255)
        unit: str = Field(..., min_length=1, max_length=64)
        values: dict[str, float | None] = Field(default_factory=dict)
        winnerRunId: str | None = Field(default=None, min_length=1, max_length=128)
        notes: str = Field(default="", max_length=1000)


    class BacktestRunComparisonResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        asOf: datetime
        alignment: BacktestComparisonAlignment
        baselineRunId: str = Field(..., min_length=1, max_length=128)
        runs: list[RunStatusResponse]
        metrics: list[BacktestRunComparisonMetric] = Field(default_factory=list)
        alignmentWarnings: list[str] = Field(default_factory=list)
        blockedReasons: list[str] = Field(default_factory=list)


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
    "BacktestAttributionExposureResponse",
    "BacktestDataProvenance",
    "BacktestExecutionAssumptions",
    "BacktestResultLinks",
    "BacktestReplayTimelineResponse",
    "BacktestRunComparisonRequest",
    "BacktestRunComparisonResponse",
    "BacktestRunDetailResponse",
    "BacktestRunRequest",
    "BacktestRunResponse",
    "BacktestStreamEvent",
    "BacktestSummary",
    "BacktestValidationReport",
    "ClosedPositionListResponse",
    "RunListResponse",
    "RunPinsResponse",
    "RunRecordResponse",
    "RunStatusResponse",
    "StrategyReferenceInput",
    "TradeRole",
]
