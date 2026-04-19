from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

try:  # pragma: no cover - exercised after the published contracts package adds intraday models.
    from asset_allocation_contracts.intraday import (  # type: ignore[attr-defined]
        IntradayEventSeverity,
        IntradayMarketSession,
        IntradayMonitorClaimRequest,
        IntradayMonitorClaimResponse,
        IntradayMonitorCompleteRequest,
        IntradayMonitorEvent,
        IntradayMonitorFailRequest,
        IntradayMonitorRunStatus,
        IntradayMonitorRunSummary,
        IntradayMonitorTriggerKind,
        IntradayRefreshBatchStatus,
        IntradayRefreshBatchSummary,
        IntradayRefreshClaimRequest,
        IntradayRefreshClaimResponse,
        IntradayRefreshCompleteRequest,
        IntradayRefreshFailRequest,
        IntradaySymbolMonitorStatus,
        IntradaySymbolStatus,
        IntradayWatchlistDetail,
        IntradayWatchlistSummary,
        IntradayWatchlistUpsertRequest,
    )
except Exception:  # pragma: no cover - default while control-plane depends on a published contracts version without intraday.
    IntradayMarketSession = Literal["us_equities_regular"]
    IntradayMonitorRunStatus = Literal["queued", "claimed", "completed", "failed"]
    IntradayRefreshBatchStatus = Literal["queued", "claimed", "completed", "failed"]
    IntradayMonitorTriggerKind = Literal["scheduled", "manual"]
    IntradayEventSeverity = Literal["info", "warning", "error"]
    IntradaySymbolMonitorStatus = Literal["idle", "observed", "refresh_queued", "refreshed", "failed"]

    def _normalize_symbol(value: object) -> str:
        symbol = str(value or "").strip().upper()
        if not symbol:
            raise ValueError("Symbol values must be non-empty.")
        if len(symbol) > 32:
            raise ValueError("Symbol values must be 32 characters or fewer.")
        return symbol

    def _normalize_symbols(values: object) -> list[str]:
        if values is None:
            return []
        if not isinstance(values, list):
            raise TypeError("symbols must be a list.")

        normalized: list[str] = []
        seen: set[str] = set()
        for raw in values:
            symbol = _normalize_symbol(raw)
            if symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
        return normalized

    class IntradayWatchlistSummary(BaseModel):
        model_config = ConfigDict(extra="forbid")

        watchlistId: str = Field(min_length=1, max_length=64)
        name: str = Field(min_length=1, max_length=255)
        description: str | None = Field(default=None, max_length=2_000)
        enabled: bool = True
        symbolCount: int = Field(default=0, ge=0)
        pollIntervalMinutes: int = Field(default=5, ge=1, le=1_440)
        refreshCooldownMinutes: int = Field(default=15, ge=1, le=1_440)
        autoRefreshEnabled: bool = True
        marketSession: IntradayMarketSession = "us_equities_regular"
        nextDueAt: datetime | None = None
        lastRunAt: datetime | None = None
        updatedAt: datetime | None = None


    class IntradayWatchlistDetail(IntradayWatchlistSummary):
        model_config = ConfigDict(extra="forbid")

        symbols: list[str] = Field(default_factory=list)
        createdAt: datetime | None = None

        _normalize_symbols_field = field_validator("symbols", mode="before")(_normalize_symbols)


    class IntradayWatchlistUpsertRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        name: str = Field(min_length=1, max_length=255)
        description: str | None = Field(default=None, max_length=2_000)
        enabled: bool = True
        symbols: list[str] = Field(default_factory=list)
        pollIntervalMinutes: int = Field(default=5, ge=1, le=1_440)
        refreshCooldownMinutes: int = Field(default=15, ge=1, le=1_440)
        autoRefreshEnabled: bool = True
        marketSession: IntradayMarketSession = "us_equities_regular"

        _normalize_symbols_field = field_validator("symbols", mode="before")(_normalize_symbols)


    class IntradaySymbolStatus(BaseModel):
        model_config = ConfigDict(extra="forbid")

        watchlistId: str | None = Field(default=None, min_length=1, max_length=64)
        symbol: str = Field(min_length=1, max_length=32)
        monitorStatus: IntradaySymbolMonitorStatus = "idle"
        lastSnapshotAt: datetime | None = None
        lastObservedPrice: float | None = None
        lastSuccessfulMarketRefreshAt: datetime | None = None
        lastRunId: str | None = Field(default=None, min_length=1, max_length=64)
        lastError: str | None = Field(default=None, max_length=2_000)
        updatedAt: datetime | None = None

        @field_validator("symbol", mode="before")
        @classmethod
        def _normalize_symbol_field(cls, value: object) -> str:
            return _normalize_symbol(value)


    class IntradayMonitorRunSummary(BaseModel):
        model_config = ConfigDict(extra="forbid")

        runId: str = Field(min_length=1, max_length=64)
        watchlistId: str = Field(min_length=1, max_length=64)
        watchlistName: str | None = Field(default=None, max_length=255)
        triggerKind: IntradayMonitorTriggerKind = "scheduled"
        status: IntradayMonitorRunStatus = "queued"
        forceRefresh: bool = False
        symbolCount: int = Field(default=0, ge=0)
        observedSymbolCount: int = Field(default=0, ge=0)
        eligibleRefreshCount: int = Field(default=0, ge=0)
        refreshBatchCount: int = Field(default=0, ge=0)
        executionName: str | None = Field(default=None, max_length=255)
        dueAt: datetime | None = None
        queuedAt: datetime | None = None
        claimedAt: datetime | None = None
        completedAt: datetime | None = None
        lastError: str | None = Field(default=None, max_length=2_000)


    class IntradayMonitorEvent(BaseModel):
        model_config = ConfigDict(extra="forbid")

        eventId: str | None = Field(default=None, min_length=1, max_length=64)
        runId: str | None = Field(default=None, min_length=1, max_length=64)
        watchlistId: str | None = Field(default=None, min_length=1, max_length=64)
        symbol: str | None = Field(default=None, min_length=1, max_length=32)
        eventType: str = Field(min_length=1, max_length=64)
        severity: IntradayEventSeverity = "info"
        message: str = Field(min_length=1, max_length=1_000)
        details: dict[str, Any] = Field(default_factory=dict)
        createdAt: datetime | None = None

        @field_validator("symbol", mode="before")
        @classmethod
        def _normalize_optional_symbol_field(cls, value: object) -> str | None:
            if value is None:
                return None
            return _normalize_symbol(value)


    class IntradayMonitorClaimRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        executionName: str | None = Field(default=None, max_length=255)


    class IntradayMonitorClaimResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        run: IntradayMonitorRunSummary | None = None
        watchlist: IntradayWatchlistDetail | None = None
        currentSymbolStatuses: list[IntradaySymbolStatus] = Field(default_factory=list)
        claimToken: str | None = Field(default=None, min_length=1, max_length=128)


    class IntradayMonitorCompleteRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        claimToken: str = Field(min_length=1, max_length=128)
        symbolStatuses: list[IntradaySymbolStatus] = Field(default_factory=list)
        events: list[IntradayMonitorEvent] = Field(default_factory=list)
        refreshSymbols: list[str] = Field(default_factory=list)

        _normalize_refresh_symbols = field_validator("refreshSymbols", mode="before")(_normalize_symbols)


    class IntradayMonitorFailRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        claimToken: str = Field(min_length=1, max_length=128)
        error: str = Field(min_length=1, max_length=2_000)


    class IntradayRefreshBatchSummary(BaseModel):
        model_config = ConfigDict(extra="forbid")

        batchId: str = Field(min_length=1, max_length=64)
        runId: str = Field(min_length=1, max_length=64)
        watchlistId: str = Field(min_length=1, max_length=64)
        watchlistName: str | None = Field(default=None, max_length=255)
        domain: str = Field(default="market", min_length=1, max_length=64)
        bucketLetter: str = Field(min_length=1, max_length=1)
        status: IntradayRefreshBatchStatus = "queued"
        symbols: list[str] = Field(default_factory=list)
        symbolCount: int = Field(default=0, ge=0)
        executionName: str | None = Field(default=None, max_length=255)
        claimedAt: datetime | None = None
        completedAt: datetime | None = None
        createdAt: datetime | None = None
        updatedAt: datetime | None = None
        lastError: str | None = Field(default=None, max_length=2_000)

        _normalize_symbols_field = field_validator("symbols", mode="before")(_normalize_symbols)


    class IntradayRefreshClaimRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        executionName: str | None = Field(default=None, max_length=255)


    class IntradayRefreshClaimResponse(BaseModel):
        model_config = ConfigDict(extra="forbid")

        batch: IntradayRefreshBatchSummary | None = None
        claimToken: str | None = Field(default=None, min_length=1, max_length=128)


    class IntradayRefreshCompleteRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        claimToken: str = Field(min_length=1, max_length=128)


    class IntradayRefreshFailRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        claimToken: str = Field(min_length=1, max_length=128)
        error: str = Field(min_length=1, max_length=2_000)


__all__ = [
    "IntradayEventSeverity",
    "IntradayMarketSession",
    "IntradayMonitorClaimRequest",
    "IntradayMonitorClaimResponse",
    "IntradayMonitorCompleteRequest",
    "IntradayMonitorEvent",
    "IntradayMonitorFailRequest",
    "IntradayMonitorRunStatus",
    "IntradayMonitorRunSummary",
    "IntradayMonitorTriggerKind",
    "IntradayRefreshBatchStatus",
    "IntradayRefreshBatchSummary",
    "IntradayRefreshClaimRequest",
    "IntradayRefreshClaimResponse",
    "IntradayRefreshCompleteRequest",
    "IntradayRefreshFailRequest",
    "IntradaySymbolMonitorStatus",
    "IntradaySymbolStatus",
    "IntradayWatchlistDetail",
    "IntradayWatchlistSummary",
    "IntradayWatchlistUpsertRequest",
]
