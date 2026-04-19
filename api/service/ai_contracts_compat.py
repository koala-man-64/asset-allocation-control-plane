from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

try:  # pragma: no cover - exercised when the shared contracts package is published.
    from asset_allocation_contracts.ai_chat import (  # type: ignore[attr-defined]
        AiChatCompletedData,
        AiChatCompletedEvent,
        AiChatError,
        AiChatErrorData,
        AiChatErrorEvent,
        AiChatOutputTextDeltaData,
        AiChatOutputTextDeltaEvent,
        AiChatReasoningSummaryDeltaData,
        AiChatReasoningSummaryDeltaEvent,
        AiChatRequest,
        AiChatStartedData,
        AiChatStartedEvent,
        AiChatStatusData,
        AiChatStatusEvent,
        AiChatStreamEvent,
    )
except Exception:  # pragma: no cover - default path until the shared package gains these types.
    class AiChatRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")

        prompt: str = Field(min_length=1, max_length=40_000)
        role: str | None = Field(default=None, max_length=200)
        systemInstructions: str | None = Field(default=None, max_length=10_000)


    class AiChatError(BaseModel):
        model_config = ConfigDict(extra="forbid")

        code: str = Field(min_length=1, max_length=120)
        message: str = Field(min_length=1, max_length=2_000)
        retryable: bool


    class AiChatStartedData(BaseModel):
        model_config = ConfigDict(extra="forbid")

        requestId: str = Field(min_length=1)
        model: str = Field(min_length=1)
        providerResponseId: str | None = None


    class AiChatStatusData(BaseModel):
        model_config = ConfigDict(extra="forbid")

        code: str = Field(min_length=1, max_length=120)
        message: str = Field(min_length=1, max_length=500)
        providerResponseId: str | None = None


    class AiChatReasoningSummaryDeltaData(BaseModel):
        model_config = ConfigDict(extra="forbid")

        delta: str = Field(min_length=1)


    class AiChatOutputTextDeltaData(BaseModel):
        model_config = ConfigDict(extra="forbid")

        delta: str = Field(min_length=1)


    class AiChatCompletedData(BaseModel):
        model_config = ConfigDict(extra="forbid")

        requestId: str = Field(min_length=1)
        model: str = Field(min_length=1)
        providerResponseId: str | None = None
        outputText: str = ""
        reasoningSummary: str = ""
        finishReason: str | None = None


    class AiChatErrorData(BaseModel):
        model_config = ConfigDict(extra="forbid")

        error: AiChatError


    class _AiChatBaseEvent(BaseModel):
        model_config = ConfigDict(extra="forbid")

        sequenceNumber: int = Field(ge=1)


    class AiChatStartedEvent(_AiChatBaseEvent):
        event: Literal["started"]
        data: AiChatStartedData


    class AiChatStatusEvent(_AiChatBaseEvent):
        event: Literal["status"]
        data: AiChatStatusData


    class AiChatReasoningSummaryDeltaEvent(_AiChatBaseEvent):
        event: Literal["reasoning_summary_delta"]
        data: AiChatReasoningSummaryDeltaData


    class AiChatOutputTextDeltaEvent(_AiChatBaseEvent):
        event: Literal["output_text_delta"]
        data: AiChatOutputTextDeltaData


    class AiChatCompletedEvent(_AiChatBaseEvent):
        event: Literal["completed"]
        data: AiChatCompletedData


    class AiChatErrorEvent(_AiChatBaseEvent):
        event: Literal["error"]
        data: AiChatErrorData


    AiChatStreamEvent = Annotated[
        AiChatStartedEvent
        | AiChatStatusEvent
        | AiChatReasoningSummaryDeltaEvent
        | AiChatOutputTextDeltaEvent
        | AiChatCompletedEvent
        | AiChatErrorEvent,
        Field(discriminator="event"),
    ]


AI_CHAT_REQUEST_OPENAPI_SCHEMA = AiChatRequest.model_json_schema()
AI_CHAT_STREAM_EVENT_OPENAPI_SCHEMA = TypeAdapter(AiChatStreamEvent).json_schema()
