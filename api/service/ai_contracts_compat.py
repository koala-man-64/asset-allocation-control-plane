from __future__ import annotations

from asset_allocation_contracts.ai_chat import (
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
from pydantic import TypeAdapter


AI_CHAT_REQUEST_OPENAPI_SCHEMA = AiChatRequest.model_json_schema()
AI_CHAT_STREAM_EVENT_OPENAPI_SCHEMA = TypeAdapter(AiChatStreamEvent).json_schema()

__all__ = [
    "AiChatCompletedData",
    "AiChatCompletedEvent",
    "AiChatError",
    "AiChatErrorData",
    "AiChatErrorEvent",
    "AiChatOutputTextDeltaData",
    "AiChatOutputTextDeltaEvent",
    "AiChatReasoningSummaryDeltaData",
    "AiChatReasoningSummaryDeltaEvent",
    "AiChatRequest",
    "AiChatStartedData",
    "AiChatStartedEvent",
    "AiChatStatusData",
    "AiChatStatusEvent",
    "AiChatStreamEvent",
    "AI_CHAT_REQUEST_OPENAPI_SCHEMA",
    "AI_CHAT_STREAM_EVENT_OPENAPI_SCHEMA",
]
