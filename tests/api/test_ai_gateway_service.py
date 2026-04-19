from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest

from api.service.auth import AuthContext
from api.service.ai_contracts_compat import AiChatRequest
from api.service.openai_responses_gateway import AiRelayAttachment, OpenAIResponsesGateway
from api.service.settings import AiRelaySettings


def _gateway_settings() -> AiRelaySettings:
    return AiRelaySettings(
        enabled=True,
        api_key="test-key",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        timeout_seconds=30.0,
        max_prompt_chars=40_000,
        max_files=4,
        max_file_bytes=5 * 1024 * 1024,
        max_total_file_bytes=20 * 1024 * 1024,
        max_output_tokens=4_000,
        required_roles=["AssetAllocation.AiRelay.Use"],
    )


def test_build_input_items_preserves_prompt_metadata_and_files() -> None:
    gateway = OpenAIResponsesGateway(_gateway_settings())
    chat_request = AiChatRequest(
        prompt="Summarize this file.",
        role="security reviewer",
        systemInstructions="Keep the answer concise.",
    )
    attachments = [
        AiRelayAttachment(filename="notes.txt", content_type="text/plain", data=b"hello"),
        AiRelayAttachment(filename="chart.png", content_type="image/png", data=b"png-bytes"),
    ]

    items = gateway.build_input_items(chat_request, attachments)

    assert items[0]["role"] == "user"
    content = items[0]["content"]
    assert content[0]["text"].startswith("Requested role/persona:")
    assert content[1]["text"].startswith("Additional caller instructions")
    assert content[2]["type"] == "input_file"
    assert content[2]["filename"] == "notes.txt"
    assert content[3]["type"] == "input_image"
    assert content[3]["image_url"].startswith("data:image/png;base64,")
    assert content[4] == {"type": "input_text", "text": "Summarize this file."}


@pytest.mark.asyncio
async def test_stream_response_maps_provider_events(monkeypatch: pytest.MonkeyPatch) -> None:
    payload_holder: dict[str, object] = {}

    class _FakeStream:
        def __init__(self, events):
            self._events = list(events)
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._events:
                raise StopAsyncIteration
            return self._events.pop(0)

        async def close(self):
            self.closed = True

    class _FakeResponses:
        def __init__(self, events):
            self._events = events

        async def create(self, **kwargs):
            payload_holder.update(kwargs)
            return _FakeStream(self._events)

    class _FakeAsyncOpenAI:
        def __init__(self, *, api_key: str, timeout: float):
            assert api_key == "test-key"
            assert timeout == 30.0
            self.responses = _FakeResponses(
                [
                    SimpleNamespace(type="response.created", response=SimpleNamespace(id="resp_123")),
                    SimpleNamespace(type="response.in_progress"),
                    SimpleNamespace(type="response.reasoning_summary_text.delta", delta="Thinking..."),
                    SimpleNamespace(type="response.output_text.delta", delta="Hello"),
                    SimpleNamespace(
                        type="response.completed",
                        response=SimpleNamespace(id="resp_123", output_text="Hello", status="completed"),
                    ),
                ]
            )

        async def close(self):
            return None

    fake_openai = types.ModuleType("openai")
    fake_openai.AsyncOpenAI = _FakeAsyncOpenAI
    fake_openai.APIConnectionError = type("APIConnectionError", (Exception,), {})
    fake_openai.APIStatusError = type("APIStatusError", (Exception,), {})
    fake_openai.APITimeoutError = type("APITimeoutError", (Exception,), {})
    fake_openai.RateLimitError = type("RateLimitError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "openai", fake_openai)

    gateway = OpenAIResponsesGateway(_gateway_settings())
    events = []
    async for event in gateway.stream_response(
        request_id="req-1",
        auth_context=AuthContext(mode="anonymous", subject=None, claims={}),
        chat_request=AiChatRequest(prompt="Say hello."),
        attachments=[],
    ):
        events.append(event.model_dump(mode="json"))

    assert [event["event"] for event in events] == [
        "started",
        "status",
        "reasoning_summary_delta",
        "output_text_delta",
        "completed",
    ]
    assert events[0]["data"]["providerResponseId"] == "resp_123"
    assert events[2]["data"]["delta"] == "Thinking..."
    assert events[3]["data"]["delta"] == "Hello"
    assert events[4]["data"]["outputText"] == "Hello"
    assert payload_holder["stream"] is True
    assert payload_holder["store"] is False
    assert payload_holder["model"] == "gpt-5.4-mini"
    assert payload_holder["reasoning"] == {"effort": "low", "summary": "auto"}
