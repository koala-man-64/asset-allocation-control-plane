from __future__ import annotations

import json
from typing import Any

import pytest

from api.service.ai_contracts_compat import (
    AiChatCompletedData,
    AiChatCompletedEvent,
    AiChatOutputTextDeltaData,
    AiChatOutputTextDeltaEvent,
    AiChatStartedData,
    AiChatStartedEvent,
)
from api.service.app import create_app
from api.service.auth import AuthContext, AuthError
from api.service.openai_responses_gateway import AiRelayAttachment, AiRelayGatewayError
from tests.api._auth import install_auth_stub
from tests.api._client import get_test_client


def _parse_sse_events(body: str) -> list[tuple[str, dict[str, Any]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    for block in body.split("\n\n"):
        lines = [line for line in block.splitlines() if line.strip()]
        if not lines or lines[0].startswith(":"):
            continue
        event_name = ""
        data_lines: list[str] = []
        for line in lines:
            if line.startswith("event:"):
                event_name = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].strip())
        if event_name and data_lines:
            events.append((event_name, json.loads("".join(data_lines))))
    return events


async def _success_stream(**kwargs):  # type: ignore[no-untyped-def]
    attachments = kwargs["attachments"]
    assert isinstance(attachments, list)
    yield AiChatStartedEvent(
        sequenceNumber=1,
        event="started",
        data=AiChatStartedData(requestId=kwargs["request_id"], model="gpt-5.4-mini", providerResponseId="resp_1"),
    )
    yield AiChatOutputTextDeltaEvent(
        sequenceNumber=2,
        event="output_text_delta",
        data=AiChatOutputTextDeltaData(delta="ok"),
    )
    yield AiChatCompletedEvent(
        sequenceNumber=3,
        event="completed",
        data=AiChatCompletedData(
            requestId=kwargs["request_id"],
            model="gpt-5.4-mini",
            providerResponseId="resp_1",
            outputText="ok",
            reasoningSummary="",
            finishReason="completed",
        ),
    )


@pytest.mark.asyncio
async def test_ai_chat_stream_json_request_returns_sse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")

    app = create_app()
    monkeypatch.setattr(app.state.ai_relay_gateway, "stream_response", _success_stream)

    async with get_test_client(app) as client:
        response = await client.post("/api/ai/chat/stream", json={"prompt": "Say ok."})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-store"

    events = _parse_sse_events(response.text)
    assert [name for name, _ in events] == ["started", "output_text_delta", "completed"]
    assert events[-1][1]["data"]["outputText"] == "ok"


@pytest.mark.asyncio
async def test_ai_chat_stream_multipart_request_supports_files(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("multipart")
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")

    captured: dict[str, Any] = {}

    async def _stream_with_capture(**kwargs):  # type: ignore[no-untyped-def]
        captured["attachments"] = kwargs["attachments"]
        async for event in _success_stream(**kwargs):
            yield event

    app = create_app()
    monkeypatch.setattr(app.state.ai_relay_gateway, "stream_response", _stream_with_capture)

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/ai/chat/stream",
            data={"request": json.dumps({"prompt": "Read the file."})},
            files=[("files", ("notes.txt", b"hello", "text/plain"))],
        )

    assert response.status_code == 200
    assert len(captured["attachments"]) == 1
    attachment = captured["attachments"][0]
    assert isinstance(attachment, AiRelayAttachment)
    assert attachment.filename == "notes.txt"
    assert attachment.content_type == "text/plain"
    assert attachment.data == b"hello"


@pytest.mark.asyncio
async def test_ai_chat_stream_returns_401_when_authentication_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_error=AuthError(status_code=401, detail="Unauthorized.", www_authenticate="Bearer"),
    )

    async with get_test_client(app) as client:
        response = await client.post("/api/ai/chat/stream", json={"prompt": "Say ok."})

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_ai_chat_stream_returns_403_for_missing_ai_role(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(mode="oidc", subject="user-1", claims={"roles": ["AssetAllocation.Access"]}),
    )

    async with get_test_client(app) as client:
        response = await client.post("/api/ai/chat/stream", json={"prompt": "Say ok."}, headers={"Authorization": "Bearer token"})

    assert response.status_code == 403
    assert response.json() == {"detail": "Missing required roles: AssetAllocation.AiRelay.Use."}


@pytest.mark.asyncio
async def test_ai_chat_stream_rejects_unsupported_files(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("multipart")
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/ai/chat/stream",
            data={"request": json.dumps({"prompt": "Read the file."})},
            files=[("files", ("malware.exe", b"MZ", "application/octet-stream"))],
        )

    assert response.status_code == 415


@pytest.mark.asyncio
async def test_ai_chat_stream_rejects_oversized_files(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("multipart")
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")
    monkeypatch.setenv("AI_RELAY_MAX_FILE_BYTES", "3")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/ai/chat/stream",
            data={"request": json.dumps({"prompt": "Read the file."})},
            files=[("files", ("notes.txt", b"toolarge", "text/plain"))],
        )

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_ai_chat_stream_returns_503_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_RELAY_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post("/api/ai/chat/stream", json={"prompt": "Say ok."})

    assert response.status_code == 503
    assert response.json() == {"detail": "AI relay is disabled."}


@pytest.mark.asyncio
async def test_ai_chat_stream_emits_terminal_error_event_on_upstream_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("AI_RELAY_ENABLED", "true")
    monkeypatch.setenv("AI_RELAY_API_KEY", "test-key")

    async def _failing_stream(**kwargs):  # type: ignore[no-untyped-def]
        yield AiChatStartedEvent(
            sequenceNumber=1,
            event="started",
            data=AiChatStartedData(requestId=kwargs["request_id"], model="gpt-5.4-mini", providerResponseId="resp_1"),
        )
        raise AiRelayGatewayError(
            status_code=502,
            code="upstream_failed",
            message="Upstream failed.",
            retryable=False,
        )

    app = create_app()
    monkeypatch.setattr(app.state.ai_relay_gateway, "stream_response", _failing_stream)

    async with get_test_client(app) as client:
        response = await client.post("/api/ai/chat/stream", json={"prompt": "very secret prompt"})

    assert response.status_code == 200
    events = _parse_sse_events(response.text)
    assert [name for name, _ in events] == ["started", "error"]
    assert events[-1][1]["data"]["error"]["code"] == "upstream_failed"
    assert "very secret prompt" not in caplog.text
