from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from starlette.datastructures import UploadFile as StarletteUploadFile

from api.service.ai_contracts_compat import (
    AI_CHAT_REQUEST_OPENAPI_SCHEMA,
    AI_CHAT_STREAM_EVENT_OPENAPI_SCHEMA,
    AiChatRequest,
)
from api.service.dependencies import get_ai_relay_gateway, get_settings, require_ai_relay_access
from api.service.openai_responses_gateway import AiRelayAttachment, AiRelayGatewayError


logger = logging.getLogger("asset-allocation.api.ai")
router = APIRouter()

_ALLOWED_CONTENT_TYPES = {
    "application/json",
    "application/pdf",
    "text/csv",
    "text/markdown",
    "text/plain",
    "image/gif",
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
}
_EXTENSION_CONTENT_TYPES = {
    ".csv": "text/csv",
    ".gif": "image/gif",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".json": "application/json",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".txt": "text/plain",
    ".webp": "image/webp",
}
_HEARTBEAT_SECONDS = 10.0


def _normalize_content_type(upload: UploadFile) -> str:
    raw = (upload.content_type or "").split(";", 1)[0].strip().lower()
    if raw and raw != "application/octet-stream":
        return raw

    guessed, _ = mimetypes.guess_type(upload.filename or "")
    if guessed:
        return guessed.lower()

    suffix = ""
    if upload.filename and "." in upload.filename:
        suffix = "." + upload.filename.rsplit(".", 1)[1].lower()
    return _EXTENSION_CONTENT_TYPES.get(suffix, raw)


async def _read_attachments(request: Request) -> tuple[AiChatRequest, list[AiRelayAttachment]]:
    settings = get_settings(request).ai_relay
    content_type = (request.headers.get("content-type") or "").lower()

    if "application/json" in content_type:
        payload = await request.json()
        try:
            chat_request = AiChatRequest.model_validate(payload)
        except Exception as exc:
            raise HTTPException(status_code=422, detail="Malformed AI chat request body.") from exc
        if len(chat_request.prompt) > settings.max_prompt_chars:
            raise HTTPException(status_code=413, detail="Prompt exceeds the configured size limit.")
        return chat_request, []

    if "multipart/form-data" not in content_type:
        raise HTTPException(status_code=415, detail="Unsupported media type. Use application/json or multipart/form-data.")

    try:
        form = await request.form()
    except Exception as exc:
        raise HTTPException(status_code=422, detail="Malformed multipart AI chat request.") from exc

    raw_request = form.get("request")
    if not isinstance(raw_request, str) or not raw_request.strip():
        raise HTTPException(status_code=422, detail="Multipart AI chat requests require a JSON 'request' field.")

    try:
        chat_request = AiChatRequest.model_validate_json(raw_request)
    except Exception as exc:
        raise HTTPException(status_code=422, detail="Multipart AI chat request JSON is invalid.") from exc

    if len(chat_request.prompt) > settings.max_prompt_chars:
        raise HTTPException(status_code=413, detail="Prompt exceeds the configured size limit.")

    raw_files = form.getlist("files")
    if len(raw_files) > settings.max_files:
        raise HTTPException(status_code=413, detail="Too many files were uploaded.")

    attachments: list[AiRelayAttachment] = []
    total_bytes = 0
    for item in raw_files:
        if not isinstance(item, (UploadFile, StarletteUploadFile)):
            raise HTTPException(status_code=422, detail="Multipart AI chat requests may only include file uploads in 'files'.")
        if not item.filename:
            raise HTTPException(status_code=422, detail="Uploaded files must include a filename.")

        content_type = _normalize_content_type(item)
        if content_type not in _ALLOWED_CONTENT_TYPES:
            raise HTTPException(status_code=415, detail=f"Unsupported file type: {item.filename}.")

        data = await item.read()
        if len(data) > settings.max_file_bytes:
            raise HTTPException(status_code=413, detail=f"Uploaded file exceeds the per-file size limit: {item.filename}.")

        total_bytes += len(data)
        if total_bytes > settings.max_total_file_bytes:
            raise HTTPException(status_code=413, detail="Combined uploaded files exceed the configured size limit.")

        attachments.append(
            AiRelayAttachment(
                filename=item.filename,
                content_type=content_type,
                data=data,
            )
        )

    return chat_request, attachments


def _sse_event(event_name: str, payload: dict[str, Any]) -> bytes:
    return f"event: {event_name}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n".encode("utf-8")


def _sse_comment(comment: str) -> bytes:
    return f": {comment}\n\n".encode("utf-8")


@router.post(
    "/chat/stream",
    summary="Stream an AI chat relay response",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": AI_CHAT_REQUEST_OPENAPI_SCHEMA,
                },
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["request"],
                        "properties": {
                            "request": {
                                "type": "string",
                                "description": "JSON-encoded AiChatRequest payload.",
                            },
                            "files": {
                                "type": "array",
                                "items": {"type": "string", "format": "binary"},
                            },
                        },
                    }
                },
            },
        },
        "responses": {
            "200": {
                "description": "Server-sent events carrying typed AI relay stream events.",
                "content": {
                    "text/event-stream": {
                        "schema": AI_CHAT_STREAM_EVENT_OPENAPI_SCHEMA,
                    }
                },
            }
        },
    },
)
async def stream_chat(request: Request) -> StreamingResponse:
    auth_context = require_ai_relay_access(request)
    chat_request, attachments = await _read_attachments(request)
    gateway = get_ai_relay_gateway(request)

    try:
        gateway.assert_ready()
    except AiRelayGatewayError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc

    request_id = str(uuid4())
    logger.info(
        "AI relay request accepted: request_id=%s subject=%s model=%s files=%s total_bytes=%s",
        request_id,
        auth_context.subject or "-",
        get_settings(request).ai_relay.model,
        len(attachments),
        sum(item.size_bytes for item in attachments),
    )

    async def event_stream() -> Any:
        sequence_number = 0
        stream = gateway.stream_response(
            request_id=request_id,
            auth_context=auth_context,
            chat_request=chat_request,
            attachments=attachments,
        )
        iterator = stream.__aiter__()
        terminal_event = "aborted"

        try:
            while True:
                if await request.is_disconnected():
                    terminal_event = "client_disconnected"
                    logger.info("AI relay client disconnected: request_id=%s", request_id)
                    break

                try:
                    event = await asyncio.wait_for(iterator.__anext__(), timeout=_HEARTBEAT_SECONDS)
                except asyncio.TimeoutError:
                    yield _sse_comment("heartbeat")
                    continue
                except StopAsyncIteration:
                    break
                except AiRelayGatewayError as exc:
                    sequence_number += 1
                    error_event = exc.to_error_event(sequence_number=sequence_number)
                    terminal_event = "error"
                    yield _sse_event(error_event.event, error_event.model_dump(mode="json"))
                    logger.warning(
                        "AI relay upstream failure: request_id=%s code=%s retryable=%s",
                        request_id,
                        exc.code,
                        exc.retryable,
                    )
                    break
                except Exception:
                    sequence_number += 1
                    logger.exception("AI relay stream crashed: request_id=%s", request_id)
                    error_event = AiRelayGatewayError(
                        status_code=502,
                        code="ai_relay_stream_failed",
                        message="AI relay streaming failed unexpectedly.",
                        retryable=False,
                    ).to_error_event(sequence_number=sequence_number)
                    terminal_event = "error"
                    yield _sse_event(error_event.event, error_event.model_dump(mode="json"))
                    break

                sequence_number = event.sequenceNumber
                terminal_event = event.event if event.event in {"completed", "error"} else terminal_event
                yield _sse_event(event.event, event.model_dump(mode="json"))
        finally:
            await stream.aclose()
            logger.info(
                "AI relay stream closed: request_id=%s terminal_event=%s",
                request_id,
                terminal_event,
            )

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
