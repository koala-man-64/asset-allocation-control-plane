from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Mapping, Sequence

import anyio
from fastapi import FastAPI


@dataclass
class WebSocketSession:
    _to_app: anyio.abc.ObjectSendStream[dict]
    _from_app: anyio.abc.ObjectReceiveStream[dict]

    async def send_text(self, text: str) -> None:
        await self._to_app.send({"type": "websocket.receive", "text": str(text)})

    async def send_json(self, payload: Any) -> None:
        await self.send_text(json.dumps(payload))

    async def receive_message(self, *, timeout_seconds: float = 2.0) -> dict:
        with anyio.fail_after(timeout_seconds):
            return await self._from_app.receive()

    async def receive_text(self, *, timeout_seconds: float = 2.0) -> str:
        msg = await self.receive_message(timeout_seconds=timeout_seconds)
        if msg.get("type") == "websocket.send" and "text" in msg:
            return str(msg["text"])
        raise AssertionError(f"Expected websocket.send text frame, got: {msg!r}")

    async def receive_json(self, *, timeout_seconds: float = 2.0) -> Any:
        return json.loads(await self.receive_text(timeout_seconds=timeout_seconds))


class WebSocketHandshakeError(AssertionError):
    def __init__(self, message: dict):
        super().__init__(f"WebSocket handshake failed: {message!r}")
        self.message = message


@asynccontextmanager
async def connect_websocket(
    app: FastAPI,
    path: str,
    *,
    timeout_seconds: float = 2.0,
    headers: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
    manage_lifespan: bool = True,
) -> AsyncIterator[WebSocketSession]:
    """
    In-memory ASGI websocket harness.

    Avoids starlette.testclient.TestClient, which is incompatible with the
    sandbox constraints (socket syscalls blocked).
    """
    path_only, _, raw_query = path.partition("?")
    encoded_headers: list[tuple[bytes, bytes]] = []
    if headers is not None:
        items = headers.items() if isinstance(headers, Mapping) else headers
        encoded_headers = [
            (
                str(name).lower().encode("ascii", errors="ignore"),
                str(value).encode("utf-8"),
            )
            for name, value in items
        ]

    send_to_app, recv_by_app = anyio.create_memory_object_stream[dict](100)
    send_by_app, recv_from_app = anyio.create_memory_object_stream[dict](100)

    async def _receive() -> dict:
        return await recv_by_app.receive()

    async def _send(message: dict) -> None:
        await send_by_app.send(message)

    scope = {
        "type": "websocket",
        "asgi": {"spec_version": "2.1", "version": "3.0"},
        "scheme": "ws",
        "path": path_only,
        "raw_path": path_only.encode("ascii", errors="ignore"),
        "query_string": raw_query.encode("ascii", errors="ignore"),
        "headers": encoded_headers,
        "client": ("testclient", 123),
        "server": ("testserver", 80),
        "subprotocols": [],
        "extensions": {},
    }

    @asynccontextmanager
    async def _session_context() -> AsyncIterator[WebSocketSession]:
        handshake_error: WebSocketHandshakeError | None = None
        async with anyio.create_task_group() as tg:
            tg.start_soon(app, scope, _receive, _send)

            # Initiate websocket handshake.
            await send_to_app.send({"type": "websocket.connect"})

            with anyio.fail_after(timeout_seconds):
                first = await recv_from_app.receive()

            if first.get("type") != "websocket.accept":
                handshake_error = WebSocketHandshakeError(first)
                tg.cancel_scope.cancel()
            else:
                session = WebSocketSession(_to_app=send_to_app, _from_app=recv_from_app)
                try:
                    yield session
                finally:
                    # Close politely, then cancel any remaining server task work.
                    try:
                        await send_to_app.send({"type": "websocket.disconnect", "code": 1000})
                    except Exception:
                        pass
                    tg.cancel_scope.cancel()

        if handshake_error is not None:
            raise handshake_error

    if manage_lifespan:
        async with app.router.lifespan_context(app):
            async with _session_context() as session:
                yield session
        return

    async with _session_context() as session:
        yield session
