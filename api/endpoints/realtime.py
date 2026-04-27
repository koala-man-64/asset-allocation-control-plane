from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from api.service.dependencies import get_websocket_ticket_store, validate_auth
from api.service.log_streaming import LogStreamManager
from api.service.realtime import manager as realtime_manager
from api.service.realtime_tickets import to_utc_timestamp


logger = logging.getLogger("asset-allocation.api.realtime")

router = APIRouter()

WEBSOCKET_UNAUTHORIZED_CLOSE_CODE = 4401


@router.post("/realtime/ticket")
def issue_realtime_ticket(request: Request) -> JSONResponse:
    auth_context = validate_auth(request)
    store = get_websocket_ticket_store(request)
    ticket = store.issue(subject=auth_context.subject, auth_mode=auth_context.mode)
    logger.info(
        "realtime_ticket_issued: request_id=%s subject=%s mode=%s session_id=%s expires_at=%s",
        str(getattr(request.state, "request_id", "") or request.headers.get("x-request-id", "") or "-"),
        auth_context.subject or "-",
        auth_context.mode,
        auth_context.session_id or "-",
        to_utc_timestamp(ticket.expires_at),
    )
    return JSONResponse(
        {
            "ticket": ticket.ticket,
            "expiresAt": to_utc_timestamp(ticket.expires_at),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.websocket("/ws/updates")
async def websocket_updates(websocket: WebSocket) -> None:
    ticket = str(websocket.query_params.get("ticket") or "").strip()
    store = get_websocket_ticket_store(websocket)
    if not store.consume(ticket):
        logger.warning("realtime_ticket_rejected: reason=missing_or_invalid_or_reused client=%s", websocket.client)
        await websocket.close(code=WEBSOCKET_UNAUTHORIZED_CLOSE_CODE, reason="Unauthorized")
        return

    log_stream_manager: LogStreamManager = websocket.app.state.log_stream_manager
    await realtime_manager.connect(websocket)
    try:
        while True:
            data_str = await websocket.receive_text()

            if data_str == "ping":
                await websocket.send_text("pong")
                continue

            try:
                msg = json.loads(data_str)
                action = msg.get("action")
                topics = msg.get("topics", [])

                if not isinstance(topics, list):
                    continue

                if action == "subscribe":
                    await realtime_manager.subscribe(websocket, topics)
                    await log_stream_manager.ensure_streams(topics)
                elif action == "unsubscribe":
                    await realtime_manager.unsubscribe(websocket, topics)
                    await log_stream_manager.prune_unused_streams(topics)
            except json.JSONDecodeError:
                continue
    except WebSocketDisconnect:
        pass
    finally:
        realtime_manager.disconnect(websocket)
        await log_stream_manager.prune_unused_streams()
