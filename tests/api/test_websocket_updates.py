from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import anyio
import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from api.service.realtime_tickets import utc_now
from tests.api._auth import install_auth_stub
from tests.api._client import get_test_client
from tests.api._password_auth import password_verifier_for
from tests.api._websocket import WebSocketHandshakeError, connect_websocket

TEST_ORIGIN = "http://test"


def _set_required_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_CSP", "default-src 'self'; base-uri 'none'; frame-ancestors 'none'")

    monkeypatch.delenv("UI_DIST_DIR", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)


async def _issue_realtime_ticket(client) -> str:
    response = await client.post("/api/realtime/ticket")
    assert response.status_code == 200
    return str(response.json()["ticket"])


@pytest.mark.asyncio
async def test_websocket_updates_endpoint_accepts_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    app = create_app()
    async with app.router.lifespan_context(app):
        async with get_test_client(app, manage_lifespan=False) as client:
            ticket = await _issue_realtime_ticket(client)

        async with connect_websocket(app, f"/api/ws/updates?ticket={ticket}", manage_lifespan=False) as websocket:
            await websocket.send_text("ping")
            assert await websocket.receive_text() == "pong"

@pytest.mark.asyncio
async def test_websocket_pubsub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    
    # Mock the manager used by app
    from api.service.app import realtime_manager
    import json
    
    app = create_app()
    
    # Ensure a clean manager across tests.
    realtime_manager.active_connections.clear()
    realtime_manager.subscriptions.clear()
    async with app.router.lifespan_context(app):
        async with get_test_client(app, manage_lifespan=False) as client:
            ticket = await _issue_realtime_ticket(client)

        async with connect_websocket(app, f"/api/ws/updates?ticket={ticket}", manage_lifespan=False) as ws:
            # 1. Subscribe to "test-topic"
            await ws.send_text(json.dumps({"action": "subscribe", "topics": ["test-topic"]}))

            # Give the server loop a chance to process the subscribe message.
            with anyio.fail_after(2):
                while len(realtime_manager.subscriptions.get("test-topic", set())) < 1:
                    await anyio.sleep(0)

            # 2. Broadcast to "test-topic"
            await realtime_manager.broadcast("test-topic", {"status": "ok"})

            # 3. Verify receipt
            msg = await ws.receive_json()
            assert msg["topic"] == "test-topic"
            assert msg["data"]["status"] == "ok"

            # 4. Broadcast to other topic
            await realtime_manager.broadcast("other-topic", {"status": "ignored"})

            # 5. Verify connection stays alive (and no unexpected queued messages)
            await ws.send_text("ping")
            assert await ws.receive_text() == "pong"


class _FakeStreamingLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_FakeStreamingLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "executionName", "type": "string"},
                        {"name": "stream_s", "type": "string"},
                        {"name": "msg", "type": "string"},
                    ],
                    "rows": [
                        [
                            "2026-02-10T00:00:00Z",
                            "bronze-market-job-exec-001",
                            "stderr",
                            "stream log line",
                        ],
                    ],
                }
            ]
        }


@pytest.mark.asyncio
async def test_websocket_job_log_stream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace-id")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "bronze-market-job")
    monkeypatch.setenv("REALTIME_LOG_STREAM_POLL_SECONDS", "1")
    monkeypatch.setenv("REALTIME_LOG_STREAM_LOOKBACK_SECONDS", "30")
    monkeypatch.setenv("REALTIME_LOG_STREAM_BATCH_SIZE", "20")

    from api.service.app import realtime_manager

    fake_logs = _FakeStreamingLogAnalyticsClient()
    with patch("api.service.log_streaming.AzureLogAnalyticsClient", return_value=fake_logs):
        app = create_app()
        realtime_manager.active_connections.clear()
        realtime_manager.subscriptions.clear()
        async with app.router.lifespan_context(app):
            async with get_test_client(app, manage_lifespan=False) as client:
                ticket = await _issue_realtime_ticket(client)

            async with connect_websocket(
                app,
                f"/api/ws/updates?ticket={ticket}",
                manage_lifespan=False,
            ) as ws:
                await ws.send_text(
                    json.dumps(
                        {
                            "action": "subscribe",
                            "topics": [
                                "job-logs:bronze-market-job/executions/bronze-market-job-exec-001"
                            ],
                        }
                    )
                )

                with anyio.fail_after(2):
                    msg = await ws.receive_json()

    assert msg["topic"] == "job-logs:bronze-market-job/executions/bronze-market-job-exec-001"
    assert msg["data"]["type"] == "CONSOLE_LOG_STREAM"
    payload = msg["data"]["payload"]
    assert payload["resourceType"] == "job"
    assert payload["resourceName"] == "bronze-market-job"
    assert payload["lines"][0]["message"] == "stream log line"
    assert payload["lines"][0]["timestamp"] == "2026-02-10T00:00:00Z"
    assert payload["lines"][0]["executionName"] == "bronze-market-job-exec-001"
    assert payload["lines"][0]["stream_s"] == "stderr"
    assert fake_logs.queries
    assert "let execFilter = 'bronze-market-job-exec-001';" in fake_logs.queries[0][1]


@pytest.mark.asyncio
async def test_websocket_ticket_required_and_single_use_for_oidc_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_required_env(tmp_path, monkeypatch)
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")

    app = create_app()
    async with app.router.lifespan_context(app):
        async with get_test_client(app, manage_lifespan=False) as client:
            unauthenticated = await client.post("/api/realtime/ticket")
            assert unauthenticated.status_code == 401

            install_auth_stub(
                monkeypatch,
                app.state.auth,
                auth_context=AuthContext(mode="oidc", subject="user-123", claims={"sub": "user-123"}),
            )
            authenticated = await client.post("/api/realtime/ticket", headers={"Authorization": "Bearer token"})
            assert authenticated.status_code == 200
            payload = authenticated.json()
            assert set(payload) == {"ticket", "expiresAt"}
            ticket = payload["ticket"]

        with pytest.raises(WebSocketHandshakeError) as missing_ticket:
            async with connect_websocket(app, "/api/ws/updates", manage_lifespan=False):
                pass
        assert missing_ticket.value.message["type"] == "websocket.close"
        assert missing_ticket.value.message["code"] == 4401

        async with connect_websocket(
            app,
            f"/api/ws/updates?ticket={ticket}",
            manage_lifespan=False,
        ) as websocket:
            await websocket.send_text("ping")
            assert await websocket.receive_text() == "pong"

        with pytest.raises(WebSocketHandshakeError) as replayed_ticket:
            async with connect_websocket(
                app,
                f"/api/ws/updates?ticket={ticket}",
                manage_lifespan=False,
            ):
                pass
        assert replayed_ticket.value.message["code"] == 4401


@pytest.mark.asyncio
async def test_websocket_rejects_invalid_and_expired_tickets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_required_env(tmp_path, monkeypatch)
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")

    app = create_app()

    async with app.router.lifespan_context(app):
        install_auth_stub(
            monkeypatch,
            app.state.auth,
            auth_context=AuthContext(mode="oidc", subject="user-123", claims={"sub": "user-123"}),
        )
        with pytest.raises(WebSocketHandshakeError) as invalid_ticket:
            async with connect_websocket(
                app,
                "/api/ws/updates?ticket=invalid-ticket",
                manage_lifespan=False,
            ):
                pass
        assert invalid_ticket.value.message["code"] == 4401

        async with get_test_client(app, manage_lifespan=False) as client:
            ticket_response = await client.post(
                "/api/realtime/ticket",
                headers={"Authorization": "Bearer token"},
            )
            assert ticket_response.status_code == 200
            expired_ticket = ticket_response.json()["ticket"]

        record = app.state.websocket_ticket_store._tickets[expired_ticket]
        app.state.websocket_ticket_store._tickets[expired_ticket] = replace(
            record,
            expires_at=utc_now(),
        )

        with pytest.raises(WebSocketHandshakeError) as expired:
            async with connect_websocket(
                app,
                f"/api/ws/updates?ticket={expired_ticket}",
                manage_lifespan=False,
            ):
                pass
        assert expired.value.message["code"] == 4401


@pytest.mark.asyncio
async def test_websocket_ticket_supports_oidc_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_required_env(tmp_path, monkeypatch)
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation")

    app = create_app()
    async with app.router.lifespan_context(app):
        install_auth_stub(
            monkeypatch,
            app.state.auth,
            auth_context=AuthContext(mode="oidc", subject="user-123", claims={"sub": "user-123"}),
        )

        async with get_test_client(app, manage_lifespan=False) as client:
            response = await client.post("/api/realtime/ticket", headers={"Authorization": "Bearer token"})
            assert response.status_code == 200
            ticket = response.json()["ticket"]

        async with connect_websocket(
            app,
            f"/api/ws/updates?ticket={ticket}",
            manage_lifespan=False,
        ) as websocket:
            await websocket.send_text("ping")
            assert await websocket.receive_text() == "pong"


@pytest.mark.asyncio
async def test_websocket_ticket_supports_password_cookie_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_required_env(tmp_path, monkeypatch)
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "cookie")
    monkeypatch.setenv("API_AUTH_SESSION_SECRET_KEYS", "test-session-secret-key-value-at-least-32-chars")
    monkeypatch.setenv("UI_AUTH_PROVIDER", "password")
    monkeypatch.setenv("UI_SHARED_PASSWORD_HASH", password_verifier_for("operator-secret"))

    app = create_app()
    async with app.router.lifespan_context(app):
        async with get_test_client(app, manage_lifespan=False) as client:
            login = await client.post(
                "/api/auth/session",
                json={"password": "operator-secret"},
                headers={"Origin": TEST_ORIGIN},
            )
            assert login.status_code == 200
            csrf_token = login.cookies.get("aa_csrf_dev")
            response = await client.post(
                "/api/realtime/ticket",
                headers={"Origin": TEST_ORIGIN, "X-CSRF-Token": csrf_token or ""},
            )
            assert response.status_code == 200
            ticket = response.json()["ticket"]

        async with connect_websocket(
            app,
            f"/api/ws/updates?ticket={ticket}",
            manage_lifespan=False,
        ) as websocket:
            await websocket.send_text("ping")
            assert await websocket.receive_text() == "pong"
