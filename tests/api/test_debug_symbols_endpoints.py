from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from api.service.app import create_app
from asset_allocation_runtime_common.foundation.debug_symbols import DebugSymbolsState
from tests.api._client import get_test_client


def _state(symbols_raw: str, updated_by: str | None = "tester") -> DebugSymbolsState:
    return DebugSymbolsState(
        symbols_raw=symbols_raw,
        symbols=[token for token in symbols_raw.split(",") if token],
        updated_at=datetime.now(timezone.utc),
        updated_by=updated_by,
    )


@pytest.mark.asyncio
async def test_get_debug_symbols_returns_404_when_absent(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.system.read_debug_symbols_state", return_value=None):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/debug-symbols")

    assert resp.status_code == 404
    assert "not configured" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_debug_symbols_returns_runtime_config_backed_state(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch(
        "api.endpoints.system.read_debug_symbols_state",
        return_value=_state("AAPL,MSFT"),
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/debug-symbols")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbols"] == "AAPL,MSFT"
    assert payload["updatedBy"] == "tester"


@pytest.mark.asyncio
async def test_put_debug_symbols_replaces_runtime_config_backed_state(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch(
        "api.endpoints.system.replace_debug_symbols_state",
        return_value=_state("AAPL,MSFT"),
    ) as update_mock:
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.put(
                "/api/system/debug-symbols",
                json={"symbols": '["aapl", "msft"]'},
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbols"] == "AAPL,MSFT"
    assert update_mock.call_args.kwargs["dsn"] == "postgresql://user:pass@localhost/db"
    assert update_mock.call_args.kwargs["symbols"] == '["aapl", "msft"]'


@pytest.mark.asyncio
async def test_put_debug_symbols_rejects_empty_value(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.put(
            "/api/system/debug-symbols",
            json={"symbols": "   "},
        )

    assert resp.status_code == 400
    assert "required" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_delete_debug_symbols_removes_runtime_config_row(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.system.delete_debug_symbols_state", return_value=True) as delete_mock:
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.delete("/api/system/debug-symbols")

    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}
    assert delete_mock.call_args.kwargs["dsn"] == "postgresql://user:pass@localhost/db"
