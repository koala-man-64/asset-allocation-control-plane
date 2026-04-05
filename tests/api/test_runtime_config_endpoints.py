from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from api.service.app import create_app
from core.runtime_config import RuntimeConfigItem
from tests.api._client import get_test_client


def _item(*, key: str, value: str, scope: str = "global") -> RuntimeConfigItem:
    return RuntimeConfigItem(
        scope=scope,
        key=key,
        value=value,
        description="desc",
        updated_at=datetime.now(timezone.utc),
        updated_by="tester",
    )


@pytest.mark.asyncio
async def test_runtime_config_catalog(monkeypatch):
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/runtime-config/catalog")
    assert resp.status_code == 200
    payload = resp.json()
    assert "items" in payload
    keys = [item.get("key") for item in payload["items"]]
    assert "DEBUG_SYMBOLS" in keys
    assert "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS" in keys
    assert "SILVER_LATEST_ONLY" not in keys
    assert "SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED" not in keys
    assert "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID" not in keys


@pytest.mark.asyncio
async def test_get_runtime_config_requires_postgres(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/runtime-config?scope=global")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_get_runtime_config_returns_items(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    rows = [_item(key="DEBUG_SYMBOLS", value="AAPL,MSFT")]
    with patch("api.endpoints.system.list_runtime_config", return_value=rows):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/runtime-config?scope=global")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["scope"] == "global"
    assert payload["items"][0]["key"] == "DEBUG_SYMBOLS"
    assert payload["items"][0]["value"] == "AAPL,MSFT"


@pytest.mark.asyncio
async def test_set_runtime_config_rejects_forbidden_key(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.post(
            "/api/system/runtime-config",
            json={"key": "POSTGRES_DSN", "value": "nope"},
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_runtime_config_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.post(
            "/api/system/runtime-config",
            json={"key": "SYSTEM_HEALTH_TTL_SECONDS", "value": ""},
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_runtime_config_normalizes_value_before_upsert(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    def _fake_upsert(**kwargs):
        assert kwargs["value"] == "3"
        return _item(key=kwargs["key"], value=kwargs["value"], scope=kwargs["scope"])

    with patch("api.endpoints.system.upsert_runtime_config", side_effect=_fake_upsert):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post(
                "/api/system/runtime-config",
                json={
                    "key": "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS",
                    "scope": "global",
                    "value": " 3 ",
                },
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["key"] == "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS"
    assert payload["value"] == "3"


@pytest.mark.asyncio
async def test_set_runtime_config_normalizes_debug_symbols_before_upsert(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    def _fake_upsert(**kwargs):
        assert kwargs["key"] == "DEBUG_SYMBOLS"
        assert kwargs["value"] == "AAPL,MSFT,NVDA"
        return _item(key=kwargs["key"], value=kwargs["value"], scope=kwargs["scope"])

    with patch("api.endpoints.system.upsert_runtime_config", side_effect=_fake_upsert):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post(
                "/api/system/runtime-config",
                json={
                    "key": "DEBUG_SYMBOLS",
                    "scope": "global",
                    "value": '["aapl", "msft", "nvda"]',
                },
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["key"] == "DEBUG_SYMBOLS"
    assert payload["value"] == "AAPL,MSFT,NVDA"
