from __future__ import annotations

import logging

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


class _Bomb:
    def __getattribute__(self, name: str):
        raise AssertionError(f"shallow health endpoint touched dependency attribute {name!r}")


@pytest.mark.asyncio
async def test_healthz_and_readyz_are_shallow_and_preserve_response_shapes() -> None:
    app = create_app()
    app.state.system_health_cache = _Bomb()
    app.state.alpha_vantage_gateway = _Bomb()
    app.state.massive_gateway = _Bomb()
    app.state.quiver_gateway = _Bomb()

    async with get_test_client(app, manage_lifespan=False) as client:
        health = await client.get("/healthz?apikey=super-secret", headers={"Authorization": "Bearer secret-token"})
        ready = await client.get("/readyz?token=super-secret", headers={"Authorization": "Bearer secret-token"})

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_http_middleware_logs_query_metadata_without_raw_query(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = create_app()

    with caplog.at_level(logging.INFO, logger="asset-allocation.api"):
        async with get_test_client(app, manage_lifespan=False) as client:
            response = await client.get(
                "/healthz?apikey=super-secret&symbol=AAPL",
                headers={
                    "Authorization": "Bearer request-token",
                    "Referer": "https://ui.example/path?access_token=referer-token",
                },
            )

    assert response.status_code == 200

    api_messages = "\n".join(record.getMessage() for record in caplog.records if record.name == "asset-allocation.api")
    assert "query_param_count=2" in api_messages
    assert "query_keys=['apikey', 'symbol']" in api_messages
    assert "query=apikey=super-secret" not in api_messages
    assert "super-secret" not in api_messages
    assert "request-token" not in api_messages
    assert "referer-token" not in api_messages
