from __future__ import annotations

import asyncio

import pytest

from api.service.app import (
    _background_workers_enabled,
    _shutdown_background_task,
    create_app,
)
from tests.api._client import get_test_client


def test_background_workers_default_off_in_test_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_MODE", "true")

    assert _background_workers_enabled() is False


def test_background_workers_always_off_in_test_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_MODE", "true")
    assert _background_workers_enabled() is False


@pytest.mark.asyncio
async def test_shutdown_background_task_suppresses_cancelled_error() -> None:
    stop_event = asyncio.Event()

    async def _block_forever() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(_block_forever())
    await _shutdown_background_task(
        task,
        stop_event=stop_event,
        task_name="unit-test-task",
        graceful_timeout_seconds=0.01,
    )

    assert task.done()


@pytest.mark.asyncio
async def test_app_lifespan_with_postgres_dsn_defaults_workers_off_in_tests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/health")

    assert response.status_code == 200
