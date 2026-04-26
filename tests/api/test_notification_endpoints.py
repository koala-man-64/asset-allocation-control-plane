from __future__ import annotations

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


async def test_notification_action_approve_requires_post(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/notifications/actions/token-1/approve")

    assert response.status_code == 405


async def test_notification_action_route_rejects_mismatched_decision_before_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/notifications/actions/token-1/approve",
            json={"decision": "deny", "reason": "wrong route"},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Decision payload must be 'approve'."
