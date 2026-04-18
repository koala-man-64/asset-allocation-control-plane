from __future__ import annotations

import asyncio

from fastapi import Request

from api.endpoints import internal as internal_routes
from api.service.auth import AuthContext
from api.service.app import create_app
from asset_allocation_contracts.backtest import BacktestReconcileResponse


def test_internal_backtest_reconcile_returns_contract_payload(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        internal_routes,
        "reconcile_backtest_runs",
        lambda dsn: BacktestReconcileResponse(
            dispatchedCount=1,
            dispatchFailedCount=0,
            failedStaleRunningCount=0,
            skippedActiveCount=2,
            noActionCount=0,
            dispatchedRunIds=["run-1"],
            dispatchFailedRunIds=[],
            failedRunIds=[],
        ),
    )

    async def _run() -> dict[str, object]:
        app = create_app()
        app.state.auth = type(
            "_Auth",
            (),
            {"authenticate_headers": staticmethod(lambda _headers: AuthContext(mode="anonymous", subject=None, claims={}))},
        )()
        app.state.settings = type(
            "_Settings",
            (),
            {"postgres_dsn": "postgresql://test:test@localhost:5432/asset_allocation"},
        )()
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/internal/backtests/runs/reconcile",
                "headers": [],
                "app": app,
            }
        )
        payload = await internal_routes.reconcile_backtest_run_queue(request)
        return payload.model_dump() if hasattr(payload, "model_dump") else dict(payload)

    payload = asyncio.run(_run())
    assert payload["dispatchedCount"] == 1
    assert payload["skippedActiveCount"] == 2
    assert payload["dispatchedRunIds"] == ["run-1"]
