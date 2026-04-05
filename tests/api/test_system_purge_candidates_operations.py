import asyncio

import pytest

from api.endpoints import system
from api.service.app import create_app
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_create_purge_candidates_operation_completes_with_result(monkeypatch: pytest.MonkeyPatch) -> None:

    def _fake_collect(*args, **kwargs):
        return (
            [
                {
                    "symbol": "AAA",
                    "matchedValue": 0.91,
                    "rowsContributing": 2,
                    "latestAsOf": "2026-02-01T00:00:00Z",
                }
            ],
            42,
            1,
            2,
        )

    monkeypatch.setattr(system, "_collect_purge_candidates", _fake_collect)

    app = create_app()
    payload = {
        "layer": "silver",
        "domain": "market",
        "column": "Close",
        "operator": "lt",
        "value": 1,
        "aggregation": "avg",
        "recent_rows": 1,
        "offset": 0,
    }

    async with get_test_client(app) as client:
        submit = await client.post("/api/system/purge-candidates", json=payload)
        assert submit.status_code == 202
        operation_id = submit.json().get("operationId")
        assert operation_id

        operation_payload = None
        for _ in range(40):
            status_resp = await client.get(f"/api/system/purge/{operation_id}")
            assert status_resp.status_code == 200
            operation_payload = status_resp.json()
            if operation_payload.get("status") != "running":
                break
            await asyncio.sleep(0.025)

        assert operation_payload is not None
        assert operation_payload.get("status") == "succeeded"
        result = operation_payload.get("result") or {}
        assert result.get("summary", {}).get("totalRowsScanned") == 42
        assert result.get("summary", {}).get("symbolsMatched") == 1
        assert result.get("symbols", [])[0].get("symbol") == "AAA"


@pytest.mark.asyncio
async def test_create_purge_candidates_operation_sets_failed_status_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:

    def _failing_collect(*args, **kwargs):
        raise RuntimeError("preview exploded")

    monkeypatch.setattr(system, "_collect_purge_candidates", _failing_collect)

    app = create_app()
    payload = {
        "layer": "silver",
        "domain": "market",
        "column": "Close",
        "operator": "lt",
        "value": 1,
        "aggregation": "avg",
        "recent_rows": 1,
        "offset": 0,
    }

    async with get_test_client(app) as client:
        submit = await client.post("/api/system/purge-candidates", json=payload)
        assert submit.status_code == 202
        operation_id = submit.json().get("operationId")
        assert operation_id

        operation_payload = None
        for _ in range(40):
            status_resp = await client.get(f"/api/system/purge/{operation_id}")
            assert status_resp.status_code == 200
            operation_payload = status_resp.json()
            if operation_payload.get("status") != "running":
                break
            await asyncio.sleep(0.025)

        assert operation_payload is not None
        assert operation_payload.get("status") == "failed"
        assert "preview exploded" in str(operation_payload.get("error") or "")
