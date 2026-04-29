from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from psycopg import OperationalError

from api.endpoints import backtests as backtest_endpoints
from api.service.backtest_contracts_compat import StrategyReferenceInput
from api.service.app import create_app
from core.backtest_request_resolution import ResolvedBacktestRequest
from core.backtest_repository import BacktestRepository
from core.backtest_runtime import ResolvedBacktestDefinition
from core.ranking_engine.contracts import RankingSchemaConfig
from core.strategy_engine.contracts import StrategyConfig, UniverseDefinition
from tests.api._client import get_test_client


def _sample_universe() -> UniverseDefinition:
    return UniverseDefinition.model_validate(
        {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {
                        "kind": "condition",
                        "field": "market.close",
                        "operator": "gt",
                        "value": 1,
                    }
                ],
            },
        }
    )


def _sample_definition() -> ResolvedBacktestDefinition:
    universe = _sample_universe()
    return ResolvedBacktestDefinition(
        strategy_name="mom-spy-res",
        strategy_version=3,
        strategy_config=StrategyConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "rebalance": "weekly",
                "longOnly": True,
                "topN": 2,
                "lookbackWindow": 20,
                "holdingPeriod": 5,
                "costModel": "default",
                "rankingSchemaName": "quality",
                "intrabarConflictPolicy": "stop_first",
                "regimePolicy": {
                    "modelName": "default-regime",
                    "mode": "observe_only",
                },
                "exits": [],
            }
        ),
        strategy_config_raw={
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 2,
            "lookbackWindow": 20,
            "holdingPeriod": 5,
            "costModel": "default",
            "rankingSchemaName": "quality",
            "intrabarConflictPolicy": "stop_first",
            "regimePolicy": {
                "modelName": "default-regime",
                "mode": "observe_only",
            },
            "exits": [],
        },
        strategy_universe=universe,
        ranking_schema_name="quality",
        ranking_schema_version=7,
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "f1",
                                "table": "market_data",
                                "column": "return_20d",
                                "weight": 1,
                                "direction": "desc",
                                "missingValuePolicy": "exclude",
                                "transforms": [],
                            }
                        ],
                        "transforms": [],
                    }
                ],
                "overallTransforms": [],
            }
        ),
        ranking_universe_name="large-cap-quality",
        ranking_universe_version=5,
        ranking_universe=universe,
        regime_model_name="default-regime",
        regime_model_version=1,
        regime_model_config={"highVolEnterThreshold": 28.0},
    )


def _sample_resolved_request(*, input_mode: str = "strategy_ref") -> ResolvedBacktestRequest:
    definition = _sample_definition()
    strategy_ref = (
        StrategyReferenceInput(strategyName=definition.strategy_name or "mom-spy-res", strategyVersion=definition.strategy_version)
        if input_mode == "strategy_ref"
        else None
    )
    return ResolvedBacktestRequest(
        input_mode=input_mode,
        start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        bar_size="5m",
        schedule=[
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
        definition=definition,
        strategy_ref=strategy_ref,
        request_payload={
            "strategyRef": strategy_ref.model_dump(mode="json") if strategy_ref is not None else None,
            "strategyConfig": definition.strategy_config_raw if input_mode == "inline" else None,
            "startTs": "2026-03-03T14:30:00+00:00",
            "endTs": "2026-03-03T14:35:00+00:00",
            "barSize": "5m",
        },
        effective_config={
            "schemaVersion": 1,
            "inputMode": input_mode,
            "strategy": definition.strategy_config_raw,
            "pins": {
                "strategyName": definition.strategy_name,
                "strategyVersion": definition.strategy_version,
                "rankingSchemaName": definition.ranking_schema_name,
                "rankingSchemaVersion": definition.ranking_schema_version,
                "universeName": definition.ranking_universe_name,
                "universeVersion": definition.ranking_universe_version,
                "regimeModelName": definition.regime_model_name,
                "regimeModelVersion": definition.regime_model_version,
            },
            "execution": {
                "startTs": "2026-03-03T14:30:00+00:00",
                "endTs": "2026-03-03T14:35:00+00:00",
                "barSize": "5m",
                "barsResolved": 2,
            },
            "fingerprints": {
                "configFingerprint": "config-fp-1",
                "requestFingerprint": "request-fp-1",
                "resultsSchemaVersion": 4,
            },
        },
        config_fingerprint="config-fp-1",
        request_fingerprint="request-fp-1",
    )


@pytest.mark.asyncio
async def test_list_backtests_returns_repo_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "list_runs",
        lambda self, **kwargs: [
            {
                "run_id": "run-1",
                "status": "queued",
                "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
                "started_at": None,
                "completed_at": None,
                "run_name": "Smoke",
                "start_date": "2026-03-01",
                "end_date": "2026-03-08",
                "error": None,
                "strategy_name": "quality-trend",
                "strategy_version": 4,
                "bar_size": "5m",
                "execution_name": "backtest-exec-01",
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests?limit=10&offset=0")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runs"][0]["run_id"] == "run-1"
    assert payload["runs"][0]["strategy_name"] == "quality-trend"
    assert payload["runs"][0]["strategy_version"] == 4
    assert payload["runs"][0]["bar_size"] == "5m"
    assert payload["runs"][0]["execution_name"] == "backtest-exec-01"
    assert "output_dir" not in payload["runs"][0]
    assert payload["limit"] == 10


@pytest.mark.asyncio
async def test_get_backtest_status_returns_frozen_pin_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "started_at": datetime(2026, 3, 8, 0, 10, tzinfo=timezone.utc),
            "completed_at": datetime(2026, 3, 8, 0, 20, tzinfo=timezone.utc),
            "run_name": "Intraday smoke",
            "start_date": "2026-03-01",
            "end_date": "2026-03-08",
            "error": None,
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "execution_name": "backtest-exec-01",
            "results_ready_at": "2026-03-08T00:25:00+00:00",
            "results_schema_version": 4,
            "effective_config": {
                "pins": {
                    "strategyName": "quality-trend",
                    "strategyVersion": 4,
                    "rankingSchemaName": "quality-momentum",
                    "rankingSchemaVersion": 7,
                    "universeName": "large-cap-quality",
                    "universeVersion": 5,
                    "regimeModelName": "default-regime",
                    "regimeModelVersion": 1,
                }
            },
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["strategy_name"] == "quality-trend"
    assert payload["strategy_version"] == 4
    assert payload["bar_size"] == "5m"
    assert payload["results_schema_version"] == 4
    assert payload["pins"]["rankingSchemaVersion"] == 7
    assert payload["pins"]["regimeModelVersion"] == 1


@pytest.mark.asyncio
async def test_submit_backtest_freezes_pinned_versions_and_queues_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("BACKTEST_ACA_JOB_NAME", "backtests-job")

    captured: dict[str, object] = {}

    def fake_create_run(self, **kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {
            "run_id": "run-1",
            "status": "queued",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "started_at": None,
            "completed_at": None,
            "run_name": kwargs.get("run_name"),
            "start_date": "2026-03-01",
            "end_date": "2026-03-08",
            "error": None,
            "strategy_name": kwargs.get("strategy_name"),
            "strategy_version": kwargs.get("strategy_version"),
            "bar_size": kwargs.get("bar_size"),
            "execution_name": None,
        }

    monkeypatch.setattr(BacktestRepository, "create_run", fake_create_run)
    monkeypatch.setattr(
        backtest_endpoints,
        "resolve_backtest_request",
        lambda *args, **kwargs: _sample_resolved_request(),
    )
    monkeypatch.setattr(
        backtest_endpoints,
        "_trigger_backtest_job",
        lambda job_name: {"status": "queued", "executionName": None, "jobName": job_name},
    )

    app = create_app()
    payload = {
        "strategyName": "mom-spy-res",
        "strategyVersion": 3,
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
        "runName": "Intraday smoke",
    }
    async with get_test_client(app) as client:
        response = await client.post("/api/backtests/", json=payload)

    assert response.status_code == 200
    assert captured["strategy_name"] == "mom-spy-res"
    assert captured["strategy_version"] == 3
    assert captured["ranking_schema_name"] == "quality"
    assert captured["ranking_schema_version"] == 7
    assert captured["universe_name"] == "large-cap-quality"
    assert captured["universe_version"] == 5
    assert captured["regime_model_name"] == "default-regime"
    assert captured["regime_model_version"] == 1
    effective_config = captured["effective_config"]
    assert isinstance(effective_config, dict)
    assert effective_config["pins"]["rankingSchemaVersion"] == 7
    assert effective_config["pins"]["regimeModelName"] == "default-regime"
    assert effective_config["pins"]["regimeModelVersion"] == 1
    assert effective_config["execution"]["barsResolved"] == 2
    assert captured["config_fingerprint"] == "config-fp-1"
    assert captured["request_fingerprint"] == "request-fp-1"
    assert "output_dir" not in captured
    assert "adls_container" not in captured
    assert "adls_prefix" not in captured


@pytest.mark.asyncio
async def test_lookup_backtest_returns_completed_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(backtest_endpoints, "resolve_backtest_request", lambda *args, **kwargs: _sample_resolved_request())
    monkeypatch.setattr(
        BacktestRepository,
        "find_latest_completed_request_run",
        lambda self, *, request_fingerprint: {
            "run_id": "run-lookup-1",
            "status": "completed",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "completed_at": datetime(2026, 3, 8, 0, 30, tzinfo=timezone.utc),
            "results_ready_at": datetime(2026, 3, 8, 0, 35, tzinfo=timezone.utc),
            "results_schema_version": 4,
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": request_fingerprint,
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
    )
    monkeypatch.setattr(BacktestRepository, "find_latest_inflight_request_run", lambda self, *, request_fingerprint: None)
    monkeypatch.setattr(BacktestRepository, "find_latest_failed_request_run", lambda self, *, request_fingerprint: None)
    monkeypatch.setattr(
        BacktestRepository,
        "get_summary",
        lambda self, run_id: {"run_id": run_id, "total_return": 0.12, "trades": 5},
    )

    app = create_app()
    payload = {
        "strategyRef": {"strategyName": "quality-trend", "strategyVersion": 4},
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
    }
    async with get_test_client(app) as client:
        response = await client.post("/api/backtests/results/lookup", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["found"] is True
    assert body["state"] == "completed"
    assert body["run"]["run_id"] == "run-lookup-1"
    assert body["result"]["total_return"] == 0.12
    assert body["links"]["summaryUrl"] == "/api/backtests/run-lookup-1/summary"


@pytest.mark.asyncio
async def test_lookup_backtest_returns_not_run_when_no_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(backtest_endpoints, "resolve_backtest_request", lambda *args, **kwargs: _sample_resolved_request())
    monkeypatch.setattr(BacktestRepository, "find_latest_completed_request_run", lambda self, *, request_fingerprint: None)
    monkeypatch.setattr(BacktestRepository, "find_latest_inflight_request_run", lambda self, *, request_fingerprint: None)
    monkeypatch.setattr(BacktestRepository, "find_latest_failed_request_run", lambda self, *, request_fingerprint: None)

    app = create_app()
    payload = {
        "strategyRef": {"strategyName": "quality-trend"},
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
    }
    async with get_test_client(app) as client:
        response = await client.post("/api/backtests/results/lookup", json=payload)

    assert response.status_code == 200
    assert response.json() == {
        "found": False,
        "state": "not_run",
        "run": None,
        "result": None,
        "links": None,
    }


@pytest.mark.asyncio
async def test_lookup_backtest_returns_inflight_state_without_creating_work(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(backtest_endpoints, "resolve_backtest_request", lambda *args, **kwargs: _sample_resolved_request())
    monkeypatch.setattr(BacktestRepository, "find_latest_completed_request_run", lambda self, *, request_fingerprint: None)
    monkeypatch.setattr(
        BacktestRepository,
        "find_latest_inflight_request_run",
        lambda self, *, request_fingerprint: {
            "run_id": "run-inflight-1",
            "status": "running",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "started_at": datetime(2026, 3, 8, 0, 5, tzinfo=timezone.utc),
            "results_ready_at": None,
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": request_fingerprint,
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
    )
    monkeypatch.setattr(BacktestRepository, "find_latest_failed_request_run", lambda self, *, request_fingerprint: None)

    app = create_app()
    payload = {
        "strategyRef": {"strategyName": "quality-trend"},
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
    }
    async with get_test_client(app) as client:
        response = await client.post("/api/backtests/results/lookup", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["found"] is False
    assert body["state"] == "running"
    assert body["run"]["run_id"] == "run-inflight-1"
    assert body["result"] is None
    assert body["links"] is None


@pytest.mark.asyncio
async def test_run_backtest_creates_new_run_even_when_completed_match_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("BACKTEST_ACA_JOB_NAME", "backtests-job")
    captured: dict[str, Any] = {}

    def fake_create_run(self, **kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {
            "run_id": "run-created-1",
            "status": "queued",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "strategy_name": kwargs.get("strategy_name"),
            "strategy_version": kwargs.get("strategy_version"),
            "bar_size": kwargs.get("bar_size"),
            "request_fingerprint": kwargs.get("request_fingerprint"),
            "effective_config": kwargs.get("effective_config"),
        }

    monkeypatch.setattr(backtest_endpoints, "resolve_backtest_request", lambda *args, **kwargs: _sample_resolved_request())
    monkeypatch.setattr(BacktestRepository, "find_latest_inflight_request_run", lambda self, *, request_fingerprint: None)
    monkeypatch.setattr(BacktestRepository, "create_run", fake_create_run)
    monkeypatch.setattr(backtest_endpoints, "_trigger_backtest_job", lambda job_name: {"status": "queued", "jobName": job_name})

    app = create_app()
    payload = {
        "strategyRef": {"strategyName": "quality-trend"},
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
    }
    async with get_test_client(app) as client:
        response = await client.post("/api/backtests/runs", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["created"] is True
    assert body["reusedInflight"] is False
    assert body["streamUrl"] == "/api/backtests/run-created-1/events"
    assert captured["request_fingerprint"] == "request-fp-1"
    assert captured["config_fingerprint"] == "config-fp-1"


@pytest.mark.asyncio
async def test_run_backtest_reuses_inflight_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(backtest_endpoints, "resolve_backtest_request", lambda *args, **kwargs: _sample_resolved_request())
    monkeypatch.setattr(
        BacktestRepository,
        "find_latest_inflight_request_run",
        lambda self, *, request_fingerprint: {
            "run_id": "run-existing-1",
            "status": "queued",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": request_fingerprint,
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
    )

    app = create_app()
    payload = {
        "strategyRef": {"strategyName": "quality-trend"},
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
    }
    async with get_test_client(app) as client:
        response = await client.post("/api/backtests/runs", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["created"] is False
    assert body["reusedInflight"] is True
    assert body["run"]["run_id"] == "run-existing-1"
    assert body["streamUrl"] == "/api/backtests/run-existing-1/events"


@pytest.mark.asyncio
async def test_split_backtest_endpoints_reject_invalid_strategy_input(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    app = create_app()
    invalid_payload = {
        "strategyRef": {"strategyName": "quality-trend"},
        "strategyConfig": _sample_definition().strategy_config_raw,
        "startTs": "2026-03-03T14:30:00Z",
        "endTs": "2026-03-03T14:35:00Z",
        "barSize": "5m",
    }

    async with get_test_client(app) as client:
        lookup_response = await client.post("/api/backtests/results/lookup", json=invalid_payload)
        run_response = await client.post("/api/backtests/runs", json=invalid_payload)

    assert lookup_response.status_code == 422
    assert run_response.status_code == 422


@pytest.mark.asyncio
async def test_stream_backtest_events_emits_accepted_heartbeat_and_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    runs = [
        {
            "run_id": "run-stream-1",
            "status": "queued",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": "request-fp-1",
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
        {
            "run_id": "run-stream-1",
            "status": "running",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "started_at": datetime(2026, 3, 8, 0, 1, tzinfo=timezone.utc),
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": "request-fp-1",
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
        {
            "run_id": "run-stream-1",
            "status": "running",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "started_at": datetime(2026, 3, 8, 0, 1, tzinfo=timezone.utc),
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": "request-fp-1",
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
        {
            "run_id": "run-stream-1",
            "status": "completed",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "started_at": datetime(2026, 3, 8, 0, 1, tzinfo=timezone.utc),
            "completed_at": datetime(2026, 3, 8, 0, 2, tzinfo=timezone.utc),
            "results_ready_at": datetime(2026, 3, 8, 0, 3, tzinfo=timezone.utc),
            "results_schema_version": 4,
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": "request-fp-1",
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
    ]

    def fake_get_run(self, run_id):  # type: ignore[no-untyped-def]
        if len(runs) > 1:
            return runs.pop(0)
        return runs[0]

    monkeypatch.setattr(BacktestRepository, "get_run", fake_get_run)
    monkeypatch.setattr(BacktestRepository, "get_summary", lambda self, run_id: {"run_id": run_id, "total_return": 0.12})

    app = create_app()
    async with get_test_client(app) as client:
        async with client.stream("GET", "/api/backtests/run-stream-1/events") as response:
            chunks: list[str] = []
            async for text in response.aiter_text():
                chunks.append(text)
        body = "".join(chunks)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert "event: accepted" in body
    assert "event: status" in body
    assert "event: heartbeat" in body
    assert "event: completed" in body
    assert '"summaryUrl":"/api/backtests/run-stream-1/summary"' in body


@pytest.mark.asyncio
async def test_stream_backtest_events_emits_failed_terminal_event(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    runs = [
        {
            "run_id": "run-stream-2",
            "status": "queued",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": "request-fp-2",
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
        {
            "run_id": "run-stream-2",
            "status": "failed",
            "submitted_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "completed_at": datetime(2026, 3, 8, 0, 2, tzinfo=timezone.utc),
            "error": "boom",
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "bar_size": "5m",
            "request_fingerprint": "request-fp-2",
            "effective_config": {"pins": _sample_resolved_request().effective_config["pins"]},
        },
    ]

    def fake_get_run(self, run_id):  # type: ignore[no-untyped-def]
        if len(runs) > 1:
            return runs.pop(0)
        return runs[0]

    monkeypatch.setattr(BacktestRepository, "get_run", fake_get_run)

    app = create_app()
    async with get_test_client(app) as client:
        async with client.stream("GET", "/api/backtests/run-stream-2/events") as response:
            chunks: list[str] = []
            async for text in response.aiter_text():
                chunks.append(text)
        body = "".join(chunks)

    assert response.status_code == 200
    assert "event: accepted" in body
    assert "event: failed" in body
    assert '"error":"boom"' in body


@pytest.mark.asyncio
async def test_get_summary_returns_postgres_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
            "bar_size": "5m",
            "results_schema_version": 4,
        },
    )
    monkeypatch.setattr(
        BacktestRepository,
        "get_summary",
        lambda self, run_id: {
            "run_id": run_id,
            "run_name": "Intraday smoke",
            "total_return": 0.12,
            "annualized_return": 0.5,
            "annualized_volatility": 0.2,
            "sharpe_ratio": 2.5,
            "max_drawdown": -0.08,
            "trades": 12,
            "initial_cash": 100000.0,
            "final_equity": 112000.0,
            "gross_total_return": 0.123,
            "gross_annualized_return": 0.51,
            "total_commission": 30.0,
            "total_slippage_cost": 12.0,
            "total_transaction_cost": 42.0,
            "cost_drag_bps": 4.2,
            "avg_gross_exposure": 0.94,
            "avg_net_exposure": 0.91,
            "sortino_ratio": 2.9,
            "calmar_ratio": 6.25,
            "closed_positions": 7,
            "winning_positions": 4,
            "losing_positions": 3,
            "hit_rate": 4 / 7,
            "avg_win_pnl": 210.0,
            "avg_loss_pnl": -120.0,
            "avg_win_return": 0.08,
            "avg_loss_return": -0.03,
            "payoff_ratio": 1.75,
            "profit_factor": 2.1,
            "expectancy_pnl": 68.0,
            "expectancy_return": 0.021,
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sharpe_ratio"] == 2.5
    assert payload["gross_total_return"] == 0.123
    assert payload["closed_positions"] == 7
    assert payload["metadata"] == {
        "results_schema_version": 4,
        "bar_size": "5m",
        "periods_per_year": 19656,
        "strategy_scope": "long_only",
    }


@pytest.mark.asyncio
async def test_get_summary_returns_404_for_unknown_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(BacktestRepository, "get_run", lambda self, run_id: None)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-404/summary")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_backtests_returns_503_when_postgres_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def _raise_operational_error(self, **kwargs):  # type: ignore[no-untyped-def]
        raise OperationalError("db unavailable")

    monkeypatch.setattr(BacktestRepository, "list_runs", _raise_operational_error)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests")

    assert response.status_code == 503


@pytest.mark.asyncio
async def test_get_summary_returns_409_for_unpublished_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {"run_id": run_id, "status": "running", "results_ready_at": None},
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/summary")

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_get_timeseries_returns_empty_payload_for_published_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
            "bar_size": "5m",
            "results_schema_version": 4,
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_timeseries", lambda self, run_id: 0)
    monkeypatch.setattr(BacktestRepository, "list_timeseries", lambda self, run_id, **kwargs: [])

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/metrics/timeseries")

    assert response.status_code == 200
    assert response.json() == {
        "metadata": {
            "results_schema_version": 4,
            "bar_size": "5m",
            "periods_per_year": 19656,
            "strategy_scope": "long_only",
        },
        "points": [],
        "total_points": 0,
        "truncated": False,
    }


@pytest.mark.asyncio
async def test_get_timeseries_synthesizes_period_return_from_daily_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
            "bar_size": "5m",
            "results_schema_version": 4,
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_timeseries", lambda self, run_id: 1)
    monkeypatch.setattr(
        BacktestRepository,
        "list_timeseries",
        lambda self, run_id, **kwargs: [
            {
                "date": "2026-03-08T10:00:00Z",
                "portfolio_value": 101000.0,
                "drawdown": -0.01,
                "daily_return": 0.01,
                "cumulative_return": 0.01,
                "cash": 1000.0,
                "gross_exposure": 1.0,
                "net_exposure": 1.0,
                "turnover": 0.1,
                "commission": 1.0,
                "slippage_cost": 0.5,
                "trade_count": 2,
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/metrics/timeseries")

    assert response.status_code == 200
    point = response.json()["points"][0]
    assert point["daily_return"] == 0.01
    assert point["period_return"] == 0.01
    assert point["trade_count"] == 2


@pytest.mark.asyncio
async def test_get_rolling_metrics_returns_empty_payload_for_published_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
            "bar_size": "5m",
            "results_schema_version": 4,
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_rolling_metrics", lambda self, run_id, *, window_days: 0)
    monkeypatch.setattr(BacktestRepository, "list_rolling_metrics", lambda self, run_id, **kwargs: [])

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/metrics/rolling?window_days=63")

    assert response.status_code == 200
    assert response.json() == {
        "metadata": {
            "results_schema_version": 4,
            "bar_size": "5m",
            "periods_per_year": 19656,
            "strategy_scope": "long_only",
        },
        "points": [],
        "total_points": 0,
        "truncated": False,
    }


@pytest.mark.asyncio
async def test_get_rolling_metrics_synthesizes_window_periods_from_window_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
            "bar_size": "5m",
            "results_schema_version": 4,
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_rolling_metrics", lambda self, run_id, *, window_days: 1)
    monkeypatch.setattr(
        BacktestRepository,
        "list_rolling_metrics",
        lambda self, run_id, **kwargs: [
            {
                "date": "2026-03-08T10:00:00Z",
                "window_days": 63,
                "rolling_return": 0.12,
                "rolling_volatility": 0.2,
                "rolling_sharpe": 0.6,
                "rolling_max_drawdown": -0.08,
                "turnover_sum": 2.5,
                "commission_sum": 12.0,
                "slippage_cost_sum": 4.0,
                "n_trades_sum": 8.0,
                "gross_exposure_avg": 0.95,
                "net_exposure_avg": 0.95,
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/metrics/rolling?window_days=63")

    assert response.status_code == 200
    point = response.json()["points"][0]
    assert point["window_days"] == 63
    assert point["window_periods"] == 63


@pytest.mark.asyncio
async def test_get_trades_returns_empty_payload_for_published_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_trades", lambda self, run_id: 0)
    monkeypatch.setattr(BacktestRepository, "list_trades", lambda self, run_id, **kwargs: [])

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/trades?limit=100&offset=0")

    assert response.status_code == 200
    assert response.json() == {"trades": [], "total": 0, "limit": 100, "offset": 0}


@pytest.mark.asyncio
async def test_get_trades_returns_position_lifecycle_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_trades", lambda self, run_id: 1)
    monkeypatch.setattr(
        BacktestRepository,
        "list_trades",
        lambda self, run_id, **kwargs: [
            {
                "execution_date": "2026-03-08T10:00:00Z",
                "symbol": "MSFT",
                "quantity": 10.0,
                "price": 100.0,
                "notional": 1000.0,
                "commission": 1.0,
                "slippage_cost": 0.5,
                "cash_after": 98998.5,
                "position_id": "pos-1",
                "trade_role": "entry",
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/trades?limit=100&offset=0")

    assert response.status_code == 200
    trade = response.json()["trades"][0]
    assert trade["position_id"] == "pos-1"
    assert trade["trade_role"] == "entry"


@pytest.mark.asyncio
async def test_get_closed_positions_returns_paginated_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_closed_positions", lambda self, run_id: 1)
    monkeypatch.setattr(
        BacktestRepository,
        "list_closed_positions",
        lambda self, run_id, **kwargs: [
            {
                "position_id": "pos-1",
                "symbol": "MSFT",
                "opened_at": "2026-03-08T10:00:00Z",
                "closed_at": "2026-03-10T10:00:00Z",
                "holding_period_bars": 8,
                "average_cost": 100.0,
                "exit_price": 108.0,
                "max_quantity": 15.0,
                "resize_count": 2,
                "realized_pnl": 75.0,
                "realized_return": 0.05,
                "total_commission": 3.0,
                "total_slippage_cost": 1.5,
                "total_transaction_cost": 4.5,
                "exit_reason": "take_profit_fixed",
                "exit_rule_id": "tp-1",
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/positions/closed?limit=50&offset=0")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["positions"][0]["position_id"] == "pos-1"
    assert payload["positions"][0]["exit_rule_id"] == "tp-1"


@pytest.mark.asyncio
async def test_get_policy_events_returns_paginated_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BacktestRepository,
        "get_run",
        lambda self, run_id: {
            "run_id": run_id,
            "status": "completed",
            "results_ready_at": "2026-03-08T12:00:00+00:00",
        },
    )
    monkeypatch.setattr(BacktestRepository, "count_policy_events", lambda self, run_id: 1)
    monkeypatch.setattr(
        BacktestRepository,
        "list_policy_events",
        lambda self, run_id, **kwargs: [
            {
                "run_id": run_id,
                "event_seq": 1,
                "bar_ts": "2026-03-08T10:00:00Z",
                "scope": "strategy",
                "policy_type": "rebalance",
                "decision": "applied",
                "reason_code": "scheduled",
                "symbol": None,
                "position_id": None,
                "policy_id": None,
                "observed_value": 12.5,
                "threshold_value": None,
                "action": "rebalance",
                "details": {"frequency": "every_bar"},
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/policy-events?limit=50&offset=0")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["events"][0]["reason_code"] == "scheduled"
    assert payload["events"][0]["details"] == {"frequency": "every_bar"}
