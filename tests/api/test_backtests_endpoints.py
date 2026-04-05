from __future__ import annotations

from datetime import datetime, timezone

import pytest

from api.endpoints import backtests as backtest_endpoints
from api.service.app import create_app
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
                        "table": "market_data",
                        "column": "close",
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
                    "targetGrossExposureByRegime": {
                        "trending_bull": 1.0,
                        "trending_bear": 0.5,
                        "choppy_mean_reversion": 0.75,
                        "high_vol": 0.0,
                        "unclassified": 0.0,
                    },
                    "blockOnTransition": True,
                    "blockOnUnclassified": True,
                    "honorHaltFlag": True,
                    "onBlocked": "skip_entries",
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
                "targetGrossExposureByRegime": {
                    "trending_bull": 1.0,
                    "trending_bear": 0.5,
                    "choppy_mean_reversion": 0.75,
                    "high_vol": 0.0,
                    "unclassified": 0.0,
                },
                "blockOnTransition": True,
                "blockOnUnclassified": True,
                "honorHaltFlag": True,
                "onBlocked": "skip_entries",
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
                "output_dir": "/tmp/backtests",
                "adls_container": "common",
                "adls_prefix": "backtests/mom-spy-res",
                "error": None,
            }
        ],
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests?limit=10&offset=0")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runs"][0]["run_id"] == "run-1"
    assert payload["limit"] == 10


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
            "output_dir": kwargs.get("output_dir"),
            "adls_container": kwargs.get("adls_container"),
            "adls_prefix": kwargs.get("adls_prefix"),
            "error": None,
        }

    monkeypatch.setattr(BacktestRepository, "create_run", fake_create_run)
    monkeypatch.setattr(backtest_endpoints, "resolve_backtest_definition", lambda *args, **kwargs: _sample_definition())
    monkeypatch.setattr(
        backtest_endpoints,
        "validate_backtest_submission",
        lambda *args, **kwargs: [
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
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


@pytest.mark.asyncio
async def test_get_summary_returns_runtime_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        backtest_endpoints,
        "load_summary",
        lambda run_id, *, repo: {
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
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/backtests/run-1/summary")

    assert response.status_code == 200
    assert response.json()["sharpe_ratio"] == 2.5
