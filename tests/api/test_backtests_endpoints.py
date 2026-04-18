from __future__ import annotations

from datetime import datetime, timezone

import pytest
from psycopg import OperationalError

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
    assert "output_dir" not in captured
    assert "adls_container" not in captured
    assert "adls_prefix" not in captured


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
