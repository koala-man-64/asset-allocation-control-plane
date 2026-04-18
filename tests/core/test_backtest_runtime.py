from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from core.backtest_runtime import (
    ResolvedBacktestDefinition,
    _regime_context_for_session,
    _score_snapshot,
    validate_backtest_submission,
)
from core.ranking_engine.contracts import RankingSchemaConfig
from core.strategy_engine.contracts import StrategyConfig, UniverseDefinition
from core.strategy_engine import universe as universe_service


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
    )


def test_score_snapshot_breaks_ties_by_symbol() -> None:
    ranked = _score_snapshot(
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-03T14:30:00Z")] * 3,
                "symbol": ["MSFT", "AAPL", "NVDA"],
                "market_data__close": [10.0, 10.0, 10.0],
                "market_data__return_20d": [0.5, 0.5, 0.2],
            }
        ),
        definition=_sample_definition(),
        rebalance_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
    )

    assert ranked["symbol"].tolist()[:2] == ["AAPL", "MSFT"]
    assert ranked["ordinal"].tolist()[:2] == [1, 2]


def test_validate_backtest_submission_rejects_intraday_coverage_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    specs = {
        "market_data": universe_service.UniverseTableSpec(
            schema="gold",
            name="market_data",
            as_of_column="as_of_ts",
            as_of_kind="intraday",
            columns={
                "open": universe_service.UniverseColumnSpec("open", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "high": universe_service.UniverseColumnSpec("high", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "low": universe_service.UniverseColumnSpec("low", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "close": universe_service.UniverseColumnSpec("close", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "volume": universe_service.UniverseColumnSpec("volume", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "return_20d": universe_service.UniverseColumnSpec("return_20d", "double precision", "number", universe_service._NUMBER_OPERATORS),
            },
        )
    }
    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    monkeypatch.setattr(
        "core.backtest_runtime._load_run_schedule",
        lambda *args, **kwargs: [
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
    )
    monkeypatch.setattr(
        "core.backtest_runtime._load_exact_coverage",
        lambda *args, **kwargs: {datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc)},
    )

    with pytest.raises(ValueError) as exc:
        validate_backtest_submission(
            "postgresql://test:test@localhost:5432/asset_allocation",
            definition=_sample_definition(),
            start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
            bar_size="5m",
        )

    assert "Intraday feature coverage gap" in str(exc.value)


def test_validate_backtest_submission_rejects_regime_coverage_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = ResolvedBacktestDefinition(
        **(_sample_definition().__dict__ | {"regime_model_name": "default-regime", "regime_model_version": 1})
    )
    specs = {
        "market_data": universe_service.UniverseTableSpec(
            schema="gold",
            name="market_data",
            as_of_column="as_of_ts",
            as_of_kind="intraday",
            columns={
                "open": universe_service.UniverseColumnSpec("open", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "high": universe_service.UniverseColumnSpec("high", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "low": universe_service.UniverseColumnSpec("low", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "close": universe_service.UniverseColumnSpec("close", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "volume": universe_service.UniverseColumnSpec("volume", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "return_20d": universe_service.UniverseColumnSpec("return_20d", "double precision", "number", universe_service._NUMBER_OPERATORS),
            },
        )
    }
    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    monkeypatch.setattr(
        "core.backtest_runtime._load_run_schedule",
        lambda *args, **kwargs: [
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
    )
    monkeypatch.setattr(
        "core.backtest_runtime._load_exact_coverage",
        lambda *args, **kwargs: {
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        },
    )
    monkeypatch.setattr(
        "core.backtest_runtime._validate_regime_history_coverage",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("Regime history coverage gap")),
    )

    with pytest.raises(ValueError) as exc:
        validate_backtest_submission(
            "postgresql://test:test@localhost:5432/asset_allocation",
            definition=definition,
            start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
            bar_size="5m",
        )

    assert "Regime history coverage gap" in str(exc.value)


def test_regime_context_blocks_and_scales_exposure() -> None:
    policy = StrategyConfig.model_validate(
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
                "enabled": True,
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
    ).regimePolicy

    confirmed = _regime_context_for_session(
        policy,
        {
            "regime_code": "trending_bear",
            "regime_status": "confirmed",
            "halt_flag": False,
            "matched_rule_id": "trending_bear",
        },
    )
    assert confirmed["blocked"] is False
    assert confirmed["exposure_multiplier"] == 0.5

    transition = _regime_context_for_session(
        policy,
        {
            "regime_code": "trending_bear",
            "regime_status": "transition",
            "halt_flag": False,
        },
    )
    assert transition["blocked"] is True
    assert transition["blocked_reason"] == "transition"
    assert transition["blocked_action"] == "skip_entries"
