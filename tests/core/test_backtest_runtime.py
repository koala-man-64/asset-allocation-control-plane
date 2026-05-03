from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from core.backtest_runtime import (
    PolicyRuntimeState,
    ResolvedBacktestDefinition,
    _apply_strategy_risk_policy,
    _regime_context_for_session,
    _schedule_rebalance_targets,
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


def _policy_config(**overrides) -> StrategyConfig:
    payload = {
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
    payload.update(overrides)
    return StrategyConfig.model_validate(payload)


def test_structured_rebalance_policy_honors_every_n_bars_and_manual_skip() -> None:
    records = [
        {"symbol": "MSFT", "selected": True, "target_weight": 1.0},
    ]
    snapshot = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-03-03T14:30:00Z")],
            "symbol": ["MSFT"],
            "market_data__close": [100.0],
            "market_data__open": [100.0],
        }
    )
    events: list[dict[str, object]] = []
    state = PolicyRuntimeState()
    policy = _policy_config(rebalancePolicy={"frequency": "every_n_bars", "intervalBars": 2}).rebalancePolicy

    first_targets = _schedule_rebalance_targets(
        records,
        policy=policy,
        risk_policy=None,
        state=state,
        positions={},
        snapshot=snapshot,
        previous_close_by_symbol={},
        close_equity=100000.0,
        current_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        bar_index=0,
        policy_event_rows=events,
    )
    skipped_targets = _schedule_rebalance_targets(
        records,
        policy=policy,
        risk_policy=None,
        state=state,
        positions={},
        snapshot=snapshot,
        previous_close_by_symbol={},
        close_equity=100000.0,
        current_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        bar_index=1,
        policy_event_rows=events,
    )
    second_targets = _schedule_rebalance_targets(
        records,
        policy=policy,
        risk_policy=None,
        state=state,
        positions={},
        snapshot=snapshot,
        previous_close_by_symbol={},
        close_equity=100000.0,
        current_ts=datetime(2026, 3, 3, 14, 40, tzinfo=timezone.utc),
        bar_index=2,
        policy_event_rows=events,
    )

    assert first_targets == {"MSFT": 1.0}
    assert skipped_targets is None
    assert second_targets == {"MSFT": 1.0}

    manual_events: list[dict[str, object]] = []
    manual_policy = _policy_config(rebalancePolicy={"frequency": "manual"}).rebalancePolicy
    assert (
        _schedule_rebalance_targets(
            records,
            policy=manual_policy,
            risk_policy=None,
            state=PolicyRuntimeState(),
            positions={},
            snapshot=snapshot,
            previous_close_by_symbol={},
            close_equity=100000.0,
            current_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            bar_index=0,
            policy_event_rows=manual_events,
        )
        is None
    )
    assert manual_events[0]["reason_code"] == "manual_mode"


def test_rebalance_policy_applies_drift_turnover_and_reentry_constraints() -> None:
    policy = _policy_config(
        rebalancePolicy={
            "frequency": "every_bar",
            "maxTurnoverPct": 10.0,
            "allowPartialRebalance": True,
        },
        strategyRiskPolicy={
            "stopLoss": {"thresholdPct": 8.0, "action": "reduce_exposure", "reductionPct": 50.0},
            "reentry": {"cooldownBars": 1},
        },
    )
    state = PolicyRuntimeState(position_cooldown_until={"MSFT": 0})
    events: list[dict[str, object]] = []
    snapshot = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-03-03T14:30:00Z")],
            "symbol": ["MSFT"],
            "market_data__close": [100.0],
            "market_data__open": [100.0],
        }
    )

    targets = _schedule_rebalance_targets(
        [{"symbol": "MSFT", "selected": True, "target_weight": 1.0}],
        policy=policy.rebalancePolicy,
        risk_policy=policy.strategyRiskPolicy,
        state=state,
        positions={},
        snapshot=snapshot,
        previous_close_by_symbol={},
        close_equity=100000.0,
        current_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        bar_index=0,
        policy_event_rows=events,
    )

    assert targets == {}
    assert any(event["reason_code"] == "cooldown" for event in events)

    state = PolicyRuntimeState()
    events = []
    targets = _schedule_rebalance_targets(
        [{"symbol": "MSFT", "selected": True, "target_weight": 1.0}],
        policy=policy.rebalancePolicy,
        risk_policy=policy.strategyRiskPolicy,
        state=state,
        positions={},
        snapshot=snapshot,
        previous_close_by_symbol={},
        close_equity=100000.0,
        current_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        bar_index=0,
        policy_event_rows=events,
    )

    assert targets == {"MSFT": 0.1}
    assert any(event["reason_code"] == "turnover_cap" and event["action"] == "partial_rebalance" for event in events)

    drift_policy = _policy_config(
        rebalancePolicy={
            "frequency": "every_bar",
            "driftThresholdPct": 1.0,
        }
    ).rebalancePolicy
    events = []
    targets = _schedule_rebalance_targets(
        [{"symbol": "MSFT", "selected": True, "target_weight": 0.005}],
        policy=drift_policy,
        risk_policy=None,
        state=PolicyRuntimeState(),
        positions={},
        snapshot=snapshot,
        previous_close_by_symbol={},
        close_equity=100000.0,
        current_ts=datetime(2026, 3, 3, 14, 40, tzinfo=timezone.utc),
        bar_index=0,
        policy_event_rows=events,
    )

    assert targets is None
    assert events[0]["reason_code"] == "drift_below_threshold"


def test_strategy_risk_policy_applies_nav_stop_and_take_profit_baseline_reset() -> None:
    stop_policy = _policy_config(
        strategyRiskPolicy={
            "stopLoss": {"thresholdPct": 5.0, "action": "reduce_exposure", "reductionPct": 50.0},
            "reentry": {"cooldownBars": 2},
        }
    ).strategyRiskPolicy
    state = PolicyRuntimeState(strategy_nav_baseline=100000.0, strategy_nav_peak=100000.0)
    events: list[dict[str, object]] = []

    _apply_strategy_risk_policy(
        stop_policy,
        state=state,
        close_equity=94000.0,
        current_ts=datetime(2026, 3, 3, 15, 0, tzinfo=timezone.utc),
        bar_index=4,
        policy_event_rows=events,
    )

    assert state.strategy_exposure_multiplier == 0.5
    assert state.strategy_cooldown_until_index == 6
    assert state.strategy_nav_baseline == 94000.0
    assert events[0]["reason_code"] == "nav_drawdown_stop_loss"

    take_profit_policy = _policy_config(
        strategyRiskPolicy={
            "takeProfit": {"thresholdPct": 10.0, "action": "rebalance_to_target"},
            "reentry": {"cooldownBars": 0},
        }
    ).strategyRiskPolicy
    state = PolicyRuntimeState(strategy_nav_baseline=100000.0, strategy_nav_peak=100000.0)
    events = []

    _apply_strategy_risk_policy(
        take_profit_policy,
        state=state,
        close_equity=111000.0,
        current_ts=datetime(2026, 3, 3, 15, 5, tzinfo=timezone.utc),
        bar_index=5,
        policy_event_rows=events,
    )

    assert state.strategy_nav_baseline == 111000.0
    assert state.strategy_nav_peak == 111000.0
    assert events[0]["reason_code"] == "nav_gain_take_profit"


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


def test_regime_context_attaches_multilabel_metadata_without_blocking() -> None:
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
                "modelName": "default-regime",
                "mode": "observe_only",
            },
            "exits": [],
        }
    ).regimePolicy

    context = _regime_context_for_session(
        policy,
        {
            "active_regimes": ["trending_down", "high_volatility"],
            "signals": [
                {
                    "regime_code": "trending_down",
                    "display_name": "Trending (Down)",
                    "signal_state": "active",
                    "score": 0.92,
                    "activation_threshold": 0.6,
                    "is_active": True,
                    "matched_rule_id": "trending_down",
                    "evidence": {},
                }
            ],
            "halt_flag": False,
            "halt_reason": None,
        },
    )
    assert context["primary_regime_code"] == "trending_down"
    assert context["active_regimes"] == ["trending_down", "high_volatility"]
    assert context["signals"][0]["regime_code"] == "trending_down"
    assert context["halt_flag"] is False
