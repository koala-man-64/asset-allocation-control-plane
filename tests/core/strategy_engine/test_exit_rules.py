from core.strategy_engine import (
    ExitRuleEvaluator,
    PositionState,
    PriceBar,
    StrategyConfig,
    StrategySimulator,
)


def _sample_universe_payload() -> dict:
    return {
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
                    "value": 10,
                }
            ],
        },
    }


def test_stop_loss_fixed_emits_exit_at_trigger_price() -> None:
    config = StrategyConfig.model_validate(
        {
            "universe": _sample_universe_payload(),
            "rebalance": "weekly",
            "exits": [{"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08}],
        }
    )
    position = PositionState(symbol="AAPL", entry_date="2026-03-01", entry_price=100.0, quantity=10.0)
    bar = PriceBar(date="2026-03-02", high=101.0, low=90.0, close=95.0)

    evaluation = ExitRuleEvaluator().evaluate_bar(config, position, bar)

    assert evaluation.decision is not None
    assert evaluation.decision.rule_id == "stop-8"
    assert evaluation.decision.exit_price == 92.0
    assert evaluation.position_state.bars_held == 1


def test_trailing_stop_pct_updates_anchor_before_evaluation() -> None:
    config = StrategyConfig.model_validate(
        {
            "universe": _sample_universe_payload(),
            "rebalance": "weekly",
            "exits": [{"id": "trail-7", "type": "trailing_stop_pct", "value": 0.07}],
        }
    )
    position = PositionState(
        symbol="AAPL",
        entry_date="2026-03-01",
        entry_price=100.0,
        quantity=10.0,
        highest_since_entry=100.0,
    )
    bar = PriceBar(date="2026-03-02", high=120.0, low=111.0, close=118.0)

    evaluation = ExitRuleEvaluator().evaluate_bar(config, position, bar)

    assert evaluation.decision is not None
    assert round(evaluation.decision.exit_price, 2) == 111.60
    assert evaluation.position_state.highest_since_entry == 120.0


def test_trailing_stop_atr_uses_feature_value() -> None:
    config = StrategyConfig.model_validate(
        {
            "universe": _sample_universe_payload(),
            "rebalance": "weekly",
            "exits": [
                {
                    "id": "trail-atr",
                    "type": "trailing_stop_atr",
                    "value": 3.0,
                    "atrColumn": "atr_14d",
                }
            ],
        }
    )
    position = PositionState(
        symbol="AAPL",
        entry_date="2026-03-01",
        entry_price=100.0,
        quantity=10.0,
        highest_since_entry=110.0,
    )
    bar = PriceBar(date="2026-03-02", high=115.0, low=108.0, close=114.0, features={"atr_14d": 2.0})

    evaluation = ExitRuleEvaluator().evaluate_bar(config, position, bar)

    assert evaluation.decision is not None
    assert evaluation.decision.rule_id == "trail-atr"
    assert evaluation.decision.exit_price == 109.0


def test_time_stop_uses_close_after_required_bars() -> None:
    config = StrategyConfig.model_validate(
        {
            "universe": _sample_universe_payload(),
            "rebalance": "weekly",
            "exits": [{"id": "time-stop", "type": "time_stop", "value": 3}],
        }
    )
    position = PositionState(
        symbol="AAPL",
        entry_date="2026-03-01",
        entry_price=100.0,
        quantity=10.0,
        bars_held=2,
    )
    bar = PriceBar(date="2026-03-04", close=107.0)

    evaluation = ExitRuleEvaluator().evaluate_bar(config, position, bar)

    assert evaluation.decision is not None
    assert evaluation.decision.exit_reason == "time_stop"
    assert evaluation.decision.exit_price == 107.0


def test_intrabar_conflict_policy_prefers_stop_or_take_profit() -> None:
    base_payload = {
        "universe": _sample_universe_payload(),
        "rebalance": "weekly",
        "exits": [
            {"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08, "priority": 10},
            {"id": "take-10", "type": "take_profit_fixed", "value": 0.10, "priority": 5},
        ],
    }
    position = PositionState(symbol="AAPL", entry_date="2026-03-01", entry_price=100.0, quantity=10.0)
    bar = PriceBar(date="2026-03-02", high=115.0, low=90.0, close=105.0)
    evaluator = ExitRuleEvaluator()

    stop_first = evaluator.evaluate_bar(StrategyConfig.model_validate(base_payload), position, bar)
    assert stop_first.decision is not None
    assert stop_first.decision.rule_id == "stop-8"
    assert stop_first.intrabar_conflict is True

    take_profit_first = evaluator.evaluate_bar(
        StrategyConfig.model_validate({**base_payload, "intrabarConflictPolicy": "take_profit_first"}),
        position,
        bar,
    )
    assert take_profit_first.decision is not None
    assert take_profit_first.decision.rule_id == "take-10"

    priority_order = evaluator.evaluate_bar(
        StrategyConfig.model_validate({**base_payload, "intrabarConflictPolicy": "priority_order"}),
        position,
        bar,
    )
    assert priority_order.decision is not None
    assert priority_order.decision.rule_id == "take-10"


def test_strategy_simulator_emits_trade_metadata() -> None:
    config = StrategyConfig.model_validate(
        {
            "universe": _sample_universe_payload(),
            "rebalance": "weekly",
            "exits": [
                {"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08},
                {"id": "take-10", "type": "take_profit_fixed", "value": 0.10},
            ],
        }
    )
    position = PositionState(symbol="AAPL", entry_date="2026-03-01", entry_price=100.0, quantity=10.0)
    bars = [PriceBar(date="2026-03-02", high=115.0, low=90.0, close=105.0)]

    result = StrategySimulator().simulate_position(config, position, bars)

    assert result.intrabar_conflict_count == 1
    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == "stop_loss_fixed"
    assert result.trades[0].exit_rule_id == "stop-8"
    assert result.trades[0].entry_price == 100.0
    assert result.trades[0].exit_price == 92.0
    assert result.trades[0].bars_held == 1
    assert result.trades[0].intrabar_conflict_count == 1
