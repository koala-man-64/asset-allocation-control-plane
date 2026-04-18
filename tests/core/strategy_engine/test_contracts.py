from pydantic import ValidationError

from core.strategy_engine import StrategyConfig


def _sample_universe_payload() -> dict:
    return {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 10,
                }
            ],
        },
    }


def test_strategy_config_normalizes_exit_defaults() -> None:
    config = StrategyConfig.model_validate(
        {
            "universe": _sample_universe_payload(),
            "rebalance": "weekly",
            "exits": [{"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08}],
        }
    )

    assert config.intrabarConflictPolicy == "stop_first"
    assert config.exits[0].priority == 0
    assert config.exits[0].scope == "position"
    assert config.exits[0].action == "exit_full"
    assert config.exits[0].reference == "entry_price"
    assert config.exits[0].priceField == "low"


def test_strategy_config_rejects_duplicate_exit_rule_ids() -> None:
    try:
        StrategyConfig.model_validate(
            {
                "universe": _sample_universe_payload(),
                "rebalance": "weekly",
                "exits": [
                    {"id": "dup", "type": "stop_loss_fixed", "value": 0.08},
                    {"id": "dup", "type": "take_profit_fixed", "value": 0.1},
                ],
            }
        )
    except ValidationError as exc:
        assert "Duplicate exit rule id 'dup'" in str(exc)
    else:
        raise AssertionError("Expected ValidationError for duplicate exit rule ids")


def test_time_stop_rejects_non_close_price_field() -> None:
    try:
        StrategyConfig.model_validate(
            {
                "universe": _sample_universe_payload(),
                "rebalance": "weekly",
                "exits": [
                    {
                        "id": "time-stop",
                        "type": "time_stop",
                        "value": 5,
                        "priceField": "low",
                    }
                ],
            }
        )
    except ValidationError as exc:
        assert "time_stop only supports priceField='close'" in str(exc)
    else:
        raise AssertionError("Expected ValidationError for invalid time_stop priceField")


def test_strategy_config_rejects_empty_universe_group() -> None:
    try:
        StrategyConfig.model_validate(
            {
                "universe": {
                    "source": "postgres_gold",
                    "root": {"kind": "group", "operator": "and", "clauses": []},
                },
                "rebalance": "weekly",
                "exits": [],
            }
        )
    except ValidationError as exc:
        assert "clauses" in str(exc)
    else:
        raise AssertionError("Expected ValidationError for empty universe groups")
