from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from core.strategy_engine.contracts import StrategyConfig
from core.strategy_engine.exit_rules import ExitRuleEvaluator
from core.strategy_engine.position_state import PositionState, PriceBar, TemporalValue


@dataclass(frozen=True)
class SimulatedTrade:
    symbol: str
    entry_date: TemporalValue
    exit_date: TemporalValue
    quantity: float
    entry_price: float
    exit_price: float
    exit_reason: str
    exit_rule_id: str
    bars_held: int
    intrabar_conflict_count: int


@dataclass(frozen=True)
class SimulationResult:
    position_state: PositionState
    trades: tuple[SimulatedTrade, ...]
    intrabar_conflict_count: int


class StrategySimulator:
    def __init__(self, evaluator: ExitRuleEvaluator | None = None) -> None:
        self._evaluator = evaluator or ExitRuleEvaluator()

    def simulate_position(
        self,
        strategy_config: StrategyConfig,
        position: PositionState,
        bars: Sequence[PriceBar],
    ) -> SimulationResult:
        state = position
        conflict_count = 0
        trades: list[SimulatedTrade] = []

        for bar in bars:
            evaluation = self._evaluator.evaluate_bar(strategy_config, state, bar)
            state = evaluation.position_state
            if evaluation.intrabar_conflict:
                conflict_count += 1
            if evaluation.decision is None:
                continue

            trades.append(
                SimulatedTrade(
                    symbol=state.symbol,
                    entry_date=position.entry_date,
                    exit_date=bar.date,
                    quantity=state.quantity,
                    entry_price=position.entry_price,
                    exit_price=evaluation.decision.exit_price,
                    exit_reason=evaluation.decision.exit_reason,
                    exit_rule_id=evaluation.decision.rule_id,
                    bars_held=state.bars_held,
                    intrabar_conflict_count=conflict_count,
                )
            )
            break

        return SimulationResult(
            position_state=state,
            trades=tuple(trades),
            intrabar_conflict_count=conflict_count,
        )
