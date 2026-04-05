from __future__ import annotations

from dataclasses import dataclass

from core.strategy_engine.contracts import ExitRule, StrategyConfig
from core.strategy_engine.position_state import PositionState, PriceBar

STOP_LIKE_RULE_TYPES = {"stop_loss_fixed", "trailing_stop_pct", "trailing_stop_atr"}
TAKE_PROFIT_RULE_TYPES = {"take_profit_fixed"}


@dataclass(frozen=True)
class ExitDecision:
    rule_id: str
    exit_reason: str
    exit_price: float
    price_field: str
    priority: int
    ordinal: int
    rule_type: str

    @property
    def is_stop_like(self) -> bool:
        return self.rule_type in STOP_LIKE_RULE_TYPES

    @property
    def is_take_profit(self) -> bool:
        return self.rule_type in TAKE_PROFIT_RULE_TYPES


@dataclass(frozen=True)
class ExitEvaluation:
    position_state: PositionState
    decision: ExitDecision | None
    candidates: tuple[ExitDecision, ...]
    intrabar_conflict: bool


class ExitRuleEvaluator:
    def evaluate_bar(
        self,
        strategy_config: StrategyConfig,
        position: PositionState,
        bar: PriceBar,
    ) -> ExitEvaluation:
        next_position = position.advance(bar)
        candidates: list[ExitDecision] = []

        for ordinal, rule in enumerate(strategy_config.exits):
            if next_position.bars_held < rule.minHoldBars:
                continue
            decision = self._evaluate_rule(rule, next_position, bar, ordinal)
            if decision is not None:
                candidates.append(decision)

        chosen = self._choose_decision(candidates, strategy_config.intrabarConflictPolicy)
        return ExitEvaluation(
            position_state=next_position,
            decision=chosen,
            candidates=tuple(candidates),
            intrabar_conflict=len(candidates) > 1,
        )

    def _evaluate_rule(
        self,
        rule: ExitRule,
        position: PositionState,
        bar: PriceBar,
        ordinal: int,
    ) -> ExitDecision | None:
        if rule.type == "stop_loss_fixed":
            trigger = position.entry_price * (1 - float(rule.value))
            return self._price_threshold_decision(
                rule,
                bar,
                ordinal,
                trigger,
                operator="lte",
            )

        if rule.type == "take_profit_fixed":
            trigger = position.entry_price * (1 + float(rule.value))
            return self._price_threshold_decision(
                rule,
                bar,
                ordinal,
                trigger,
                operator="gte",
            )

        if rule.type == "trailing_stop_pct":
            anchor = position.highest_since_entry or position.entry_price
            trigger = anchor * (1 - float(rule.value))
            return self._price_threshold_decision(
                rule,
                bar,
                ordinal,
                trigger,
                operator="lte",
            )

        if rule.type == "trailing_stop_atr":
            atr_value = bar.get_feature(str(rule.atrColumn))
            if atr_value is None or isinstance(atr_value, bool):
                return None
            trigger = (position.highest_since_entry or position.entry_price) - (
                float(rule.value) * float(atr_value)
            )
            return self._price_threshold_decision(
                rule,
                bar,
                ordinal,
                trigger,
                operator="lte",
            )

        if rule.type == "time_stop":
            price = bar.get_price("close")
            if price is None:
                return None
            if position.bars_held >= int(float(rule.value)):
                return ExitDecision(
                    rule_id=rule.id,
                    exit_reason=rule.type,
                    exit_price=price,
                    price_field="close",
                    priority=int(rule.priority or 0),
                    ordinal=ordinal,
                    rule_type=rule.type,
                )
            return None

        return None

    def _price_threshold_decision(
        self,
        rule: ExitRule,
        bar: PriceBar,
        ordinal: int,
        trigger_price: float,
        *,
        operator: str,
    ) -> ExitDecision | None:
        price_field = str(rule.priceField)
        observed_price = bar.get_price(price_field)
        if observed_price is None:
            return None

        triggered = observed_price <= trigger_price if operator == "lte" else observed_price >= trigger_price
        if not triggered:
            return None

        return ExitDecision(
            rule_id=rule.id,
            exit_reason=rule.type,
            exit_price=float(trigger_price),
            price_field=price_field,
            priority=int(rule.priority or 0),
            ordinal=ordinal,
            rule_type=rule.type,
        )

    def _choose_decision(
        self,
        candidates: list[ExitDecision],
        intrabar_conflict_policy: str,
    ) -> ExitDecision | None:
        if not candidates:
            return None

        ordered = sorted(candidates, key=lambda candidate: (candidate.priority, candidate.ordinal))
        if intrabar_conflict_policy == "priority_order":
            return ordered[0]

        stop_candidates = [candidate for candidate in ordered if candidate.is_stop_like]
        take_profit_candidates = [candidate for candidate in ordered if candidate.is_take_profit]
        if stop_candidates and take_profit_candidates:
            if intrabar_conflict_policy == "take_profit_first":
                return take_profit_candidates[0]
            return stop_candidates[0]

        return ordered[0]
