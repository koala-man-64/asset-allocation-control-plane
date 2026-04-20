import logging
import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP

from alpaca.config import ExecutionConfig
from alpaca.models import AlpacaPosition
from alpaca.price_cache import PriceCache

logger = logging.getLogger(__name__)


@dataclass
class PlannedOrder:
    symbol: str
    side: str
    qty: float
    estimated_price: float
    estimated_notional: float


@dataclass
class RebalancePlan:
    orders: list[PlannedOrder] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    valid: bool = True
    error: str | None = None


class RebalancePlanner:
    def __init__(self, config: ExecutionConfig, price_cache: PriceCache) -> None:
        self._config = config
        self._prices = price_cache

    def plan(
        self,
        target_weights: dict[str, float],
        current_positions: dict[str, AlpacaPosition],
        equity: float,
    ) -> RebalancePlan:
        plan = RebalancePlan()

        if equity <= 0:
            plan.valid = False
            plan.error = "Equity must be positive"
            return plan

        all_symbols = set(target_weights.keys()) | set(current_positions.keys())

        for symbol in all_symbols:
            target_weight = target_weights.get(symbol, 0.0)
            current_position = current_positions.get(symbol)

            price = self._prices.get_price(symbol)
            if price is None and current_position:
                price = current_position.current_price

            if price is None or price <= 0:
                plan.skipped.append((symbol, "Missing price"))
                logger.warning("Skipping %s: no price available.", symbol)
                continue

            current_qty = current_position.qty if current_position else 0.0
            target_notional = equity * target_weight
            target_qty_raw = target_notional / price
            delta_qty_raw = target_qty_raw - current_qty

            if abs(delta_qty_raw) < 1e-9:
                continue

            side = "buy" if delta_qty_raw > 0 else "sell"
            rounded_qty = self._round_qty(abs(delta_qty_raw))

            if rounded_qty < self._config.min_trade_shares:
                plan.skipped.append((symbol, f"Qty {rounded_qty} < min {self._config.min_trade_shares}"))
                continue

            estimated_notional = rounded_qty * price
            if estimated_notional < self._config.min_trade_notional:
                is_close = target_weight == 0.0 and current_qty != 0
                if not is_close:
                    plan.skipped.append(
                        (symbol, f"Notional {estimated_notional:.2f} < min {self._config.min_trade_notional}")
                    )
                    continue

            if target_weight == 0.0 and current_position:
                rounded_qty = abs(current_position.qty)

            if rounded_qty <= 0:
                continue

            plan.orders.append(
                PlannedOrder(
                    symbol=symbol,
                    side=side,
                    qty=rounded_qty,
                    estimated_price=price,
                    estimated_notional=rounded_qty * price,
                )
            )

        return plan

    def _round_qty(self, qty: float) -> float:
        if self._config.allow_fractional_shares:
            return float(Decimal(str(qty)).quantize(Decimal("0.000000001"), rounding=ROUND_HALF_UP))

        mode = self._config.rounding_mode
        decimal_qty = Decimal(str(qty))
        if mode == "toward_zero":
            return int(decimal_qty)
        if mode == "floor":
            return int(math.floor(qty))
        if mode == "ceil":
            return int(math.ceil(qty))
        if mode == "nearest":
            return int(round(qty))
        return int(qty)
