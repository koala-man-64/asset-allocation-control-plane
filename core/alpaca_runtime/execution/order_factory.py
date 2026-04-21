import hashlib
from typing import Any

from alpaca.config import ExecutionConfig
from core.alpaca_runtime.execution.rebalance_planner import PlannedOrder


class OrderFactory:
    def __init__(self, config: ExecutionConfig) -> None:
        self._config = config

    def create_order_payload(
        self,
        plan_order: PlannedOrder,
        strategy_id: str,
        rebalance_id: str,
    ) -> dict[str, Any]:
        raw_id = f"{strategy_id}|{rebalance_id}|{plan_order.symbol}|{plan_order.side}"
        if len(raw_id) <= 48:
            client_order_id = raw_id
        else:
            digest = hashlib.md5(raw_id.encode("utf-8")).hexdigest()[:12]
            prefix = f"{plan_order.symbol}-{plan_order.side}"
            client_order_id = f"{prefix}-{digest}"[:48]

        return {
            "symbol": plan_order.symbol,
            "qty": plan_order.qty,
            "side": plan_order.side,
            "type": self._config.default_order_type,
            "time_in_force": self._config.time_in_force,
            "client_order_id": client_order_id,
        }
