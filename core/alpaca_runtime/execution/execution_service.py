import asyncio
import logging
from typing import Any

from alpaca.config import ExecutionConfig
from alpaca.models import AlpacaOrder
from alpaca.trading_rest import AlpacaTradingClient
from core.alpaca_runtime.execution.order_factory import OrderFactory
from core.alpaca_runtime.execution.rebalance_planner import RebalancePlan, RebalancePlanner
from core.alpaca_runtime.price_cache import PriceCache
from core.alpaca_runtime.state import StateManager

logger = logging.getLogger(__name__)


class ExecutionService:
    def __init__(
        self,
        config: Any,
        client: AlpacaTradingClient,
        state_manager: StateManager,
        price_cache: PriceCache,
    ) -> None:
        self._config = config
        self._execution_config: ExecutionConfig = getattr(config, "execution", config)
        self._client = client
        self._state_manager = state_manager
        self._price_cache = price_cache

        self._planner = RebalancePlanner(self._execution_config, price_cache)
        self._factory = OrderFactory(self._execution_config)

    async def rebalance_to_target_weights(
        self,
        target_weights: dict[str, float],
        strategy_id: str,
        rebalance_id: str,
        wait_for_fills: bool = True,
        timeout_s: float = 300.0,
    ) -> dict[str, Any]:
        """Execute a rebalance against the current brokerage state."""

        logger.info("Starting rebalance %s for strategy %s", rebalance_id, strategy_id)

        current_state = self._state_manager.state
        current_positions = current_state.positions.copy()

        equity = current_state.account.equity
        if equity <= 0:
            logger.error("Equity is zero or negative. Cannot rebalance.")
            return {"status": "failed", "reason": "No equity"}

        plan: RebalancePlan = self._planner.plan(
            target_weights=target_weights,
            current_positions=current_positions,
            equity=equity,
        )

        if not plan.valid:
            return {"status": "failed", "reason": plan.error}

        logger.info("Plan generated: %s orders. Skipped: %s", len(plan.orders), len(plan.skipped))

        submitted_orders: list[AlpacaOrder] = []
        errors: list[tuple[str, str]] = []

        for order_plan in plan.orders:
            payload = self._factory.create_order_payload(order_plan, strategy_id, rebalance_id)
            try:
                loop = asyncio.get_running_loop()
                order = await loop.run_in_executor(None, lambda: self._client.submit_order(**payload))
                submitted_orders.append(order)
                logger.info("Submitted %s %s %s (id=%s)", order.side, order.qty, order.symbol, order.id)
            except Exception as exc:
                logger.error("Failed to submit order for %s: %s", order_plan.symbol, exc)
                errors.append((order_plan.symbol, str(exc)))

        result: dict[str, Any] = {
            "status": "submitted",
            "submitted_count": len(submitted_orders),
            "orders": [order.id for order in submitted_orders],
            "errors": errors,
            "plan_skipped": plan.skipped,
        }

        if wait_for_fills and submitted_orders:
            logger.info("Waiting for %s orders to fill (timeout=%ss)...", len(submitted_orders), timeout_s)
            fills = await self._wait_for_orders(submitted_orders, timeout_s)
            result["status"] = "completed"
            result["filled_count"] = len(fills)
            result["fills"] = fills

        return result

    async def _wait_for_orders(self, orders: list[AlpacaOrder], timeout_s: float) -> list[str]:
        start_time = asyncio.get_event_loop().time()
        pending_ids = {order.id for order in orders}
        filled_ids: set[str] = set()

        while pending_ids:
            if asyncio.get_event_loop().time() - start_time > timeout_s:
                logger.warning("Timed out waiting for fills.")
                break

            check_ids = list(pending_ids)
            for order_id in check_ids:
                try:
                    loop = asyncio.get_running_loop()
                    order = await loop.run_in_executor(None, lambda: self._client.get_order(order_id))

                    if order.status == "filled":
                        filled_ids.add(order_id)
                        pending_ids.remove(order_id)
                    elif order.status in ("canceled", "expired", "rejected"):
                        pending_ids.remove(order_id)
                except Exception:
                    pass

            if pending_ids:
                await asyncio.sleep(1.0)

        return list(filled_ids)
