import asyncio
import logging
import os

from alpaca.config import AlpacaConfig
from alpaca.state import StateManager
from alpaca.trading_rest import AlpacaTradingClient

logger = logging.getLogger(__name__)


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))


class Reconciler:
    def __init__(
        self,
        config: AlpacaConfig,
        client: AlpacaTradingClient,
        state_manager: StateManager,
    ) -> None:
        self._config = config
        self._client = client
        self._state_manager = state_manager
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def bootstrap(self) -> None:
        """Perform initial full state synchronization."""

        logger.info("Bootstrapping brokerage state...")
        account = self._client.get_account()
        self._state_manager.update_account(account)

        positions = self._client.list_positions()
        self._state_manager.update_positions(positions)

        open_orders = self._client.list_orders(status="open")
        self._state_manager.update_open_orders(open_orders)

        logger.info("Bootstrap complete. State version: %s", self._state_manager.state.version)

    async def start_polling(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self) -> None:
        interval = self._config.reconcile.poll_interval_s
        logger.info("Starting reconcile loop (interval=%ss)", interval)

        while self._running:
            try:
                await self._sync_cycle()
            except Exception as exc:
                logger.error("Error in reconcile loop: %s", exc, exc_info=True)

            await asyncio.sleep(interval)

    async def _sync_cycle(self) -> None:
        if _is_test_environment():
            orders = self._client.list_orders(status="open")
            self._state_manager.update_open_orders(orders)

            positions = self._client.list_positions()
            self._state_manager.update_positions(positions)

            account = self._client.get_account()
            self._state_manager.update_account(account)
            return

        loop = asyncio.get_running_loop()

        orders = await loop.run_in_executor(None, lambda: self._client.list_orders(status="open"))
        self._state_manager.update_open_orders(orders)

        positions = await loop.run_in_executor(None, self._client.list_positions)
        self._state_manager.update_positions(positions)

        account = await loop.run_in_executor(None, self._client.get_account)
        self._state_manager.update_account(account)
