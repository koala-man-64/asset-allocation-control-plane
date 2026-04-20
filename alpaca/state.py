import logging
from datetime import datetime, timezone

from alpaca.models import AlpacaAccount, AlpacaOrder, AlpacaPosition, BrokerageState, TradeUpdateEvent

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StateManager:
    def __init__(self, initial_state: BrokerageState) -> None:
        self._state = initial_state

    @property
    def state(self) -> BrokerageState:
        return self._state

    def update_account(self, account: AlpacaAccount) -> None:
        self._state.account = account
        self._state.last_update = _utc_now()
        self._state.version += 1

    def update_positions(self, positions: list[AlpacaPosition]) -> None:
        self._state.positions = {position.symbol: position for position in positions}
        self._state.last_update = _utc_now()
        self._state.version += 1

    def update_open_orders(self, orders: list[AlpacaOrder]) -> None:
        self._state.open_orders = {order.id: order for order in orders}
        self._state.last_update = _utc_now()
        self._state.version += 1

    def apply_trade_event(self, event: TradeUpdateEvent) -> None:
        """Apply a streaming trade event to the local state."""

        order = event.order

        if event.event in ("new", "accepted", "pending_new"):
            self._state.open_orders[order.id] = order
        elif event.event in ("filled", "canceled", "expired", "rejected", "suspended"):
            if order.id in self._state.open_orders and event.event in ("filled", "canceled", "expired", "rejected"):
                del self._state.open_orders[order.id]
        elif event.event == "partial_fill":
            self._state.open_orders[order.id] = order

        if event.event in ("filled", "partial_fill"):
            symbol = order.symbol
            filled_price = event.price
            if event.position_qty is not None:
                current_position = self._state.positions.get(symbol)
                if current_position:
                    current_position.qty = event.position_qty
                else:
                    self._state.positions[symbol] = AlpacaPosition(
                        symbol=symbol,
                        qty=event.position_qty,
                        market_value=event.position_qty * (filled_price or 0.0),
                        avg_entry_price=filled_price or 0.0,
                        current_price=filled_price or 0.0,
                        change_today=0.0,
                        unrealized_pl=0.0,
                        side="long" if event.position_qty > 0 else "short",
                    )

        self._state.last_update = _utc_now()
        self._state.version += 1
