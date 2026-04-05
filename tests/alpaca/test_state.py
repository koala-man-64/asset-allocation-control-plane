import pytest
from datetime import datetime, timezone
from alpaca.models import (
    BrokerageState, AlpacaAccount, AlpacaOrder, TradeUpdateEvent
)
from alpaca.state import StateManager

@pytest.fixture
def empty_state():
    return BrokerageState(
        account=AlpacaAccount("1", "A", "ACTIVE", "USD", 100, 100, 200, 0, datetime.now(timezone.utc)),
        positions={},
        open_orders={},
        position_states={}
    )

def test_update_open_orders(empty_state):
    mgr = StateManager(empty_state)
    order = AlpacaOrder("o1", "c1", "AAPL", datetime.now(), datetime.now(), datetime.now(), None, None, None, None, "a1", "eq", 10, 0, "market", "buy", "day", None, None, "new")
    
    mgr.update_open_orders([order])
    assert "o1" in mgr.state.open_orders
    assert mgr.state.version == 1

def test_apply_trade_event_new(empty_state):
    mgr = StateManager(empty_state)
    order = AlpacaOrder("o1", "c1", "AAPL", datetime.now(), datetime.now(), datetime.now(), None, None, None, None, "a1", "eq", 10, 0, "market", "buy", "day", None, None, "new")
    event = TradeUpdateEvent("new", None, None, datetime.now(), order)
    
    mgr.apply_trade_event(event)
    assert "o1" in mgr.state.open_orders

def test_apply_trade_event_filled(empty_state):
    mgr = StateManager(empty_state)
    # Start with open order
    order = AlpacaOrder("o1", "c1", "AAPL", datetime.now(), datetime.now(), datetime.now(), None, None, None, None, "a1", "eq", 10, 0, "market", "buy", "day", None, None, "new")
    mgr.update_open_orders([order])
    
    # Fill event
    filled_order = AlpacaOrder("o1", "c1", "AAPL", datetime.now(), datetime.now(), datetime.now(), None, None, None, None, "a1", "eq", 10, 10, "market", "buy", "day", None, None, "filled")
    event = TradeUpdateEvent("filled", 150.0, 10.0, datetime.now(), filled_order, position_qty=10.0)
    
    mgr.apply_trade_event(event)
    
    assert "o1" not in mgr.state.open_orders
    assert "AAPL" in mgr.state.positions
    assert mgr.state.positions["AAPL"].qty == 10.0
