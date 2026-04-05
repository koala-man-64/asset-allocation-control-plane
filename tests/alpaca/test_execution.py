import pytest
from unittest.mock import MagicMock
from alpaca.execution.rebalance_planner import RebalancePlanner, PlannedOrder
from alpaca.execution.order_factory import OrderFactory
from alpaca.execution.execution_service import ExecutionService
from alpaca.models import AlpacaPosition
from alpaca.config import ExecutionConfig

@pytest.fixture
def mock_exec_config():
    return ExecutionConfig(
        min_trade_notional=10.0,
        min_trade_shares=1.0,
        allow_fractional_shares=True
    )

@pytest.fixture
def mock_price_cache():
    pc = MagicMock()
    pc.get_price.side_effect = lambda s: {"AAPL": 150.0, "MSFT": 300.0}.get(s)
    return pc

def test_planner_basic_buy(mock_exec_config, mock_price_cache):
    planner = RebalancePlanner(mock_exec_config, mock_price_cache)
    
    # Target: 50% AAPL, Equity 10,000. 
    # Target Val = 5000. Price = 150. Target Qty = 33.3333...
    
    plan = planner.plan(
        target_weights={"AAPL": 0.5},
        current_positions={},
        equity=10000.0
    )
    
    assert plan.valid
    assert len(plan.orders) == 1
    o = plan.orders[0]
    assert o.symbol == "AAPL"
    assert o.side == "buy"
    # 33.333333333
    assert abs(o.qty - 33.333333333) < 1e-6
    assert o.estimated_price == 150.0

def test_planner_sell_all(mock_exec_config, mock_price_cache):
    planner = RebalancePlanner(mock_exec_config, mock_price_cache)
    
    # Current: 10 AAPL. Target: 0.
    pos = AlpacaPosition("AAPL", 10.0, 1500.0, 140.0, 150.0, 10.0, 100.0, "long")
    
    plan = planner.plan(
        target_weights={"AAPL": 0.0},
        current_positions={"AAPL": pos},
        equity=10000.0
    )
    
    assert len(plan.orders) == 1
    o = plan.orders[0]
    assert o.side == "sell"
    assert o.qty == 10.0

def test_planner_min_notional_skip(mock_price_cache):
    # Specific config for this test
    config = ExecutionConfig(
        min_trade_notional=10.0,
        min_trade_shares=0.0, # Bypass shares check
        allow_fractional_shares=True
    )
    planner = RebalancePlanner(config, mock_price_cache)
    
    # Target: Tiny AAPL. Equity 1000. 0.1% -> $1. Notional < $10 min.
    
    plan = planner.plan(
        target_weights={"AAPL": 0.001},
        current_positions={},
        equity=1000.0
    )
    
    assert len(plan.orders) == 0
    assert len(plan.skipped) == 1
    assert "Notional" in plan.skipped[0][1]

def test_order_factory_id(mock_exec_config):
    factory = OrderFactory(mock_exec_config)
    po = PlannedOrder("AAPL", "buy", 10.0, 150.0, 1500.0)
    
    payload = factory.create_order_payload(po, "strat1", "run1")
    
    assert payload["symbol"] == "AAPL"
    assert payload["qty"] == 10.0
    assert payload["side"] == "buy"
    # "strat1|run1|AAPL|buy" length is short, should be exact
    assert payload["client_order_id"] == "strat1|run1|AAPL|buy"

@pytest.mark.asyncio
async def test_execution_service_flow():
    config = MagicMock()
    config.execution = ExecutionConfig()
    client = MagicMock()
    state_mgr = MagicMock()
    price_cache = MagicMock()
    
    state_mgr.state.positions = {}
    state_mgr.state.account.equity = 10000.0
    price_cache.get_price.return_value = 100.0 # All prices 100
    
    service = ExecutionService(config, client, state_mgr, price_cache)
    
    # Target 10% AAPL = $1000 = 10 shares
    res = await service.rebalance_to_target_weights(
        {"AAPL": 0.1}, "s1", "r1", wait_for_fills=False
    )
    
    assert res["status"] == "submitted"
    assert res["submitted_count"] == 1
    client.submit_order.assert_called_once()
