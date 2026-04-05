import pytest
from unittest.mock import MagicMock
from alpaca.reconciler import Reconciler

@pytest.mark.asyncio
async def test_reconciler_bootstrap():
    client = MagicMock()
    state_mgr = MagicMock()
    config = MagicMock()
    
    reconciler = Reconciler(config, client, state_mgr)
    
    client.get_account.return_value = "ACC"
    client.list_positions.return_value = ["POS"]
    client.list_orders.return_value = ["ORD"]
    
    await reconciler.bootstrap()
    
    state_mgr.update_account.assert_called_with("ACC")
    state_mgr.update_positions.assert_called_with(["POS"])
    state_mgr.update_open_orders.assert_called_with(["ORD"])

@pytest.mark.asyncio
async def test_reconciler_sync_cycle():
    client = MagicMock()
    state_mgr = MagicMock()
    config = MagicMock()
    
    reconciler = Reconciler(config, client, state_mgr)
    
    client.list_orders.return_value = ["ORD_SYNC"]
    client.list_positions.return_value = ["POS_SYNC"]
    client.get_account.return_value = "ACC_SYNC"
    
    await reconciler._sync_cycle()
    
    state_mgr.update_open_orders.assert_called_with(["ORD_SYNC"])
    state_mgr.update_positions.assert_called_with(["POS_SYNC"])
    state_mgr.update_account.assert_called_with("ACC_SYNC")
