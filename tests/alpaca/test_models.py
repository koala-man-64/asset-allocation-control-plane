from datetime import timezone
from alpaca.models import AlpacaAccount, AlpacaOrder

def test_alpaca_account_parsing():
    data = {
        "id": "acc_123",
        "account_number": "PA123",
        "status": "ACTIVE",
        "currency": "USD",
        "cash": "10000.50",
        "equity": "10000.50",
        "buying_power": "40000.00",
        "created_at": "2023-01-01T12:00:00Z"
    }
    acc = AlpacaAccount.from_api_dict(data)
    assert acc.cash == 10000.5
    assert acc.created_at.year == 2023
    assert acc.created_at.tzinfo == timezone.utc

def test_alpaca_order_parsing():
    data = {
        "id": "ord_1",
        "client_order_id": "my_id",
        "symbol": "AAPL",
        "created_at": "2023-01-01T10:00:00Z",
        "updated_at": "2023-01-01T10:00:00Z",
        "submitted_at": "2023-01-01T10:00:00Z",
        "asset_id": "as_1",
        "asset_class": "us_equity",
        "qty": "10",
        "filled_qty": "0",
        "type": "market",
        "side": "buy",
        "time_in_force": "day",
        "limit_price": "150.00",
        "stop_price": None,
        "status": "new"
    }
    order = AlpacaOrder.from_api_dict(data)
    assert order.qty == 10.0
    assert order.limit_price == 150.0
    assert order.stop_price is None
