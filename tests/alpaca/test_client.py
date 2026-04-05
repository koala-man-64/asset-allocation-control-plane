import pytest
from unittest.mock import patch
from alpaca.config import AlpacaConfig, HttpConfig
from alpaca.trading_rest import AlpacaTradingClient

@pytest.fixture
def mock_config():
    return AlpacaConfig(
        env="paper",
        api_key_env="K",
        api_secret_env="S",
        http=HttpConfig()
    )

@pytest.fixture
def client(mock_config):
    with patch("alpaca.trading_rest.AlpacaHttpTransport") as mock_transport_cls:
        mock_transport = mock_transport_cls.return_value
        client = AlpacaTradingClient(mock_config)
        client._transport = mock_transport # Swap the instance
        yield client

def test_get_account(client):
    client._transport.get.return_value = {
        "id": "1", "account_number": "A", "status": "ACTIVE", "currency": "USD",
        "cash": "100", "equity": "100", "buying_power": "200", "created_at": "2023-01-01T00:00:00Z"
    }
    acc = client.get_account()
    assert acc.id == "1"
    client._transport.get.assert_called_with("/v2/account")

def test_submit_order(client):
    client._transport.post.return_value = {
        "id": "o1", "client_order_id": "c1", "symbol": "AAPL", "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z", "submitted_at": "2023-01-01T00:00:00Z",
        "asset_id": "a1", "asset_class": "us_equity", "type": "market", "side": "buy",
        "time_in_force": "day", "status": "new", "qty": "10", "filled_qty": "0"
    }
    
    order = client.submit_order(symbol="AAPL", qty=10, side="buy")
    
    assert order.id == "o1"
    client._transport.post.assert_called_once()
    args, kwargs = client._transport.post.call_args
    assert args[0] == "/v2/orders"
    assert kwargs["json_data"]["symbol"] == "AAPL"
    assert kwargs["json_data"]["qty"] == "10"
    assert kwargs["json_data"]["side"] == "buy"

def test_replace_order(client):
    client._transport.patch.return_value = {
        "id": "o1", "client_order_id": "c1", "symbol": "AAPL", "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z", "submitted_at": "2023-01-01T00:00:00Z",
        "asset_id": "a1", "asset_class": "us_equity", "type": "market", "side": "buy",
        "time_in_force": "day", "status": "new", "qty": "5", "filled_qty": "0"
    }
    
    client.replace_order("o1", qty=5)
    
    client._transport.patch.assert_called_once()
    args, kwargs = client._transport.patch.call_args
    assert args[0] == "/v2/orders/o1"
    assert kwargs["json_data"]["qty"] == "5"
