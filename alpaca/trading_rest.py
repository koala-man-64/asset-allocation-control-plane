from datetime import datetime
from typing import Any

from alpaca.config import AlpacaConfig
from alpaca.models import AlpacaAccount, AlpacaOrder, AlpacaPosition
from alpaca.transport_http import AlpacaHttpTransport


class AlpacaTradingClient:
    def __init__(self, config: AlpacaConfig) -> None:
        self._config = config
        self._transport = AlpacaHttpTransport(config)

    def close(self) -> None:
        self._transport.close()

    def get_account(self) -> AlpacaAccount:
        data = self._transport.get("/v2/account")
        return AlpacaAccount.from_api_dict(data)

    def list_positions(self) -> list[AlpacaPosition]:
        data = self._transport.get("/v2/positions")
        return [AlpacaPosition.from_api_dict(position) for position in data]

    def list_orders(
        self,
        status: str = "open",
        limit: int = 500,
        after: datetime | None = None,
        until: datetime | None = None,
        nested: bool = False,
        symbols: list[str] | None = None,
    ) -> list[AlpacaOrder]:
        params: dict[str, Any] = {
            "status": status,
            "limit": limit,
            "nested": nested,
        }
        if after:
            params["after"] = after.isoformat()
        if until:
            params["until"] = until.isoformat()
        if symbols:
            params["symbols"] = ",".join(symbols)

        data = self._transport.get("/v2/orders", params=params)
        return [AlpacaOrder.from_api_dict(order) for order in data]

    def get_order(self, order_id: str) -> AlpacaOrder:
        data = self._transport.get(f"/v2/orders/{order_id}")
        return AlpacaOrder.from_api_dict(data)

    def get_order_by_client_order_id(self, client_order_id: str) -> AlpacaOrder:
        data = self._transport.get("/v2/orders:by_client_order_id", params={"client_order_id": client_order_id})
        return AlpacaOrder.from_api_dict(data)

    def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        type: str = "market",
        time_in_force: str = "day",
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
    ) -> AlpacaOrder:
        payload: dict[str, Any] = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": type,
            "time_in_force": time_in_force,
        }
        if limit_price is not None:
            payload["limit_price"] = str(limit_price)
        if stop_price is not None:
            payload["stop_price"] = str(stop_price)
        if client_order_id:
            payload["client_order_id"] = client_order_id

        data = self._transport.post("/v2/orders", json_data=payload)
        return AlpacaOrder.from_api_dict(data)

    def replace_order(
        self,
        order_id: str,
        qty: float | None = None,
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
    ) -> AlpacaOrder:
        payload: dict[str, Any] = {}
        if qty is not None:
            payload["qty"] = str(qty)
        if limit_price is not None:
            payload["limit_price"] = str(limit_price)
        if stop_price is not None:
            payload["stop_price"] = str(stop_price)
        if client_order_id is not None:
            payload["client_order_id"] = client_order_id

        data = self._transport.patch(f"/v2/orders/{order_id}", json_data=payload)
        return AlpacaOrder.from_api_dict(data)

    def cancel_order(self, order_id: str) -> None:
        self._transport.delete(f"/v2/orders/{order_id}")

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        return self._transport.delete("/v2/orders")
