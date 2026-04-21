from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from alpaca.config import AlpacaConfig, AlpacaEnvironmentConfig
from alpaca.errors import AlpacaInvalidResponseError
from alpaca.models import AlpacaAccount, AlpacaOrder, AlpacaPosition
from alpaca.transport_http import AlpacaHttpTransport

_ConfigLike = AlpacaConfig | AlpacaEnvironmentConfig


def _serialize_payload(data: Any) -> Any:
    if is_dataclass(data):
        return asdict(data)
    if isinstance(data, list):
        return [_serialize_payload(item) for item in data]
    if isinstance(data, dict):
        return {
            key: _serialize_payload(value)
            for key, value in data.items()
        }
    return data


class AlpacaTradingClient:
    def __init__(self, config: _ConfigLike) -> None:
        self._config = config
        self._transport = AlpacaHttpTransport(config)

    @property
    def last_request_id(self) -> str | None:
        return self._transport.last_request_id

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "AlpacaTradingClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.close()

    def _parse_account(self, data: Any) -> AlpacaAccount:
        try:
            if not isinstance(data, dict):
                raise TypeError("expected account payload object")
            return AlpacaAccount.from_api_dict(data)
        except Exception as exc:
            raise AlpacaInvalidResponseError(
                "Alpaca account response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def _parse_order(self, data: Any) -> AlpacaOrder:
        try:
            if not isinstance(data, dict):
                raise TypeError("expected order payload object")
            return AlpacaOrder.from_api_dict(data)
        except Exception as exc:
            raise AlpacaInvalidResponseError(
                "Alpaca order response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def _parse_positions(self, data: Any) -> list[AlpacaPosition]:
        if not isinstance(data, list):
            raise AlpacaInvalidResponseError(
                "Alpaca positions response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        try:
            return [AlpacaPosition.from_api_dict(position) for position in data]
        except Exception as exc:
            raise AlpacaInvalidResponseError(
                "Alpaca positions response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def _parse_orders(self, data: Any) -> list[AlpacaOrder]:
        if not isinstance(data, list):
            raise AlpacaInvalidResponseError(
                "Alpaca orders response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        try:
            return [AlpacaOrder.from_api_dict(order) for order in data]
        except Exception as exc:
            raise AlpacaInvalidResponseError(
                "Alpaca orders response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def get_account(self) -> AlpacaAccount:
        data = self._transport.get("/v2/account")
        return self._parse_account(data)

    def list_positions(self) -> list[AlpacaPosition]:
        data = self._transport.get("/v2/positions")
        return self._parse_positions(data)

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
        return self._parse_orders(data)

    def get_order(self, order_id: str) -> AlpacaOrder:
        data = self._transport.get(f"/v2/orders/{order_id}")
        return self._parse_order(data)

    def get_order_by_client_order_id(self, client_order_id: str) -> AlpacaOrder:
        data = self._transport.get("/v2/orders:by_client_order_id", params={"client_order_id": client_order_id})
        return self._parse_order(data)

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
        return self._parse_order(data)

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
        return self._parse_order(data)

    def cancel_order(self, order_id: str) -> None:
        self._transport.delete(f"/v2/orders/{order_id}")

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        data = self._transport.delete("/v2/orders")
        if not isinstance(data, list):
            raise AlpacaInvalidResponseError(
                "Alpaca cancel-all response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        return data
