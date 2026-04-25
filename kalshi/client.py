from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal
from typing import Any

from kalshi.config import KalshiEnvironmentConfig
from kalshi.errors import KalshiInvalidResponseError
from kalshi.models import (
    KalshiAccountLimits,
    KalshiAmendOrderResult,
    KalshiBalance,
    KalshiCancelOrderResult,
    KalshiMarket,
    KalshiMarketsPage,
    KalshiOrder,
    KalshiOrderQueuePositionResponse,
    KalshiOrderbook,
    KalshiOrdersPage,
    KalshiPositionsPage,
    KalshiQueuePosition,
    KalshiQueuePositionsResponse,
    KalshiEventPosition,
    KalshiMarketPosition,
    serialize_json,
)
from kalshi.transport_http import KalshiHttpTransport


def _serialize_payload(data: Any) -> Any:
    if is_dataclass(data):
        return asdict(data)
    if isinstance(data, list):
        return [_serialize_payload(item) for item in data]
    if isinstance(data, dict):
        return {key: _serialize_payload(value) for key, value in data.items()}
    return data


def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in params.items() if value is not None and value != ""}


def _clean_body(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


class KalshiTradingClient:
    def __init__(self, config: KalshiEnvironmentConfig) -> None:
        self._config = config
        self._transport = KalshiHttpTransport(config)

    @property
    def last_request_id(self) -> str | None:
        return self._transport.last_request_id

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "KalshiTradingClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.close()

    def _parse_market(self, data: Any) -> KalshiMarket:
        try:
            if not isinstance(data, dict):
                raise TypeError("expected market payload object")
            return KalshiMarket.from_api_dict(data)
        except Exception as exc:
            raise KalshiInvalidResponseError(
                "Kalshi market response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def _parse_order(self, data: Any) -> KalshiOrder:
        try:
            if not isinstance(data, dict):
                raise TypeError("expected order payload object")
            return KalshiOrder.from_api_dict(data)
        except Exception as exc:
            raise KalshiInvalidResponseError(
                "Kalshi order response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def list_markets(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        status: str | None = None,
        tickers: str | None = None,
        min_close_ts: int | None = None,
        max_close_ts: int | None = None,
        min_updated_ts: int | None = None,
        mve_filter: str | None = None,
    ) -> KalshiMarketsPage:
        data = self._transport.get(
            "/markets",
            params=_clean_params(
                {
                    "limit": limit,
                    "cursor": cursor,
                    "event_ticker": event_ticker,
                    "series_ticker": series_ticker,
                    "status": status,
                    "tickers": tickers,
                    "min_close_ts": min_close_ts,
                    "max_close_ts": max_close_ts,
                    "min_updated_ts": min_updated_ts,
                    "mve_filter": mve_filter,
                }
            ),
            authenticated=False,
        )
        if not isinstance(data, dict) or not isinstance(data.get("markets"), list):
            raise KalshiInvalidResponseError("Kalshi markets response was invalid.", payload={"body": _serialize_payload(data)})
        try:
            return KalshiMarketsPage(
                markets=[KalshiMarket.from_api_dict(item) for item in data["markets"] if isinstance(item, dict)],
                cursor=str(data.get("cursor") or "") or None,
            )
        except Exception as exc:
            raise KalshiInvalidResponseError("Kalshi markets response was invalid.", payload={"body": _serialize_payload(data)}) from exc

    def get_market(self, ticker: str) -> KalshiMarket:
        data = self._transport.get(f"/markets/{ticker}", authenticated=False)
        if not isinstance(data, dict) or not isinstance(data.get("market"), dict):
            raise KalshiInvalidResponseError("Kalshi market response was invalid.", payload={"body": _serialize_payload(data)})
        return self._parse_market(data["market"])

    def get_orderbook(self, ticker: str, *, depth: int = 0) -> KalshiOrderbook:
        data = self._transport.get(
            f"/markets/{ticker}/orderbook",
            params=_clean_params({"depth": depth}),
            authenticated=False,
        )
        if not isinstance(data, dict) or not isinstance(data.get("orderbook_fp"), dict):
            raise KalshiInvalidResponseError(
                "Kalshi orderbook response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        try:
            return KalshiOrderbook.from_api_dict(data["orderbook_fp"])
        except Exception as exc:
            raise KalshiInvalidResponseError(
                "Kalshi orderbook response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def get_balance(self, *, subaccount: int = 0) -> KalshiBalance:
        data = self._transport.get(
            "/portfolio/balance",
            params=_clean_params({"subaccount": subaccount}),
            authenticated=True,
        )
        if not isinstance(data, dict):
            raise KalshiInvalidResponseError("Kalshi balance response was invalid.", payload={"body": _serialize_payload(data)})
        try:
            return KalshiBalance.from_api_dict(data)
        except Exception as exc:
            raise KalshiInvalidResponseError("Kalshi balance response was invalid.", payload={"body": _serialize_payload(data)}) from exc

    def list_positions(
        self,
        *,
        cursor: str | None = None,
        limit: int = 100,
        count_filter: str | None = None,
        ticker: str | None = None,
        event_ticker: str | None = None,
        subaccount: int = 0,
    ) -> KalshiPositionsPage:
        data = self._transport.get(
            "/portfolio/positions",
            params=_clean_params(
                {
                    "cursor": cursor,
                    "limit": limit,
                    "count_filter": count_filter,
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "subaccount": subaccount,
                }
            ),
            authenticated=True,
        )
        if not isinstance(data, dict):
            raise KalshiInvalidResponseError("Kalshi positions response was invalid.", payload={"body": _serialize_payload(data)})
        market_positions = data.get("market_positions")
        event_positions = data.get("event_positions")
        if not isinstance(market_positions, list) or not isinstance(event_positions, list):
            raise KalshiInvalidResponseError("Kalshi positions response was invalid.", payload={"body": _serialize_payload(data)})
        try:
            return KalshiPositionsPage(
                market_positions=[KalshiMarketPosition.from_api_dict(item) for item in market_positions if isinstance(item, dict)],
                event_positions=[KalshiEventPosition.from_api_dict(item) for item in event_positions if isinstance(item, dict)],
                cursor=str(data.get("cursor") or "") or None,
            )
        except Exception as exc:
            raise KalshiInvalidResponseError("Kalshi positions response was invalid.", payload={"body": _serialize_payload(data)}) from exc

    def list_orders(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        status: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
        subaccount: int | None = None,
    ) -> KalshiOrdersPage:
        data = self._transport.get(
            "/portfolio/orders",
            params=_clean_params(
                {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "min_ts": min_ts,
                    "max_ts": max_ts,
                    "status": status,
                    "limit": limit,
                    "cursor": cursor,
                    "subaccount": subaccount,
                }
            ),
            authenticated=True,
        )
        if not isinstance(data, dict) or not isinstance(data.get("orders"), list):
            raise KalshiInvalidResponseError("Kalshi orders response was invalid.", payload={"body": _serialize_payload(data)})
        try:
            return KalshiOrdersPage(
                orders=[KalshiOrder.from_api_dict(item) for item in data["orders"] if isinstance(item, dict)],
                cursor=str(data.get("cursor") or "") or None,
            )
        except Exception as exc:
            raise KalshiInvalidResponseError("Kalshi orders response was invalid.", payload={"body": _serialize_payload(data)}) from exc

    def get_order(self, order_id: str) -> KalshiOrder:
        data = self._transport.get(f"/portfolio/orders/{order_id}", authenticated=True)
        if not isinstance(data, dict) or not isinstance(data.get("order"), dict):
            raise KalshiInvalidResponseError("Kalshi order response was invalid.", payload={"body": _serialize_payload(data)})
        return self._parse_order(data["order"])

    def get_order_queue_position(self, order_id: str) -> KalshiOrderQueuePositionResponse:
        data = self._transport.get(f"/portfolio/orders/{order_id}/queue_position", authenticated=True)
        if not isinstance(data, dict) or ("queue_position_fp" not in data and "queue_position" not in data):
            raise KalshiInvalidResponseError(
                "Kalshi order queue position response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        return KalshiOrderQueuePositionResponse(
            queue_position_fp=Decimal(str(data.get("queue_position_fp", data.get("queue_position"))))
        )

    def get_queue_positions(
        self,
        *,
        market_tickers: str | None = None,
        event_ticker: str | None = None,
        subaccount: int = 0,
    ) -> KalshiQueuePositionsResponse:
        data = self._transport.get(
            "/portfolio/orders/queue_positions",
            params=_clean_params(
                {
                    "market_tickers": market_tickers,
                    "event_ticker": event_ticker,
                    "subaccount": subaccount,
                }
            ),
            authenticated=True,
        )
        if not isinstance(data, dict) or not isinstance(data.get("queue_positions"), list):
            raise KalshiInvalidResponseError(
                "Kalshi queue positions response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        try:
            return KalshiQueuePositionsResponse(
                queue_positions=[
                    KalshiQueuePosition.from_api_dict(item)
                    for item in data["queue_positions"]
                    if isinstance(item, dict)
                ]
            )
        except Exception as exc:
            raise KalshiInvalidResponseError(
                "Kalshi queue positions response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def get_account_limits(self) -> KalshiAccountLimits:
        data = self._transport.get("/account/limits", authenticated=True)
        if not isinstance(data, dict):
            raise KalshiInvalidResponseError(
                "Kalshi account limits response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        try:
            return KalshiAccountLimits.from_api_dict(data)
        except Exception as exc:
            raise KalshiInvalidResponseError(
                "Kalshi account limits response was invalid.",
                payload={"body": _serialize_payload(data)},
            ) from exc

    def create_order(self, order: dict[str, Any]) -> KalshiOrder:
        data = self._transport.post("/portfolio/orders", json_data=_clean_body(order), authenticated=True)
        if not isinstance(data, dict) or not isinstance(data.get("order"), dict):
            raise KalshiInvalidResponseError(
                "Kalshi create-order response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        return self._parse_order(data["order"])

    def cancel_order(self, order_id: str, *, subaccount: int | None = None) -> KalshiCancelOrderResult:
        data = self._transport.delete(
            f"/portfolio/orders/{order_id}",
            params=_clean_params({"subaccount": subaccount}),
            authenticated=True,
        )
        if not isinstance(data, dict) or not isinstance(data.get("order"), dict) or "reduced_by_fp" not in data:
            raise KalshiInvalidResponseError(
                "Kalshi cancel-order response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        return KalshiCancelOrderResult(
            order=self._parse_order(data["order"]),
            reduced_by_fp=Decimal(str(data["reduced_by_fp"])),
        )

    def amend_order(self, order_id: str, order: dict[str, Any]) -> KalshiAmendOrderResult:
        data = self._transport.post(
            f"/portfolio/orders/{order_id}/amend",
            json_data=_clean_body(order),
            authenticated=True,
        )
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("old_order"), dict)
            or not isinstance(data.get("order"), dict)
        ):
            raise KalshiInvalidResponseError(
                "Kalshi amend-order response was invalid.",
                payload={"body": _serialize_payload(data)},
            )
        return KalshiAmendOrderResult(
            old_order=self._parse_order(data["old_order"]),
            order=self._parse_order(data["order"]),
        )


def serialize_payload(payload: Any) -> Any:
    return serialize_json(payload)
