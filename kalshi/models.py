from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _bool_or_none(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return bool(value)


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def serialize_json(value: Any) -> Any:
    if is_dataclass(value):
        return {
            field.name: serialize_json(getattr(value, field.name))
            for field in fields(value)
            if getattr(value, field.name) is not None
        }
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    if isinstance(value, list):
        return [serialize_json(item) for item in value]
    if isinstance(value, tuple):
        return [serialize_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): serialize_json(item) for key, item in value.items() if item is not None}
    return value


class SerializableModel:
    def to_api_dict(self) -> dict[str, Any]:
        return serialize_json(self)


from dataclasses import dataclass, field


@dataclass(frozen=True)
class KalshiPriceRange(SerializableModel):
    start: Decimal
    end: Decimal
    step: Decimal

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiPriceRange":
        return KalshiPriceRange(
            start=Decimal(str(data["start"])),
            end=Decimal(str(data["end"])),
            step=Decimal(str(data["step"])),
        )


@dataclass(frozen=True)
class KalshiMarketLeg(SerializableModel):
    event_ticker: Optional[str]
    market_ticker: str
    side: Optional[str]
    yes_settlement_value_dollars: Decimal | None

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiMarketLeg":
        return KalshiMarketLeg(
            event_ticker=str(data.get("event_ticker") or "") or None,
            market_ticker=str(data["market_ticker"]),
            side=str(data.get("side") or "") or None,
            yes_settlement_value_dollars=_decimal_or_none(data.get("yes_settlement_value_dollars")),
        )


@dataclass(frozen=True)
class KalshiMarket(SerializableModel):
    ticker: str
    event_ticker: str
    market_type: Optional[str] = None
    yes_sub_title: Optional[str] = None
    no_sub_title: Optional[str] = None
    created_time: datetime | None = None
    updated_time: datetime | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    latest_expiration_time: datetime | None = None
    settlement_timer_seconds: int | None = None
    status: Optional[str] = None
    yes_bid_dollars: Decimal | None = None
    yes_bid_size_fp: Decimal | None = None
    yes_ask_dollars: Decimal | None = None
    yes_ask_size_fp: Decimal | None = None
    no_bid_dollars: Decimal | None = None
    no_ask_dollars: Decimal | None = None
    last_price_dollars: Decimal | None = None
    volume_fp: Decimal | None = None
    volume_24h_fp: Decimal | None = None
    result: Optional[str] = None
    can_close_early: bool | None = None
    fractional_trading_enabled: bool | None = None
    open_interest_fp: Decimal | None = None
    notional_value_dollars: Decimal | None = None
    previous_yes_bid_dollars: Decimal | None = None
    previous_yes_ask_dollars: Decimal | None = None
    previous_price_dollars: Decimal | None = None
    liquidity_dollars: Decimal | None = None
    expiration_value: Optional[str] = None
    rules_primary: Optional[str] = None
    rules_secondary: Optional[str] = None
    price_level_structure: Optional[str] = None
    price_ranges: list[KalshiPriceRange] = field(default_factory=list)
    title: Optional[str] = None
    subtitle: Optional[str] = None
    expected_expiration_time: datetime | None = None
    expiration_time: datetime | None = None
    response_price_units: Optional[str] = None
    settlement_value_dollars: Decimal | None = None
    settlement_ts: int | None = None
    occurrence_datetime: datetime | None = None
    fee_waiver_expiration_time: datetime | None = None
    early_close_condition: Optional[str] = None
    tick_size: int | None = None
    strike_type: Optional[str] = None
    floor_strike: int | None = None
    cap_strike: int | None = None
    functional_strike: Optional[str] = None
    custom_strike: dict[str, Any] | None = None
    mve_collection_ticker: Optional[str] = None
    mve_selected_legs: list[KalshiMarketLeg] = field(default_factory=list)
    primary_participant_key: Optional[str] = None
    is_provisional: bool | None = None

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiMarket":
        price_ranges = data.get("price_ranges") or []
        legs = data.get("mve_selected_legs") or []
        return KalshiMarket(
            ticker=str(data["ticker"]),
            event_ticker=str(data["event_ticker"]),
            market_type=str(data.get("market_type") or "") or None,
            yes_sub_title=str(data.get("yes_sub_title") or "") or None,
            no_sub_title=str(data.get("no_sub_title") or "") or None,
            created_time=_datetime_or_none(data.get("created_time")),
            updated_time=_datetime_or_none(data.get("updated_time")),
            open_time=_datetime_or_none(data.get("open_time")),
            close_time=_datetime_or_none(data.get("close_time")),
            latest_expiration_time=_datetime_or_none(data.get("latest_expiration_time")),
            settlement_timer_seconds=_int_or_none(data.get("settlement_timer_seconds")),
            status=str(data.get("status") or "") or None,
            yes_bid_dollars=_decimal_or_none(data.get("yes_bid_dollars")),
            yes_bid_size_fp=_decimal_or_none(data.get("yes_bid_size_fp")),
            yes_ask_dollars=_decimal_or_none(data.get("yes_ask_dollars")),
            yes_ask_size_fp=_decimal_or_none(data.get("yes_ask_size_fp")),
            no_bid_dollars=_decimal_or_none(data.get("no_bid_dollars")),
            no_ask_dollars=_decimal_or_none(data.get("no_ask_dollars")),
            last_price_dollars=_decimal_or_none(data.get("last_price_dollars")),
            volume_fp=_decimal_or_none(data.get("volume_fp")),
            volume_24h_fp=_decimal_or_none(data.get("volume_24h_fp")),
            result=str(data.get("result") or "") or None,
            can_close_early=_bool_or_none(data.get("can_close_early")),
            fractional_trading_enabled=_bool_or_none(data.get("fractional_trading_enabled")),
            open_interest_fp=_decimal_or_none(data.get("open_interest_fp")),
            notional_value_dollars=_decimal_or_none(data.get("notional_value_dollars")),
            previous_yes_bid_dollars=_decimal_or_none(data.get("previous_yes_bid_dollars")),
            previous_yes_ask_dollars=_decimal_or_none(data.get("previous_yes_ask_dollars")),
            previous_price_dollars=_decimal_or_none(data.get("previous_price_dollars")),
            liquidity_dollars=_decimal_or_none(data.get("liquidity_dollars")),
            expiration_value=str(data.get("expiration_value") or "") or None,
            rules_primary=str(data.get("rules_primary") or "") or None,
            rules_secondary=str(data.get("rules_secondary") or "") or None,
            price_level_structure=str(data.get("price_level_structure") or "") or None,
            price_ranges=[KalshiPriceRange.from_api_dict(item) for item in price_ranges if isinstance(item, dict)],
            title=str(data.get("title") or "") or None,
            subtitle=str(data.get("subtitle") or "") or None,
            expected_expiration_time=_datetime_or_none(data.get("expected_expiration_time")),
            expiration_time=_datetime_or_none(data.get("expiration_time")),
            response_price_units=str(data.get("response_price_units") or "") or None,
            settlement_value_dollars=_decimal_or_none(data.get("settlement_value_dollars")),
            settlement_ts=_int_or_none(data.get("settlement_ts")),
            occurrence_datetime=_datetime_or_none(data.get("occurrence_datetime")),
            fee_waiver_expiration_time=_datetime_or_none(data.get("fee_waiver_expiration_time")),
            early_close_condition=str(data.get("early_close_condition") or "") or None,
            tick_size=_int_or_none(data.get("tick_size")),
            strike_type=str(data.get("strike_type") or "") or None,
            floor_strike=_int_or_none(data.get("floor_strike")),
            cap_strike=_int_or_none(data.get("cap_strike")),
            functional_strike=str(data.get("functional_strike") or "") or None,
            custom_strike=data.get("custom_strike") if isinstance(data.get("custom_strike"), dict) else None,
            mve_collection_ticker=str(data.get("mve_collection_ticker") or "") or None,
            mve_selected_legs=[KalshiMarketLeg.from_api_dict(item) for item in legs if isinstance(item, dict)],
            primary_participant_key=str(data.get("primary_participant_key") or "") or None,
            is_provisional=_bool_or_none(data.get("is_provisional")),
        )


@dataclass(frozen=True)
class KalshiMarketsPage(SerializableModel):
    markets: list[KalshiMarket]
    cursor: Optional[str] = None


@dataclass(frozen=True)
class KalshiOrderbookLevel(SerializableModel):
    price_dollars: Decimal
    count_fp: Decimal

    @staticmethod
    def from_api_list(data: list[Any]) -> "KalshiOrderbookLevel":
        if len(data) != 2:
            raise ValueError("Kalshi orderbook level must contain price and count.")
        return KalshiOrderbookLevel(price_dollars=Decimal(str(data[0])), count_fp=Decimal(str(data[1])))


@dataclass(frozen=True)
class KalshiOrderbook(SerializableModel):
    yes_dollars: list[KalshiOrderbookLevel] = field(default_factory=list)
    no_dollars: list[KalshiOrderbookLevel] = field(default_factory=list)

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiOrderbook":
        return KalshiOrderbook(
            yes_dollars=[
                KalshiOrderbookLevel.from_api_list(item)
                for item in (data.get("yes_dollars") or [])
                if isinstance(item, list)
            ],
            no_dollars=[
                KalshiOrderbookLevel.from_api_list(item)
                for item in (data.get("no_dollars") or [])
                if isinstance(item, list)
            ],
        )


@dataclass(frozen=True)
class KalshiBalance(SerializableModel):
    balance: int
    portfolio_value: int
    updated_ts: int

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiBalance":
        return KalshiBalance(
            balance=int(data["balance"]),
            portfolio_value=int(data["portfolio_value"]),
            updated_ts=int(data["updated_ts"]),
        )


@dataclass(frozen=True)
class KalshiMarketPosition(SerializableModel):
    ticker: str
    total_traded_dollars: Decimal | None = None
    position_fp: Decimal | None = None
    market_exposure_dollars: Decimal | None = None
    realized_pnl_dollars: Decimal | None = None
    resting_orders_count: int | None = None
    fees_paid_dollars: Decimal | None = None
    last_updated_ts: int | None = None

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiMarketPosition":
        return KalshiMarketPosition(
            ticker=str(data["ticker"]),
            total_traded_dollars=_decimal_or_none(data.get("total_traded_dollars")),
            position_fp=_decimal_or_none(data.get("position_fp")),
            market_exposure_dollars=_decimal_or_none(data.get("market_exposure_dollars")),
            realized_pnl_dollars=_decimal_or_none(data.get("realized_pnl_dollars")),
            resting_orders_count=_int_or_none(data.get("resting_orders_count")),
            fees_paid_dollars=_decimal_or_none(data.get("fees_paid_dollars")),
            last_updated_ts=_int_or_none(data.get("last_updated_ts")),
        )


@dataclass(frozen=True)
class KalshiEventPosition(SerializableModel):
    event_ticker: str
    total_cost_dollars: Decimal | None = None
    total_cost_shares_fp: Decimal | None = None
    event_exposure_dollars: Decimal | None = None
    realized_pnl_dollars: Decimal | None = None
    fees_paid_dollars: Decimal | None = None

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiEventPosition":
        return KalshiEventPosition(
            event_ticker=str(data["event_ticker"]),
            total_cost_dollars=_decimal_or_none(data.get("total_cost_dollars")),
            total_cost_shares_fp=_decimal_or_none(data.get("total_cost_shares_fp")),
            event_exposure_dollars=_decimal_or_none(data.get("event_exposure_dollars")),
            realized_pnl_dollars=_decimal_or_none(data.get("realized_pnl_dollars")),
            fees_paid_dollars=_decimal_or_none(data.get("fees_paid_dollars")),
        )


@dataclass(frozen=True)
class KalshiPositionsPage(SerializableModel):
    market_positions: list[KalshiMarketPosition] = field(default_factory=list)
    event_positions: list[KalshiEventPosition] = field(default_factory=list)
    cursor: Optional[str] = None


@dataclass(frozen=True)
class KalshiOrder(SerializableModel):
    order_id: str
    user_id: Optional[str]
    client_order_id: Optional[str]
    ticker: str
    side: str
    action: str
    type: Optional[str] = None
    status: Optional[str] = None
    yes_price_dollars: Decimal | None = None
    no_price_dollars: Decimal | None = None
    fill_count_fp: Decimal | None = None
    remaining_count_fp: Decimal | None = None
    initial_count_fp: Decimal | None = None
    taker_fill_cost_dollars: Decimal | None = None
    maker_fill_cost_dollars: Decimal | None = None
    taker_fees_dollars: Decimal | None = None
    maker_fees_dollars: Decimal | None = None
    expiration_time: datetime | None = None
    created_time: datetime | None = None
    last_update_time: datetime | None = None
    self_trade_prevention_type: Optional[str] = None
    order_group_id: Optional[str] = None
    cancel_order_on_pause: bool | None = None
    subaccount_number: int | None = None

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiOrder":
        return KalshiOrder(
            order_id=str(data["order_id"]),
            user_id=str(data.get("user_id") or "") or None,
            client_order_id=str(data.get("client_order_id") or "") or None,
            ticker=str(data["ticker"]),
            side=str(data["side"]),
            action=str(data["action"]),
            type=str(data.get("type") or "") or None,
            status=str(data.get("status") or "") or None,
            yes_price_dollars=_decimal_or_none(data.get("yes_price_dollars")),
            no_price_dollars=_decimal_or_none(data.get("no_price_dollars")),
            fill_count_fp=_decimal_or_none(data.get("fill_count_fp")),
            remaining_count_fp=_decimal_or_none(data.get("remaining_count_fp")),
            initial_count_fp=_decimal_or_none(data.get("initial_count_fp")),
            taker_fill_cost_dollars=_decimal_or_none(data.get("taker_fill_cost_dollars")),
            maker_fill_cost_dollars=_decimal_or_none(data.get("maker_fill_cost_dollars")),
            taker_fees_dollars=_decimal_or_none(data.get("taker_fees_dollars")),
            maker_fees_dollars=_decimal_or_none(data.get("maker_fees_dollars")),
            expiration_time=_datetime_or_none(data.get("expiration_time")),
            created_time=_datetime_or_none(data.get("created_time")),
            last_update_time=_datetime_or_none(data.get("last_update_time")),
            self_trade_prevention_type=str(data.get("self_trade_prevention_type") or "") or None,
            order_group_id=str(data.get("order_group_id") or "") or None,
            cancel_order_on_pause=_bool_or_none(data.get("cancel_order_on_pause")),
            subaccount_number=_int_or_none(data.get("subaccount_number")),
        )


@dataclass(frozen=True)
class KalshiOrdersPage(SerializableModel):
    orders: list[KalshiOrder] = field(default_factory=list)
    cursor: Optional[str] = None


@dataclass(frozen=True)
class KalshiCancelOrderResult(SerializableModel):
    order: KalshiOrder
    reduced_by_fp: Decimal


@dataclass(frozen=True)
class KalshiAmendOrderResult(SerializableModel):
    old_order: KalshiOrder
    order: KalshiOrder


@dataclass(frozen=True)
class KalshiQueuePosition(SerializableModel):
    order_id: str
    market_ticker: str
    queue_position_fp: Decimal

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiQueuePosition":
        return KalshiQueuePosition(
            order_id=str(data["order_id"]),
            market_ticker=str(data["market_ticker"]),
            queue_position_fp=Decimal(str(data.get("queue_position_fp", data.get("queue_position")))),
        )


@dataclass(frozen=True)
class KalshiQueuePositionsResponse(SerializableModel):
    queue_positions: list[KalshiQueuePosition] = field(default_factory=list)


@dataclass(frozen=True)
class KalshiOrderQueuePositionResponse(SerializableModel):
    queue_position_fp: Decimal


@dataclass(frozen=True)
class KalshiAccountLimits(SerializableModel):
    usage_tier: str
    read_limit: int
    write_limit: int

    @staticmethod
    def from_api_dict(data: dict[str, Any]) -> "KalshiAccountLimits":
        return KalshiAccountLimits(
            usage_tier=str(data["usage_tier"]),
            read_limit=int(data.get("read_limit", data.get("read_rate_limit"))),
            write_limit=int(data.get("write_limit", data.get("write_rate_limit"))),
        )
