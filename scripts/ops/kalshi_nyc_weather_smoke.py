from __future__ import annotations

import base64
import os
import sys
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


SERIES_TICKER = "KXHIGHNY"
ORDER_PRICE_CENTS = 1
TIMEOUT_SECONDS = 15
BASE_URLS = {
    "demo": "https://demo-api.kalshi.co/trade-api/v2",
    "prod": "https://api.elections.kalshi.com/trade-api/v2",
    "production": "https://api.elections.kalshi.com/trade-api/v2",
    "live": "https://api.elections.kalshi.com/trade-api/v2",
}


class SmokeError(RuntimeError):
    pass


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip().strip('"')
    if not value:
        raise SmokeError(f"Missing required environment variable: {name}")
    return value


def _base_url() -> str:
    env = os.environ.get("KALSHI_ENV", "demo").strip().lower() or "demo"
    base_url = BASE_URLS.get(env)
    if not base_url:
        raise SmokeError(f"Unsupported KALSHI_ENV={env!r}; use demo, prod, production, or live.")
    return base_url


def _request_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    response = session.request(method, url, timeout=TIMEOUT_SECONDS, **kwargs)
    if response.status_code >= 400:
        raise SmokeError(f"{method} {url} failed with HTTP {response.status_code}: {response.text[:500]}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise SmokeError(f"{method} {url} returned non-JSON response.") from exc
    if not isinstance(payload, dict):
        raise SmokeError(f"{method} {url} returned unexpected JSON payload.")
    return payload


def _load_private_key(path: str) -> Any:
    key_path = Path(path).expanduser()
    if not key_path.exists():
        raise SmokeError(f"KALSHI_PRIVATE_KEY_PATH does not exist: {key_path}")
    with key_path.open("rb") as handle:
        return serialization.load_pem_private_key(handle.read(), password=None)


def _signature(private_key: Any, timestamp: str, method: str, path: str) -> str:
    path_without_query = path.split("?", 1)[0]
    message = f"{timestamp}{method.upper()}{path_without_query}".encode("utf-8")
    signed = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signed).decode("utf-8")


def _auth_headers(private_key: Any, api_key_id: str, method: str, base_url: str, path: str) -> dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    sign_path = urlparse(base_url + path).path
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": _signature(private_key, timestamp, method, sign_path),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json",
    }


def _today_event_suffix() -> str:
    today = datetime.now(ZoneInfo("America/New_York")).date()
    return today.strftime("%y%b%d").upper()


def _market_date_matches_today(market: dict[str, Any], event_suffix: str) -> bool:
    event_ticker = str(market.get("event_ticker") or "")
    if event_ticker.endswith(event_suffix):
        return True
    occurrence = str(market.get("occurrence_datetime") or "")
    if not occurrence:
        return False
    try:
        occurrence_date = datetime.fromisoformat(occurrence.replace("Z", "+00:00")).astimezone(
            ZoneInfo("America/New_York")
        ).date()
    except ValueError:
        return False
    return occurrence_date == datetime.now(ZoneInfo("America/New_York")).date()


def _decimal_field(market: dict[str, Any], name: str) -> Decimal:
    raw = market.get(name)
    if raw is None or raw == "":
        return Decimal("0")
    return Decimal(str(raw))


def _select_market(markets: list[dict[str, Any]]) -> dict[str, Any]:
    event_suffix = _today_event_suffix()
    candidates = [
        market
        for market in markets
        if str(market.get("event_ticker") or "").startswith(f"{SERIES_TICKER}-")
        and _market_date_matches_today(market, event_suffix)
    ]
    if not candidates:
        raise SmokeError(f"No active {SERIES_TICKER} market found for today's NYC date.")

    for market in candidates:
        yes_ask = _decimal_field(market, "yes_ask_dollars")
        yes_ask_size = _decimal_field(market, "yes_ask_size_fp")
        if yes_ask_size == 0 or yes_ask > Decimal("0.0100"):
            return market

    raise SmokeError("No today's market can safely accept a non-marketable 1 cent post-only YES bid.")


def _get_markets(session: requests.Session, base_url: str) -> list[dict[str, Any]]:
    payload = _request_json(
        session,
        "GET",
        f"{base_url}/markets",
        params={"series_ticker": SERIES_TICKER, "status": "open", "limit": 100},
    )
    markets = payload.get("markets")
    if not isinstance(markets, list):
        raise SmokeError("Kalshi markets response did not include a markets array.")
    return [market for market in markets if isinstance(market, dict)]


def _get_orderbook(session: requests.Session, base_url: str, ticker: str) -> dict[str, Any]:
    payload = _request_json(session, "GET", f"{base_url}/markets/{ticker}/orderbook", params={"depth": 5})
    orderbook = payload.get("orderbook_fp")
    if not isinstance(orderbook, dict):
        raise SmokeError("Kalshi orderbook response did not include orderbook_fp.")
    return orderbook


def _post_order(
    session: requests.Session,
    base_url: str,
    private_key: Any,
    api_key_id: str,
    ticker: str,
) -> dict[str, Any]:
    path = "/portfolio/orders"
    body = {
        "ticker": ticker,
        "action": "buy",
        "side": "yes",
        "count": 1,
        "type": "limit",
        "yes_price": ORDER_PRICE_CENTS,
        "post_only": True,
        "client_order_id": f"kalshi-smoke-{uuid.uuid4()}",
    }
    payload = _request_json(
        session,
        "POST",
        base_url + path,
        headers=_auth_headers(private_key, api_key_id, "POST", base_url, path),
        json=body,
    )
    order = payload.get("order")
    if not isinstance(order, dict) or not order.get("order_id"):
        raise SmokeError("Kalshi create order response did not include order.order_id.")
    return order


def _cancel_order(
    session: requests.Session,
    base_url: str,
    private_key: Any,
    api_key_id: str,
    order_id: str,
) -> dict[str, Any]:
    path = f"/portfolio/orders/{order_id}"
    payload = _request_json(
        session,
        "DELETE",
        base_url + path,
        headers=_auth_headers(private_key, api_key_id, "DELETE", base_url, path),
    )
    order = payload.get("order")
    if not isinstance(order, dict):
        raise SmokeError("Kalshi cancel order response did not include order.")
    return order


def run() -> None:
    api_key_id = _require_env("KALSHI_API_KEY_ID")
    private_key = _load_private_key(_require_env("KALSHI_PRIVATE_KEY_PATH"))
    base_url = _base_url()

    session = requests.Session()
    session.trust_env = False

    order_id: str | None = None
    cancelled = False
    try:
        market = _select_market(_get_markets(session, base_url))
        ticker = str(market["ticker"])
        print(f"market: {ticker} | {market.get('title', '')}")

        orderbook = _get_orderbook(session, base_url, ticker)
        yes_levels = orderbook.get("yes_dollars") or []
        no_levels = orderbook.get("no_dollars") or []
        print(f"orderbook: yes_levels={len(yes_levels)} no_levels={len(no_levels)}")

        order = _post_order(session, base_url, private_key, api_key_id, ticker)
        order_id = str(order["order_id"])
        status = str(order.get("status") or "")
        if status and status != "resting":
            raise SmokeError(f"Created order {order_id} but status is {status!r}, not 'resting'.")
        print(f"placed: order_id={order_id} status={status or 'unknown'}")

        canceled = _cancel_order(session, base_url, private_key, api_key_id, order_id)
        cancelled = True
        print(f"cancelled: order_id={canceled.get('order_id', order_id)} status={canceled.get('status', 'unknown')}")
    finally:
        if order_id and not cancelled:
            _cancel_order(session, base_url, private_key, api_key_id, order_id)


def main() -> int:
    try:
        run()
    except Exception as exc:
        print(f"kalshi smoke failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
