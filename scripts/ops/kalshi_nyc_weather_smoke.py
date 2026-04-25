"""Operator-facing Kalshi order smoke for the NYC weather market.

The script mirrors the env-file loading pattern used by the broker operator
scripts, then uses the repo-local Kalshi client directly to:

1. resolve the target market (today's NYC weather market by default)
2. fetch the orderbook
3. place a post-only YES limit order
4. cancel the order
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.service.settings import KalshiSettings
from kalshi import HttpConfig, KalshiEnvironmentConfig, KalshiTradingClient, normalize_private_key_pem
from kalshi.models import KalshiMarket


DEFAULT_OPERATOR_PROJECT = Path.home() / "Projects" / "asset-allocation-control-plane"
DEFAULT_ENV_PATHS = (
    ROOT / ".env",
    ROOT / ".env.web",
    DEFAULT_OPERATOR_PROJECT / ".env",
    DEFAULT_OPERATOR_PROJECT / ".env.web",
)
DEFAULT_SERIES_TICKER = "KXHIGHNY"
DEFAULT_ORDER_PRICE_CENTS = 1
DEFAULT_ORDER_DEPTH = 5
_ENV_ALIASES = {
    "demo": "demo",
    "prod": "live",
    "production": "live",
    "live": "live",
}


class SmokeError(RuntimeError):
    pass


def _existing_unique_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    result: list[Path] = []
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        result.append(resolved)
    return result


def _load_env(paths: Sequence[Path]) -> dict[str, str]:
    merged = dict(os.environ)
    for path in paths:
        values = dotenv_values(path)
        for key, value in values.items():
            merged[str(key)] = "" if value is None else str(value)
    os.environ.update(merged)
    return merged


def _get_optional_env(name: str) -> str | None:
    raw = os.environ.get(name)
    value = raw.strip().strip('"') if raw else ""
    return value or None


def _require_value(name: str, value: Optional[str]) -> str:
    if value:
        return value
    raise SmokeError(f"Missing required environment variable: {name}")


def _selected_env_paths(env_files: Sequence[str] | None) -> list[Path]:
    if env_files is None:
        return list(DEFAULT_ENV_PATHS)
    return [Path(path) for path in env_files]


def _resolve_environment(explicit_environment: str | None) -> str:
    raw = (explicit_environment or _get_optional_env("KALSHI_ENV") or "demo").lower()
    environment = _ENV_ALIASES.get(raw)
    if environment is None:
        raise SmokeError(f"Unsupported KALSHI_ENV={raw!r}; use demo, prod, production, or live.")
    return environment


def _load_private_key_pem_from_path(path: str) -> str:
    key_path = Path(path).expanduser()
    if not key_path.exists():
        raise SmokeError(f"KALSHI_PRIVATE_KEY_PATH does not exist: {key_path}")
    return key_path.read_text(encoding="utf-8")


def _legacy_environment_config(settings: KalshiSettings, environment: str) -> KalshiEnvironmentConfig | None:
    api_key_id = _get_optional_env("KALSHI_API_KEY_ID")
    private_key_pem = normalize_private_key_pem(_get_optional_env("KALSHI_PRIVATE_KEY_PEM"))
    private_key_path = _get_optional_env("KALSHI_PRIVATE_KEY_PATH")

    if private_key_pem is None and private_key_path:
        private_key_pem = _load_private_key_pem_from_path(private_key_path)

    if not api_key_id or not private_key_pem:
        return None

    base_url = settings.demo_base_url if environment == "demo" else settings.live_base_url
    return KalshiEnvironmentConfig(
        environment=environment,  # type: ignore[arg-type]
        api_key_id=api_key_id,
        private_key_pem=private_key_pem,
        base_url=base_url,
        http=HttpConfig(
            timeout_s=settings.timeout_seconds,
            read_retry_attempts=settings.read_retry_attempts,
            read_retry_base_s=settings.read_retry_base_delay_seconds,
        ),
    )


def _configured_environment_config(settings: KalshiSettings, environment: str) -> KalshiEnvironmentConfig:
    if environment == "demo":
        return KalshiEnvironmentConfig(
            environment="demo",
            api_key_id=settings.demo_api_key_id,
            private_key_pem=settings.demo_private_key_pem,
            base_url=settings.demo_base_url,
            http=HttpConfig(
                timeout_s=settings.timeout_seconds,
                read_retry_attempts=settings.read_retry_attempts,
                read_retry_base_s=settings.read_retry_base_delay_seconds,
            ),
        )
    return KalshiEnvironmentConfig(
        environment="live",
        api_key_id=settings.live_api_key_id,
        private_key_pem=settings.live_private_key_pem,
        base_url=settings.live_base_url,
        http=HttpConfig(
            timeout_s=settings.timeout_seconds,
            read_retry_attempts=settings.read_retry_attempts,
            read_retry_base_s=settings.read_retry_base_delay_seconds,
        ),
    )


def _resolve_config(environment: str) -> KalshiEnvironmentConfig:
    try:
        settings = KalshiSettings.from_env()
    except Exception as exc:
        raise SmokeError(str(exc)) from exc

    config = _configured_environment_config(settings, environment)
    if config.is_configured:
        return config

    legacy = _legacy_environment_config(settings, environment)
    if legacy is not None:
        return legacy

    prefix = "KALSHI_DEMO" if environment == "demo" else "KALSHI_LIVE"
    raise SmokeError(
        "Missing Kalshi credentials for {environment}. Set {prefix}_API_KEY_ID with "
        "{prefix}_PRIVATE_KEY_PEM, or use the legacy KALSHI_API_KEY_ID with "
        "KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH.".format(
            environment=environment,
            prefix=prefix,
        )
    )


def _today_event_suffix() -> str:
    today = datetime.now(ZoneInfo("America/New_York")).date()
    return today.strftime("%y%b%d").upper()


def _market_date_matches_today(market: KalshiMarket, event_suffix: str) -> bool:
    event_ticker = str(market.event_ticker or "")
    if event_ticker.endswith(event_suffix):
        return True
    occurrence = market.occurrence_datetime
    if occurrence is None:
        return False
    return occurrence.astimezone(ZoneInfo("America/New_York")).date() == datetime.now(
        ZoneInfo("America/New_York")
    ).date()


def _select_market(markets: list[KalshiMarket], *, series_ticker: str) -> KalshiMarket:
    event_suffix = _today_event_suffix()
    candidates = [
        market
        for market in markets
        if str(market.event_ticker or "").startswith(f"{series_ticker}-")
        and _market_date_matches_today(market, event_suffix)
    ]
    if not candidates:
        raise SmokeError(f"No active {series_ticker} market found for today's NYC date.")

    for market in candidates:
        yes_ask = market.yes_ask_dollars or Decimal("0")
        yes_ask_size = market.yes_ask_size_fp or Decimal("0")
        if yes_ask_size == 0 or yes_ask > Decimal("0.0100"):
            return market

    raise SmokeError("No today's market can safely accept a non-marketable 1 cent post-only YES bid.")


def _resolve_market(
    client: KalshiTradingClient,
    *,
    series_ticker: str,
    market_ticker: str | None,
) -> KalshiMarket:
    if market_ticker:
        return client.get_market(market_ticker.strip().upper())
    return _select_market(
        client.list_markets(limit=100, series_ticker=series_ticker, status="open").markets,
        series_ticker=series_ticker,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Place and cancel a Kalshi YES limit order for today's NYC weather market or an explicit ticker.",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=None,
        help="Env file to load. Repeat to load multiple files in order. Defaults to repo/operator .env and .env.web.",
    )
    parser.add_argument(
        "--environment",
        choices=("demo", "live"),
        default=None,
        help="Kalshi environment to use. Defaults to KALSHI_ENV or demo.",
    )
    parser.add_argument(
        "--series-ticker",
        default=DEFAULT_SERIES_TICKER,
        help=f"Series ticker used when auto-selecting today's market. Defaults to {DEFAULT_SERIES_TICKER}.",
    )
    parser.add_argument(
        "--market-ticker",
        default=None,
        help="Explicit Kalshi market ticker to use instead of auto-selecting today's series market.",
    )
    parser.add_argument(
        "--yes-price-cents",
        type=int,
        default=DEFAULT_ORDER_PRICE_CENTS,
        choices=range(1, 100),
        metavar="{1..99}",
        help=f"YES limit price in cents. Defaults to {DEFAULT_ORDER_PRICE_CENTS}.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Contract count to submit. Defaults to 1.",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=DEFAULT_ORDER_DEPTH,
        help=f"Orderbook depth to request. Defaults to {DEFAULT_ORDER_DEPTH}.",
    )
    return parser


def run(args: argparse.Namespace) -> None:
    env_paths = _existing_unique_paths(_selected_env_paths(args.env_file))
    if env_paths:
        _load_env(env_paths)
        print("Loaded env files:")
        for path in env_paths:
            print(f"  {path}")
    else:
        print("No env files found. Using the current process environment only.")

    environment = _resolve_environment(args.environment)
    config = _resolve_config(environment)
    print(
        "Kalshi smoke configuration: environment={environment} base_url={base_url} "
        "series_ticker={series} market_ticker={ticker}".format(
            environment=environment,
            base_url=config.get_base_url(),
            series=args.series_ticker,
            ticker=(args.market_ticker or "<auto>"),
        )
    )

    order_id: str | None = None
    cancelled = False
    with KalshiTradingClient(config) as client:
        try:
            market = _resolve_market(
                client,
                series_ticker=str(args.series_ticker).strip().upper(),
                market_ticker=args.market_ticker,
            )
            print(f"market: {market.ticker} | {market.title or ''} | status={market.status or 'unknown'}")

            orderbook = client.get_orderbook(market.ticker, depth=args.depth)
            print(f"orderbook: yes_levels={len(orderbook.yes_dollars)} no_levels={len(orderbook.no_dollars)}")

            order = client.create_order(
                {
                    "ticker": market.ticker,
                    "action": "buy",
                    "side": "yes",
                    "count": args.count,
                    "type": "limit",
                    "yes_price": args.yes_price_cents,
                    "post_only": True,
                    "client_order_id": f"kalshi-smoke-{uuid.uuid4()}",
                }
            )
            order_id = order.order_id
            status = str(order.status or "")
            if status and status != "resting":
                raise SmokeError(f"Created order {order_id} but status is {status!r}, not 'resting'.")
            print(f"placed: order_id={order_id} status={status or 'unknown'}")

            canceled = client.cancel_order(order_id)
            cancelled = True
            print(
                "cancelled: order_id={order_id} status={status} reduced_by_fp={reduced}".format(
                    order_id=canceled.order.order_id,
                    status=canceled.order.status or "unknown",
                    reduced=format(canceled.reduced_by_fp, "f"),
                )
            )
        finally:
            if order_id and not cancelled:
                client.cancel_order(order_id)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        run(args)
    except Exception as exc:
        print(f"kalshi smoke failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
