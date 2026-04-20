"""Read-only Schwab account smoke test with one-shot token refresh."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import httpx

from schwab import SchwabClient
from schwab.client import SchwabOAuthTokens
from schwab.config import SchwabConfig
from schwab.errors import SchwabAuthError
from schwab.local_env import load_schwab_config, save_schwab_tokens


@dataclass(frozen=True)
class SchwabAccountRow:
    account: str
    encrypted_id: str
    account_type: str
    net_liquidation: str
    cash: str
    buying_power: str


@dataclass(frozen=True)
class SchwabAccountSnapshot:
    env_path: Path
    rows: list[SchwabAccountRow]
    refreshed_tokens: bool


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _iter_mappings(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return [_as_mapping(item) for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        nested_accounts = payload.get("accounts")
        if isinstance(nested_accounts, Sequence) and not isinstance(nested_accounts, (str, bytes, bytearray)):
            return [_as_mapping(item) for item in nested_accounts if isinstance(item, Mapping)]
        return [payload]
    return []


def _first_present(mappings: Sequence[Mapping[str, Any]], *keys: str) -> Any:
    for mapping in mappings:
        for key in keys:
            value = mapping.get(key)
            if value is not None and value != "":
                return value
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _format_money(value: Any) -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return ""
    return f"${numeric:,.2f}"


def _mask_account_number(account_number: str) -> str:
    text = str(account_number or "").strip()
    if not text:
        return ""
    return f"***{text[-4:]}" if len(text) >= 4 else f"***{text}"


def _abbreviate_encrypted_id(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= 16:
        return text
    return f"{text[:6]}...{text[-6:]}"


def _build_hash_lookup(account_numbers_payload: Any) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for item in _iter_mappings(account_numbers_payload):
        account_number = str(item.get("accountNumber") or "").strip()
        hash_value = str(item.get("hashValue") or "").strip()
        if account_number and hash_value:
            lookup[account_number] = hash_value
    return lookup


def _normalize_rows(account_numbers_payload: Any, accounts_payload: Any) -> list[SchwabAccountRow]:
    hash_lookup = _build_hash_lookup(account_numbers_payload)
    rows: list[SchwabAccountRow] = []

    for item in _iter_mappings(accounts_payload):
        securities_account = _as_mapping(item.get("securitiesAccount"))
        current_balances = _as_mapping(securities_account.get("currentBalances"))
        projected_balances = _as_mapping(securities_account.get("projectedBalances"))
        initial_balances = _as_mapping(securities_account.get("initialBalances"))
        aggregated_balance = _as_mapping(item.get("aggregatedBalance"))

        views = [
            item,
            securities_account,
            current_balances,
            projected_balances,
            initial_balances,
            aggregated_balance,
        ]

        account_number = str(_first_present(views, "accountNumber") or "").strip()
        hash_value = str(hash_lookup.get(account_number) or _first_present(views, "hashValue") or "").strip()
        account_type = str(_first_present(views, "type", "accountType") or "").strip()

        net_liquidation = _first_present(
            views,
            "liquidationValue",
            "netLiquidationValue",
            "equity",
            "accountValue",
        )
        cash = _first_present(
            views,
            "cashBalance",
            "cashAvailableForTrading",
            "availableFunds",
            "availableCash",
        )
        buying_power = _first_present(
            views,
            "buyingPower",
            "dayTradingBuyingPower",
            "availableFundsNonMarginableTrade",
            "cashAvailableForTrading",
        )

        account_label = _mask_account_number(account_number)
        if not account_label and hash_value:
            account_label = _abbreviate_encrypted_id(hash_value)

        rows.append(
            SchwabAccountRow(
                account=account_label,
                encrypted_id=_abbreviate_encrypted_id(hash_value),
                account_type=account_type,
                net_liquidation=_format_money(net_liquidation),
                cash=_format_money(cash),
                buying_power=_format_money(buying_power),
            )
        )

    return rows


def _render_rows(rows: Sequence[SchwabAccountRow]) -> str:
    if not rows:
        return "No Schwab accounts were returned."

    headers = [
        ("account", "account"),
        ("encrypted_id", "encrypted_id"),
        ("account_type", "type"),
        ("net_liquidation", "net_liquidation"),
        ("cash", "cash"),
        ("buying_power", "buying_power"),
    ]
    widths: dict[str, int] = {}
    for field_name, heading in headers:
        widths[field_name] = max(len(heading), *(len(str(getattr(row, field_name))) for row in rows))

    header_line = "  ".join(heading.ljust(widths[field_name]) for field_name, heading in headers)
    separator = "  ".join("-" * widths[field_name] for field_name, _heading in headers)
    body = [
        "  ".join(str(getattr(row, field_name)).ljust(widths[field_name]) for field_name, _heading in headers)
        for row in rows
    ]
    return "\n".join([header_line, separator, *body])


def _refresh_tokens(
    config: SchwabConfig,
    env_path: Path,
    *,
    http_client: httpx.Client | None = None,
) -> tuple[SchwabConfig, SchwabOAuthTokens]:
    with SchwabClient(config, http_client=http_client) as client:
        tokens = client.refresh_access_token()

    save_schwab_tokens(env_path, tokens)
    refreshed_config = replace(
        config,
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token or config.refresh_token,
    )
    return refreshed_config, tokens


def fetch_account_snapshot(
    *,
    env_file: str | Path | None = None,
    http_client: httpx.Client | None = None,
) -> SchwabAccountSnapshot:
    env_path, config = load_schwab_config(env_file)
    refreshed_tokens = False

    for attempt in range(2):
        try:
            with SchwabClient(config, http_client=http_client) as client:
                account_numbers = client.get_account_numbers()
                accounts = client.get_accounts()
            rows = _normalize_rows(account_numbers, accounts)
            return SchwabAccountSnapshot(
                env_path=env_path,
                rows=rows,
                refreshed_tokens=refreshed_tokens,
            )
        except SchwabAuthError:
            if attempt > 0 or not config.refresh_token:
                raise
            config, _tokens = _refresh_tokens(config, env_path, http_client=http_client)
            refreshed_tokens = True

    raise RuntimeError("Unreachable Schwab account snapshot retry state.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only Schwab account smoke test that prints account balances.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to the local env file to read. Defaults to .env in the current working directory.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        snapshot = fetch_account_snapshot(env_file=args.env_file)
    except Exception as exc:
        print(f"Schwab account smoke test failed: {exc}", file=sys.stderr)
        return 1

    if snapshot.refreshed_tokens:
        print(f"Refreshed Schwab tokens and updated {snapshot.env_path}")
    print(_render_rows(snapshot.rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
