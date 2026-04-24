"""Read-only broker balance smoke test with interactive OAuth bootstrap.

This script is intentionally operator-facing. It reads the same local env files
used by the control plane, launches broker OAuth in a browser when needed,
keeps broker OAuth tokens in memory, and prints masked account identifiers with
balance fields.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
import webbrowser
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alpaca.errors import AlpacaError, AlpacaNotConfiguredError
from api.service.alpaca_gateway import AlpacaGateway
from api.service.settings import AlpacaSettings, ETradeSettings
from etrade_provider import ETradeBrokerAuthError, ETradeClient, ETradeEnvironmentConfig, ETradeError
from schwab import SchwabClient
from schwab.client import SchwabOAuthTokens
from schwab.config import SchwabConfig
from schwab.errors import SchwabAuthError, SchwabError, SchwabNotConfiguredError
from schwab.list_accounts import _normalize_rows as normalize_schwab_rows
from schwab.list_accounts import _render_rows as render_schwab_rows

DEFAULT_OPERATOR_PROJECT = Path.home() / "Projects" / "asset-allocation-control-plane"
DEFAULT_ENV_PATHS = (
    ROOT / ".env",
    ROOT / ".env.web",
    DEFAULT_OPERATOR_PROJECT / ".env",
    DEFAULT_OPERATOR_PROJECT / ".env.web",
)

ETRADE_API_BASE_URLS = {
    "sandbox": "https://apisb.etrade.com",
    "live": "https://api.etrade.com",
}


class OAuthRequiredError(RuntimeError):
    """Raised when a broker requires browser OAuth in noninteractive mode."""


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


def _mask_account(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 4:
        return f"***{text}"
    return f"***{text[-4:]}"


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.startswith("$"):
        text = text[1:]
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _format_money(value: Any) -> str:
    numeric = _to_decimal(value)
    if numeric is None:
        return ""
    return f"${numeric:,.2f}"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        return [value]
    return []


def _first_present(mappings: Sequence[Mapping[str, Any]], *keys: str) -> Any:
    lower_keys = {key.lower(): key for key in keys}
    for mapping in mappings:
        for key in keys:
            value = mapping.get(key)
            if value is not None and value != "":
                return value
        for actual_key, value in mapping.items():
            if str(actual_key).lower() in lower_keys and value is not None and value != "":
                return value
    return None


def _print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def _launch_url(url: str, *, non_interactive: bool, no_browser: bool) -> None:
    if non_interactive:
        raise OAuthRequiredError("OAuth is required, but --non-interactive was set.")
    print(url)
    if not no_browser:
        opened = webbrowser.open(url, new=2)
        if not opened:
            print("Browser launch was not acknowledged by the OS; copy the URL above if it did not open.")


def _prompt(message: str, *, non_interactive: bool) -> str:
    if non_interactive:
        raise OAuthRequiredError("OAuth is required, but --non-interactive was set.")
    return input(message).strip()


def _extract_query_value(raw: str, key: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if "=" not in text and "://" not in text:
        return text if key == "oauth_verifier" else ""
    parsed = urlparse(text)
    query = parsed.query if parsed.query else text
    values = parse_qs(query).get(key) or []
    return unquote(str(values[0])) if values else ""


def _summarize_error(exc: Exception) -> str:
    status_code = getattr(exc, "status_code", None)
    prefix = f"{type(exc).__name__}"
    if status_code:
        prefix = f"{prefix} status={status_code}"
    message = str(exc).strip() or type(exc).__name__
    if len(message) > 240:
        message = f"{message[:237]}..."
    return f"{prefix}: {message}"


def print_alpaca_balances() -> bool:
    _print_section("ALPACA")
    try:
        settings = AlpacaSettings.from_env()
    except Exception as exc:
        print(f"failed: {_summarize_error(exc)}")
        return False

    gateway = AlpacaGateway(settings)
    had_configured = False
    success = True
    try:
        for environment, configured in (
            ("paper", settings.paper_configured),
            ("live", settings.live_configured),
        ):
            if not configured:
                print(f"{environment}: not configured")
                continue
            had_configured = True
            try:
                account = gateway.get_account(environment=environment, subject="broker-balance-smoke")
            except AlpacaNotConfiguredError:
                print(f"{environment}: not configured")
                continue
            except AlpacaError as exc:
                print(f"{environment}: failed: {_summarize_error(exc)}")
                success = False
                continue

            print(
                f"{environment}: account={_mask_account(account.account_number)} "
                f"status={account.status} currency={account.currency} "
                f"cash={_format_money(account.cash)} equity={_format_money(account.equity)} "
                f"buying_power={_format_money(account.buying_power)}"
            )
    finally:
        gateway.close()

    if not had_configured:
        print("No Alpaca paper or live credentials were found.")
    return success


def _apply_schwab_tokens(config: SchwabConfig, tokens: SchwabOAuthTokens) -> SchwabConfig:
    return replace(
        config,
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token or config.refresh_token,
    )


def _fetch_schwab_payload(config: SchwabConfig) -> tuple[Any, Any]:
    with SchwabClient(config) as client:
        account_numbers = client.get_account_numbers()
        accounts = client.get_accounts()
    return account_numbers, accounts


def _refresh_schwab(config: SchwabConfig) -> SchwabConfig:
    with SchwabClient(config) as client:
        tokens = client.refresh_access_token()
    refreshed = _apply_schwab_tokens(config, tokens)
    print("Refreshed Schwab access token in process memory.")
    return refreshed


def _schwab_browser_oauth(
    config: SchwabConfig,
    *,
    non_interactive: bool,
    no_browser: bool,
) -> SchwabConfig:
    state = f"broker-balance-smoke-{secrets.token_urlsafe(16)}"
    with SchwabClient(config) as client:
        authorize_url = client.build_authorization_url(state=state)

    print(
        "Schwab OAuth required. Opening Schwab authorization URL:"
        if not non_interactive
        else "Schwab OAuth required."
    )
    _launch_url(authorize_url, non_interactive=non_interactive, no_browser=no_browser)
    print("After Schwab redirects, paste the full browser address bar URL.")
    callback_url = _prompt("Schwab callback URL: ", non_interactive=non_interactive)

    returned_state = _extract_query_value(callback_url, "state")
    if returned_state != state:
        raise RuntimeError("Schwab OAuth state mismatch. Refusing to exchange the authorization code.")

    with SchwabClient(config) as client:
        code = client.extract_authorization_code(callback_url)
        tokens = client.exchange_authorization_code(code)
    updated = _apply_schwab_tokens(config, tokens)
    print("Stored Schwab OAuth tokens in process memory for this smoke run.")
    return updated


def print_schwab_balances(
    *,
    non_interactive: bool,
    no_browser: bool,
) -> bool:
    _print_section("SCHWAB")
    try:
        config = SchwabConfig.from_mapping(os.environ)
    except Exception as exc:
        print(f"failed: {_summarize_error(exc)}")
        return False

    try:
        if config.access_token:
            try:
                account_numbers, accounts = _fetch_schwab_payload(config)
            except SchwabAuthError:
                if not config.refresh_token:
                    raise
                config = _refresh_schwab(config)
                account_numbers, accounts = _fetch_schwab_payload(config)
        elif config.refresh_token:
            config = _refresh_schwab(config)
            account_numbers, accounts = _fetch_schwab_payload(config)
        else:
            config = _schwab_browser_oauth(
                config,
                non_interactive=non_interactive,
                no_browser=no_browser,
            )
            account_numbers, accounts = _fetch_schwab_payload(config)
    except OAuthRequiredError as exc:
        print(f"requires OAuth: {exc}")
        return True
    except (SchwabAuthError, SchwabNotConfiguredError, SchwabError, RuntimeError, ValueError) as exc:
        print(f"failed: {_summarize_error(exc)}")
        return False

    rows = normalize_schwab_rows(account_numbers, accounts)
    print(render_schwab_rows(rows))
    return True


def _etrade_client(settings: ETradeSettings, environment: str) -> ETradeClient:
    normalized = str(environment or "").strip().lower()
    if normalized == "sandbox":
        env_config = ETradeEnvironmentConfig(
            environment="sandbox",
            consumer_key=settings.sandbox_consumer_key,
            consumer_secret=settings.sandbox_consumer_secret,
            api_base_url=ETRADE_API_BASE_URLS["sandbox"],
        )
    elif normalized == "live":
        env_config = ETradeEnvironmentConfig(
            environment="live",
            consumer_key=settings.live_consumer_key,
            consumer_secret=settings.live_consumer_secret,
            api_base_url=ETRADE_API_BASE_URLS["live"],
        )
    else:
        raise ValueError(f"Unsupported E*TRADE environment={environment!r}.")

    return ETradeClient(
        env_config,
        timeout_seconds=settings.timeout_seconds,
        read_retry_attempts=settings.read_retry_attempts,
        read_retry_base_delay_seconds=settings.read_retry_base_delay_seconds,
    )


def _extract_etrade_accounts(payload: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    response = _as_mapping(_as_mapping(payload).get("AccountListResponse") or payload)
    accounts_node = response.get("Accounts") or response.get("accounts") or response.get("Account") or []
    if isinstance(accounts_node, Mapping):
        account_items = accounts_node.get("Account") or accounts_node.get("account") or accounts_node
    else:
        account_items = accounts_node
    return [_as_mapping(item) for item in _as_list(account_items) if isinstance(item, Mapping)]


def _extract_etrade_balance_fields(payload: Mapping[str, Any]) -> tuple[str, str, str]:
    root = _as_mapping(payload.get("BalanceResponse") or payload)
    computed = _as_mapping(root.get("Computed") or root.get("computed"))
    account_balance = _as_mapping(root.get("accountBalance") or root.get("AccountBalance"))
    cash = _as_mapping(root.get("Cash") or root.get("cash"))
    margin = _as_mapping(root.get("Margin") or root.get("margin"))
    views = [root, computed, account_balance, cash, margin]
    net_value = _first_present(
        views,
        "netAccountValue",
        "totalAccountValue",
        "accountValue",
        "netLiquidation",
        "equity",
    )
    cash_value = _first_present(
        views,
        "cashBalance",
        "cashAvailableForWithdrawal",
        "cashAvailableForInvestment",
        "netCash",
        "availableCash",
    )
    buying_power = _first_present(
        views,
        "buyingPower",
        "marginBuyingPower",
        "cashBuyingPower",
        "cashAvailableForInvestment",
    )
    return _format_money(net_value), _format_money(cash_value), _format_money(buying_power)


def _run_etrade_oauth(
    client: ETradeClient,
    *,
    environment: str,
    non_interactive: bool,
    no_browser: bool,
) -> tuple[str, str]:
    request_token = client.fetch_request_token(callback_uri="oob")
    token = str(request_token.get("oauth_token") or "")
    token_secret = str(request_token.get("oauth_token_secret") or "")
    if not token or not token_secret:
        raise ETradeBrokerAuthError("E*TRADE did not return a complete request token.")

    authorize_url = client.build_authorize_url(request_token=token)
    print(
        f"E*TRADE {environment} OAuth required. Opening authorization URL:"
        if not non_interactive
        else f"E*TRADE {environment} OAuth required."
    )
    _launch_url(authorize_url, non_interactive=non_interactive, no_browser=no_browser)
    print("Paste the E*TRADE verification code, or paste the full callback URL if E*TRADE redirected.")
    raw_verifier = _prompt(f"E*TRADE {environment} verifier/callback URL: ", non_interactive=non_interactive)
    verifier = _extract_query_value(raw_verifier, "oauth_verifier") or raw_verifier
    verifier = verifier.strip()
    if not verifier:
        raise RuntimeError("No E*TRADE OAuth verifier was provided.")

    access_token = client.fetch_access_token(
        request_token=token,
        request_token_secret=token_secret,
        verifier=verifier,
    )
    access = str(access_token.get("oauth_token") or "")
    secret = str(access_token.get("oauth_token_secret") or "")
    if not access or not secret:
        raise ETradeBrokerAuthError("E*TRADE did not return a complete access token.")
    return access, secret


def _print_etrade_environment(
    settings: ETradeSettings,
    *,
    environment: str,
    non_interactive: bool,
    no_browser: bool,
) -> bool:
    client = _etrade_client(settings, environment)
    if not client.config.is_configured:
        print(f"{environment}: not configured")
        return True

    try:
        access_token, access_token_secret = _run_etrade_oauth(
            client,
            environment=environment,
            non_interactive=non_interactive,
            no_browser=no_browser,
        )
        accounts_payload = client.list_accounts(
            access_token=access_token,
            access_token_secret=access_token_secret,
        )
        accounts = _extract_etrade_accounts(accounts_payload)
        if not accounts:
            print(f"{environment}: no accounts returned")
            return True

        for account in accounts:
            account_key = str(_first_present([account], "accountIdKey", "accountKey", "accountId") or "").strip()
            account_id = str(_first_present([account], "accountId", "accountNumber") or account_key).strip()
            account_type = str(_first_present([account], "accountType") or "").strip()
            inst_type = str(_first_present([account], "institutionType", "instType") or "BROKERAGE").strip()
            if not account_key:
                print(f"{environment}: account={_mask_account(account_id)} missing accountIdKey; balance skipped")
                continue

            balance = client.get_balance(
                access_token=access_token,
                access_token_secret=access_token_secret,
                account_key=account_key,
                inst_type=inst_type,
                account_type=account_type or None,
                real_time_nav=False,
            )
            net_value, cash, buying_power = _extract_etrade_balance_fields(balance)
            print(
                f"{environment}: account={_mask_account(account_id)} type={account_type or '-'} "
                f"inst={inst_type or '-'} net_account_value={net_value or '-'} "
                f"cash={cash or '-'} buying_power={buying_power or '-'}"
            )
    except OAuthRequiredError as exc:
        print(f"{environment}: requires OAuth: {exc}")
        return True
    except (ETradeError, RuntimeError, ValueError) as exc:
        print(f"{environment}: failed: {_summarize_error(exc)}")
        return False

    return True


def print_etrade_balances(
    *,
    environments: Sequence[str],
    non_interactive: bool,
    no_browser: bool,
) -> bool:
    _print_section("ETRADE")
    try:
        settings = ETradeSettings.from_env()
    except Exception as exc:
        print(f"failed: {_summarize_error(exc)}")
        return False

    success = True
    for environment in environments:
        success = (
            _print_etrade_environment(
                settings,
                environment=environment,
                non_interactive=non_interactive,
                no_browser=no_browser,
            )
            and success
        )
    return success


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print read-only Alpaca, Schwab, and E*TRADE balances; launch OAuth when needed.",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=None,
        help="Env file to load. Repeat to load multiple files in order. Defaults to repo/operator .env and .env.web.",
    )
    parser.add_argument("--skip-alpaca", action="store_true", help="Skip Alpaca balance retrieval.")
    parser.add_argument("--skip-schwab", action="store_true", help="Skip Schwab balance retrieval.")
    parser.add_argument("--skip-etrade", action="store_true", help="Skip E*TRADE balance retrieval.")
    parser.add_argument(
        "--etrade-environment",
        action="append",
        choices=("sandbox", "live"),
        default=None,
        help="E*TRADE environment to query. Repeat for both. Defaults to live.",
    )
    parser.add_argument(
        "--include-etrade-sandbox",
        action="store_true",
        help="Also query E*TRADE sandbox after the selected/default environments.",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print OAuth URLs without asking the OS to open a browser.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Do not open browsers or prompt; report OAuth requirements instead.",
    )
    return parser


def _selected_etrade_environments(args: argparse.Namespace) -> list[str]:
    values = list(args.etrade_environment or ["live"])
    if args.include_etrade_sandbox:
        values.append("sandbox")
    result: list[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    loaded_paths = _existing_unique_paths(
        Path(path) for path in (args.env_file if args.env_file is not None else DEFAULT_ENV_PATHS)
    )
    if not loaded_paths:
        print("No env files were found. Pass --env-file explicitly.", file=sys.stderr)
        return 1

    _load_env(loaded_paths)

    print("Loaded env files:")
    for path in loaded_paths:
        print(f"  {path}")
    print("This script performs read-only account/balance calls. Broker OAuth tokens are kept in memory.")

    success = True
    if not args.skip_alpaca:
        success = print_alpaca_balances() and success
    if not args.skip_schwab:
        success = (
            print_schwab_balances(
                non_interactive=args.non_interactive,
                no_browser=args.no_browser,
            )
            and success
        )
    if not args.skip_etrade:
        success = (
            print_etrade_balances(
                environments=_selected_etrade_environments(args),
                non_interactive=args.non_interactive,
                no_browser=args.no_browser,
            )
            and success
        )

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
