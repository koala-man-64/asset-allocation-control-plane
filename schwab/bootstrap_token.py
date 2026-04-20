"""Manual OAuth bootstrap for Schwab tokens using a pasted callback URL."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx

from schwab.client import SchwabClient, SchwabOAuthTokens
from schwab.local_env import load_schwab_config, save_schwab_tokens


@dataclass(frozen=True)
class SchwabBootstrapResult:
    env_path: Path
    authorization_url: str
    tokens: SchwabOAuthTokens


def bootstrap_tokens_from_callback(
    callback_url: str,
    *,
    env_file: str | Path | None = None,
    state: str | None = None,
    http_client: httpx.Client | None = None,
) -> SchwabBootstrapResult:
    env_path, config = load_schwab_config(env_file)
    with SchwabClient(config, http_client=http_client) as client:
        authorization_url = client.build_authorization_url(state=state)
        authorization_code = client.extract_authorization_code(callback_url)
        tokens = client.exchange_authorization_code(authorization_code)

    save_schwab_tokens(env_path, tokens)
    return SchwabBootstrapResult(
        env_path=env_path,
        authorization_url=authorization_url,
        tokens=tokens,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap Schwab OAuth tokens by pasting the full redirected callback URL.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to the local env file to read and update. Defaults to .env in the current working directory.",
    )
    parser.add_argument(
        "--state",
        default=None,
        help="Optional OAuth state value to include in the authorization URL.",
    )
    parser.add_argument(
        "--callback-url",
        default=None,
        help="Full redirected callback URL. If omitted, the script prompts for it interactively.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        env_path, config = load_schwab_config(args.env_file)
        with SchwabClient(config) as client:
            authorization_url = client.build_authorization_url(state=args.state)

        print("Open this Schwab authorization URL in your browser:")
        print(authorization_url)
        print()
        print("After Schwab redirects to your loopback callback, the browser may show a connection or security error.")
        print("That is expected for manual copy mode. Copy the full URL from the browser address bar.")
        print()

        callback_url = args.callback_url or input("Paste the full redirected callback URL: ").strip()
        result = bootstrap_tokens_from_callback(
            callback_url,
            env_file=env_path,
            state=args.state,
        )
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Schwab bootstrap failed: {exc}", file=sys.stderr)
        return 1

    print(f"Saved SCHWAB_ACCESS_TOKEN and SCHWAB_REFRESH_TOKEN to {result.env_path}")
    print(f"Access token lifetime: {result.tokens.expires_in} seconds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
