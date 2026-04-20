"""Helpers for reading and updating local Schwab credentials in .env files."""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values, set_key

from schwab.client import SchwabOAuthTokens
from schwab.config import SchwabConfig


DEFAULT_ENV_FILE = ".env"


def resolve_env_path(env_file: str | Path | None = None) -> Path:
    candidate = Path(env_file) if env_file is not None else Path(DEFAULT_ENV_FILE)
    if candidate.is_absolute():
        return candidate
    return (Path.cwd() / candidate).resolve()


def load_schwab_config(
    env_file: str | Path | None = None,
    *,
    require_client_credentials: bool = True,
) -> tuple[Path, SchwabConfig]:
    env_path = resolve_env_path(env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Env file not found: {env_path}")

    values = dotenv_values(env_path)
    config = SchwabConfig.from_mapping(
        values,
        require_client_credentials=require_client_credentials,
    )
    return env_path, config


def save_schwab_tokens(env_path: str | Path, tokens: SchwabOAuthTokens) -> Path:
    resolved = resolve_env_path(env_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if not resolved.exists():
        resolved.write_text("", encoding="utf-8")

    set_key(str(resolved), "SCHWAB_ACCESS_TOKEN", tokens.access_token, quote_mode="never")
    if tokens.refresh_token:
        set_key(str(resolved), "SCHWAB_REFRESH_TOKEN", tokens.refresh_token, quote_mode="never")
    return resolved
