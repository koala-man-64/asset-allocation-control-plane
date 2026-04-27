from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable

import yaml


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
_QUOTED_PLACEHOLDER_PATTERN = re.compile(r'(["\'])\$\{([A-Z0-9_]+)\}\1')
_OPTIONAL_EMPTY_PLACEHOLDER_NAMES = {
    "API_PUBLIC_BASE_URL",
    "ETRADE_CALLBACK_URL",
    "SCHWAB_APP_CALLBACK_URL",
    "SYMBOL_ENRICHMENT_ALLOWED_JOBS",
}
_AI_RELAY_SECRET_BLOCK = (
    "    - name: ai-relay-api-key",
    '      value: "${AI_RELAY_API_KEY}"',
)
_AI_RELAY_ENV_BLOCK = (
    "      - name: AI_RELAY_API_KEY",
    "        secretRef: ai-relay-api-key",
)
_UI_PASSWORD_HASH_SECRET_BLOCK = (
    "    - name: ui-shared-password-hash",
    '      value: "${UI_SHARED_PASSWORD_HASH}"',
)
_UI_PASSWORD_HASH_ENV_BLOCK = (
    "      - name: UI_SHARED_PASSWORD_HASH",
    "        secretRef: ui-shared-password-hash",
)
_SECRET_KEY_VAULT_URL_ENV_BY_NAME = {
    "azure-storage-connection-string": "AZURE_STORAGE_CONNECTION_STRING_KEY_VAULT_URL",
    "alpha-vantage-api-key": "ALPHA_VANTAGE_API_KEY_KEY_VAULT_URL",
    "massive-api-key": "MASSIVE_API_KEY_KEY_VAULT_URL",
    "backtest-pg-dsn": "POSTGRES_DSN_KEY_VAULT_URL",
    "alpaca-paper-api-key-id": "ALPACA_PAPER_API_KEY_ID_KEY_VAULT_URL",
    "alpaca-paper-secret-key": "ALPACA_PAPER_SECRET_KEY_KEY_VAULT_URL",
    "alpaca-live-api-key-id": "ALPACA_LIVE_API_KEY_ID_KEY_VAULT_URL",
    "alpaca-live-secret-key": "ALPACA_LIVE_SECRET_KEY_KEY_VAULT_URL",
    "etrade-sandbox-consumer-key": "ETRADE_SANDBOX_CONSUMER_KEY_KEY_VAULT_URL",
    "etrade-sandbox-consumer-secret": "ETRADE_SANDBOX_CONSUMER_SECRET_KEY_VAULT_URL",
    "etrade-live-consumer-key": "ETRADE_LIVE_CONSUMER_KEY_KEY_VAULT_URL",
    "etrade-live-consumer-secret": "ETRADE_LIVE_CONSUMER_SECRET_KEY_VAULT_URL",
    "schwab-client-id": "SCHWAB_CLIENT_ID_KEY_VAULT_URL",
    "schwab-client-secret": "SCHWAB_CLIENT_SECRET_KEY_VAULT_URL",
    "ai-relay-api-key": "AI_RELAY_API_KEY_KEY_VAULT_URL",
    "api-auth-session-secret-keys": "API_AUTH_SESSION_SECRET_KEYS_KEY_VAULT_URL",
}


def _remove_exact_block(lines: list[str], block: Iterable[str]) -> list[str]:
    block_lines = list(block)
    block_size = len(block_lines)
    rendered: list[str] = []
    index = 0

    while index < len(lines):
        if lines[index : index + block_size] == block_lines:
            index += block_size
            continue
        rendered.append(lines[index])
        index += 1

    return rendered


def _render_placeholders(template: str, env: dict[str, str]) -> str:
    def replace_quoted(match: re.Match[str]) -> str:
        return _render_yaml_scalar(match.group(2), env, fallback=match.group(0))

    rendered = _QUOTED_PLACEHOLDER_PATTERN.sub(replace_quoted, template)
    return _PLACEHOLDER_PATTERN.sub(lambda match: _render_yaml_scalar(match.group(1), env, fallback=match.group(0)), rendered)


def _normalize_quoted_scalar_env_value(value: str) -> str:
    candidate = value.strip()
    if len(candidate) < 2:
        return value

    quote = candidate[0]
    if quote not in {'"', "'"} or candidate[-1] != quote:
        return value

    inner = candidate[1:-1].strip()
    if not inner or "\n" in inner or inner[:1] in "{[":
        return value

    return inner


def _normalize_optional_placeholder_env_value(name: str, value: str) -> str:
    candidate = value.strip()
    if name in _OPTIONAL_EMPTY_PLACEHOLDER_NAMES and candidate == f"${{{name}}}":
        return ""
    return value


def _render_yaml_scalar(name: str, env: dict[str, str], *, fallback: str) -> str:
    if name not in env:
        return fallback
    value = _normalize_optional_placeholder_env_value(name, env[name])
    return json.dumps(_normalize_quoted_scalar_env_value(value))


def _validate_rendered_manifest(rendered: str) -> None:
    unresolved = sorted(set(_PLACEHOLDER_PATTERN.findall(rendered)))
    if unresolved:
        joined = ", ".join(unresolved)
        raise ValueError(f"Rendered manifest contains unresolved placeholders: {joined}")
    try:
        loaded = yaml.safe_load(rendered)
    except yaml.YAMLError as exc:
        raise ValueError(f"Rendered manifest is invalid YAML: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ValueError("Rendered manifest must parse into a YAML mapping.")


def _apply_key_vault_secret_refs(rendered: str, env: dict[str, str]) -> str:
    secret_url_envs = {
        secret_name: env_name
        for secret_name, env_name in _SECRET_KEY_VAULT_URL_ENV_BY_NAME.items()
        if env.get(env_name, "").strip()
    }
    if not secret_url_envs:
        return rendered

    runtime_identity = env.get("API_RUNTIME_IDENTITY_RESOURCE_ID", "").strip()
    if not runtime_identity:
        raise ValueError(
            "API_RUNTIME_IDENTITY_RESOURCE_ID is required when Container App secrets use Key Vault refs."
        )

    loaded = yaml.safe_load(rendered)
    if not isinstance(loaded, dict):
        raise ValueError("Rendered manifest must parse into a YAML mapping.")

    secrets = (
        loaded.get("properties", {})
        .get("configuration", {})
        .get("secrets", [])
    )
    if not isinstance(secrets, list):
        raise ValueError("Rendered manifest properties.configuration.secrets must be a YAML sequence.")

    changed = False
    for entry in secrets:
        if not isinstance(entry, dict):
            continue
        secret_name = str(entry.get("name") or "").strip()
        env_name = secret_url_envs.get(secret_name)
        if not env_name:
            continue
        entry.pop("value", None)
        entry["keyVaultUrl"] = _normalize_quoted_scalar_env_value(env[env_name].strip())
        entry["identity"] = runtime_identity
        changed = True

    if not changed:
        return rendered

    dumped = yaml.safe_dump(loaded, sort_keys=False)
    return dumped if rendered.endswith("\n") else dumped.rstrip("\n")


def render_control_plane_manifest(template: str, env: dict[str, str]) -> str:
    ai_relay_enabled = env.get("AI_RELAY_ENABLED", "").strip().lower() == "true"
    ai_relay_api_key = env.get("AI_RELAY_API_KEY", "").strip()
    ai_relay_key_vault_url = env.get("AI_RELAY_API_KEY_KEY_VAULT_URL", "").strip()
    ui_auth_provider = env.get("UI_AUTH_PROVIDER", "").strip().lower()
    ui_shared_password_hash = env.get("UI_SHARED_PASSWORD_HASH", "").strip()

    if ai_relay_enabled and not (ai_relay_api_key or ai_relay_key_vault_url):
        raise ValueError("AI_RELAY_ENABLED=true but secret AI_RELAY_API_KEY is missing or empty.")
    if ui_auth_provider == "password" and not ui_shared_password_hash:
        raise ValueError("UI_AUTH_PROVIDER=password but secret UI_SHARED_PASSWORD_HASH is missing or empty.")

    lines = template.splitlines()
    if not (ai_relay_api_key or ai_relay_key_vault_url):
        lines = _remove_exact_block(lines, _AI_RELAY_SECRET_BLOCK)
        lines = _remove_exact_block(lines, _AI_RELAY_ENV_BLOCK)
    if not ui_shared_password_hash:
        lines = _remove_exact_block(lines, _UI_PASSWORD_HASH_SECRET_BLOCK)
        lines = _remove_exact_block(lines, _UI_PASSWORD_HASH_ENV_BLOCK)

    rendered = _render_placeholders("\n".join(lines), env)
    if template.endswith("\n"):
        rendered += "\n"
    rendered = _apply_key_vault_secret_refs(rendered, env)
    _validate_rendered_manifest(rendered)
    return rendered


def main() -> int:
    parser = argparse.ArgumentParser(description="Render the control-plane Container App manifest.")
    parser.add_argument("--template", required=True, type=Path, help="Path to the manifest template.")
    parser.add_argument("--output", required=True, type=Path, help="Path to the rendered manifest.")
    args = parser.parse_args()

    template = args.template.read_text(encoding="utf-8")
    try:
        rendered = render_control_plane_manifest(template, dict(os.environ))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
