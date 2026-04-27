from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import yaml


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_renderer(*, template: Path, output: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(extra_env)
    return subprocess.run(
        [
            sys.executable,
            str(_repo_root() / "scripts" / "automation" / "render_control_plane_manifest.py"),
            "--template",
            str(template),
            "--output",
            str(output),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _template_env(template: Path, *, overrides: dict[str, str] | None = None, drop: set[str] | None = None) -> dict[str, str]:
    names = set(_PLACEHOLDER_PATTERN.findall(template.read_text(encoding="utf-8")))
    env = {name: "test-value" for name in names}
    for name in drop or set():
        env.pop(name, None)
    env.update(overrides or {})
    return env


def _manifest_env_value(rendered: str, name: str) -> str:
    doc = yaml.safe_load(rendered)
    env_entries = doc["properties"]["template"]["containers"][0]["env"]
    for entry in env_entries:
        if entry["name"] == name:
            return entry["value"]
    raise AssertionError(f"Missing env entry {name}")


def test_renderer_omits_ai_relay_secret_and_env_binding_when_key_missing(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
            "AI_RELAY_ENABLED": "false",
            "AI_RELAY_API_KEY": "",
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert "name: ai-relay-api-key" not in rendered
    assert "\n      - name: AI_RELAY_API_KEY\n" not in rendered


def test_renderer_fails_fast_when_ai_relay_enabled_without_key(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
            "AI_RELAY_ENABLED": "true",
            "AI_RELAY_API_KEY": "",
            },
        ),
    )

    assert result.returncode == 1
    assert "AI_RELAY_ENABLED=true but secret AI_RELAY_API_KEY is missing or empty." in result.stderr
    assert not output.exists()


def test_renderer_keeps_ai_relay_secret_and_env_binding_when_key_present(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
            "AI_RELAY_ENABLED": "true",
            "AI_RELAY_API_KEY": "test-key",
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert 'value: "test-key"' in rendered
    assert "\n      - name: AI_RELAY_API_KEY\n        secretRef: ai-relay-api-key\n" in rendered


def test_renderer_omits_ui_password_hash_secret_and_env_binding_when_key_missing(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
                "UI_AUTH_PROVIDER": "",
                "UI_SHARED_PASSWORD_HASH": "",
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert "name: ui-shared-password-hash" not in rendered
    assert "\n      - name: UI_SHARED_PASSWORD_HASH\n" not in rendered


def test_renderer_fails_fast_when_ui_password_auth_enabled_without_hash(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
                "UI_AUTH_PROVIDER": "password",
                "UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED": "true",
                "UI_BREAK_GLASS_PASSWORD_ROLES": "AssetAllocation.System.Read",
                "UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS": "127.0.0.1/32",
                "UI_BREAK_GLASS_PASSWORD_EXPIRES_AT": "2099-01-01T00:00:00Z",
                "API_AUTH_SESSION_MODE": "cookie",
                "UI_SHARED_PASSWORD_HASH": "",
            },
        ),
    )

    assert result.returncode == 1
    assert "UI_AUTH_PROVIDER=password but secret UI_SHARED_PASSWORD_HASH is missing or empty." in result.stderr
    assert not output.exists()


def test_renderer_keeps_ui_password_hash_secret_and_env_binding_when_hash_present(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
                "UI_AUTH_PROVIDER": "password",
                "UI_BREAK_GLASS_PASSWORD_AUTH_ENABLED": "true",
                "UI_BREAK_GLASS_PASSWORD_ROLES": "AssetAllocation.System.Read",
                "UI_BREAK_GLASS_PASSWORD_ALLOWED_CIDRS": "127.0.0.1/32",
                "UI_BREAK_GLASS_PASSWORD_EXPIRES_AT": "2099-01-01T00:00:00Z",
                "API_AUTH_SESSION_MODE": "cookie",
                "UI_SHARED_PASSWORD_HASH": "test-password-hash",
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert 'value: "test-password-hash"' in rendered
    assert "\n      - name: UI_SHARED_PASSWORD_HASH\n        secretRef: ui-shared-password-hash\n" in rendered


def test_renderer_omits_empty_kalshi_secret_bindings(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
                "KALSHI_ENABLED": "false",
                "KALSHI_TRADING_ENABLED": "false",
                "KALSHI_LIVE_API_KEY_ID": "",
                "KALSHI_LIVE_PRIVATE_KEY_PEM": "",
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    doc = yaml.safe_load(output.read_text(encoding="utf-8"))
    secrets = {
        entry["name"]
        for entry in doc["properties"]["configuration"]["secrets"]
    }
    env_names = {
        entry["name"]
        for entry in doc["properties"]["template"]["containers"][0]["env"]
    }
    assert "kalshi-live-api-key-id" not in secrets
    assert "kalshi-live-private-key-pem" not in secrets
    assert "KALSHI_LIVE_API_KEY_ID" not in env_names
    assert "KALSHI_LIVE_PRIVATE_KEY_PEM" not in env_names
    assert _manifest_env_value(output.read_text(encoding="utf-8"), "KALSHI_ENABLED") == "false"


def test_renderer_serializes_env_values_as_yaml_safe_strings(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"
    issuer = '"https://login.microsoftonline.com/example-tenant/v2.0"'
    freshness_json = '{"readyz":{"max_age_seconds":60}}'

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
            "AI_RELAY_ENABLED": "false",
            "AI_RELAY_API_KEY": "",
            "API_OIDC_ISSUER": issuer,
            "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON": freshness_json,
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert _manifest_env_value(rendered, "API_OIDC_ISSUER") == "https://login.microsoftonline.com/example-tenant/v2.0"
    assert _manifest_env_value(rendered, "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON") == freshness_json


def test_renderer_treats_optional_self_placeholders_as_blank(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
                "API_PUBLIC_BASE_URL": "${API_PUBLIC_BASE_URL}",
                "ETRADE_CALLBACK_URL": "${ETRADE_CALLBACK_URL}",
                "SYMBOL_ENRICHMENT_ALLOWED_JOBS": "${SYMBOL_ENRICHMENT_ALLOWED_JOBS}",
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert _manifest_env_value(rendered, "API_PUBLIC_BASE_URL") == ""
    assert _manifest_env_value(rendered, "ETRADE_CALLBACK_URL") == ""
    assert _manifest_env_value(rendered, "SYMBOL_ENRICHMENT_ALLOWED_JOBS") == ""


def test_renderer_fails_fast_when_manifest_placeholders_remain_unresolved(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
            },
            drop={"API_PUBLIC_BASE_URL"},
        ),
    )

    assert result.returncode == 1
    assert "Rendered manifest contains unresolved placeholders: API_PUBLIC_BASE_URL" in result.stderr
    assert not output.exists()


def test_renderer_converts_configured_secrets_to_key_vault_refs(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api.yaml"
    output = tmp_path / "rendered.yaml"
    key_vault_url = "https://kv.vault.azure.net/secrets/postgres-dsn/version"
    runtime_identity = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/api-runtime"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env=_template_env(
            template,
            overrides={
                "AI_RELAY_ENABLED": "false",
                "AI_RELAY_API_KEY": "",
                "POSTGRES_DSN_KEY_VAULT_URL": key_vault_url,
                "API_RUNTIME_IDENTITY_RESOURCE_ID": runtime_identity,
            },
        ),
    )

    assert result.returncode == 0, result.stderr
    doc = yaml.safe_load(output.read_text(encoding="utf-8"))
    secrets = {
        entry["name"]: entry
        for entry in doc["properties"]["configuration"]["secrets"]
    }
    assert secrets["backtest-pg-dsn"]["keyVaultUrl"] == key_vault_url
    assert secrets["backtest-pg-dsn"]["identity"] == runtime_identity
    assert "value" not in secrets["backtest-pg-dsn"]
