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
