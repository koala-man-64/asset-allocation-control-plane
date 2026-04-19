from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


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


def test_renderer_omits_ai_relay_secret_and_env_binding_when_key_missing(tmp_path: Path) -> None:
    template = _repo_root() / "deploy" / "app_api_public.yaml"
    output = tmp_path / "rendered.yaml"

    result = _run_renderer(
        template=template,
        output=output,
        extra_env={
            "AI_RELAY_ENABLED": "false",
            "AI_RELAY_API_KEY": "",
        },
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
        extra_env={
            "AI_RELAY_ENABLED": "true",
            "AI_RELAY_API_KEY": "",
        },
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
        extra_env={
            "AI_RELAY_ENABLED": "true",
            "AI_RELAY_API_KEY": "test-key",
        },
    )

    assert result.returncode == 0, result.stderr
    rendered = output.read_text(encoding="utf-8")
    assert 'value: "test-key"' in rendered
    assert "\n      - name: AI_RELAY_API_KEY\n        secretRef: ai-relay-api-key\n" in rendered
