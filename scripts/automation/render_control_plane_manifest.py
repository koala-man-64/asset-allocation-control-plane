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
_AI_RELAY_SECRET_BLOCK = (
    "    - name: ai-relay-api-key",
    '      value: "${AI_RELAY_API_KEY}"',
)
_AI_RELAY_ENV_BLOCK = (
    "      - name: AI_RELAY_API_KEY",
    "        secretRef: ai-relay-api-key",
)


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


def _render_yaml_scalar(name: str, env: dict[str, str], *, fallback: str) -> str:
    if name not in env:
        return fallback
    return json.dumps(_normalize_quoted_scalar_env_value(env[name]))


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


def render_control_plane_manifest(template: str, env: dict[str, str]) -> str:
    ai_relay_enabled = env.get("AI_RELAY_ENABLED", "").strip().lower() == "true"
    ai_relay_api_key = env.get("AI_RELAY_API_KEY", "").strip()

    if ai_relay_enabled and not ai_relay_api_key:
        raise ValueError("AI_RELAY_ENABLED=true but secret AI_RELAY_API_KEY is missing or empty.")

    lines = template.splitlines()
    if not ai_relay_api_key:
        lines = _remove_exact_block(lines, _AI_RELAY_SECRET_BLOCK)
        lines = _remove_exact_block(lines, _AI_RELAY_ENV_BLOCK)

    rendered = _render_placeholders("\n".join(lines), env)
    if template.endswith("\n"):
        rendered += "\n"
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
