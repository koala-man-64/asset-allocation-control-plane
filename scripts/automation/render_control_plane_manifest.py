from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Iterable


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
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
    return _PLACEHOLDER_PATTERN.sub(lambda match: env.get(match.group(1), match.group(0)), template)


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
