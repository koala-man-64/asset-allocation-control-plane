from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
from pathlib import Path

from asset_allocation_contracts.ui_config import UiRuntimeConfig
from dotenv import dotenv_values


def _repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


ROOT = _repo_root()
OUTPUT_DIR = ROOT / "api" / "contracts"
ENV_WEB_PATH = ROOT / ".env.web"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("TEST_MODE", "1")
os.environ.setdefault("LOG_FORMAT", "TEXT")
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "localdev")
os.environ.setdefault("AZURE_CONTAINER_COMMON", "local")

_CANONICAL_API_ROOT_PREFIX = ""
_DIFF_PREVIEW_LINE_LIMIT = 40
_ITEM_PREVIEW_LIMIT = 10


def _serialize_json(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _dotenv_loading_disabled() -> bool:
    value = (os.environ.get("DISABLE_DOTENV") or "").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


def _load_env_web_overrides() -> dict[str, str | None]:
    if _dotenv_loading_disabled() or not ENV_WEB_PATH.exists():
        return {}

    previous_values: dict[str, str | None] = {}
    for raw_key, raw_value in dotenv_values(ENV_WEB_PATH).items():
        key = str(raw_key).strip()
        if not key:
            continue
        previous_values[key] = os.environ.get(key)
        os.environ[key] = "" if raw_value is None else str(raw_value)
    return previous_values


def _restore_env(previous_values: dict[str, str | None]) -> None:
    for key, value in previous_values.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _render_artifact_texts() -> dict[Path, str]:
    previous_env_values = _load_env_web_overrides()
    previous_api_root_prefix = os.environ.get("API_ROOT_PREFIX")
    os.environ["API_ROOT_PREFIX"] = _CANONICAL_API_ROOT_PREFIX
    try:
        # Contract artifacts are generated from the portable unprefixed API surface.
        # Deployments may add API_ROOT_PREFIX for ingress routing without changing this artifact.
        from api.service.app import create_app

        app = create_app()
        openapi_text = _serialize_json(app.openapi())
    finally:
        if previous_api_root_prefix is None:
            os.environ.pop("API_ROOT_PREFIX", None)
        else:
            os.environ["API_ROOT_PREFIX"] = previous_api_root_prefix
        _restore_env(previous_env_values)

    return {
        OUTPUT_DIR / "control-plane.openapi.json": openapi_text,
        OUTPUT_DIR / "ui-runtime-config.schema.json": _serialize_json(UiRuntimeConfig.model_json_schema()),
    }


def _write_artifact_texts(artifacts: dict[Path, str]) -> None:
    for path, text in artifacts.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def _find_drift(artifacts: dict[Path, str]) -> list[Path]:
    drifted: list[Path] = []
    for path, expected in artifacts.items():
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current != expected:
            drifted.append(path)
    return drifted


def _load_json_document(text: str | None) -> object | None:
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _format_item_preview(items: list[str]) -> list[str]:
    preview = [f"- {item}" for item in items[:_ITEM_PREVIEW_LIMIT]]
    remaining = len(items) - _ITEM_PREVIEW_LIMIT
    if remaining > 0:
        preview.append(f"- ... ({remaining} more)")
    return preview


def _build_diff_preview(*, current: str | None, expected: str, path: Path) -> list[str]:
    diff_lines = list(
        difflib.unified_diff(
            [] if current is None else current.splitlines(),
            expected.splitlines(),
            fromfile=f"{path.name} (tracked)",
            tofile=f"{path.name} (generated)",
            lineterm="",
        )
    )
    if not diff_lines:
        return []

    if len(diff_lines) > _DIFF_PREVIEW_LINE_LIMIT:
        remaining = len(diff_lines) - _DIFF_PREVIEW_LINE_LIMIT
        diff_lines = diff_lines[:_DIFF_PREVIEW_LINE_LIMIT] + [f"... diff truncated ({remaining} more lines)"]
    return ["Diff preview:", *diff_lines]


def _build_openapi_path_summary(*, current_json: object | None, expected_json: object | None) -> list[str]:
    if not isinstance(current_json, dict) or not isinstance(expected_json, dict):
        return []

    current_paths = current_json.get("paths")
    expected_paths = expected_json.get("paths")
    if not isinstance(current_paths, dict) or not isinstance(expected_paths, dict):
        return []

    current_path_keys = set(current_paths)
    expected_path_keys = set(expected_paths)
    removed_paths = sorted(current_path_keys - expected_path_keys)
    added_paths = sorted(expected_path_keys - current_path_keys)
    changed_paths = sorted(
        path
        for path in current_path_keys & expected_path_keys
        if current_paths.get(path) != expected_paths.get(path)
    )

    lines = [
        f"OpenAPI paths: tracked={len(current_paths)} generated={len(expected_paths)}.",
    ]
    if removed_paths:
        lines.append(f"Paths only in tracked ({len(removed_paths)}):")
        lines.extend(_format_item_preview(removed_paths))
    if added_paths:
        lines.append(f"Paths only in generated ({len(added_paths)}):")
        lines.extend(_format_item_preview(added_paths))
    if changed_paths:
        lines.append(f"Changed shared paths: {len(changed_paths)}.")
        lines.extend(_format_item_preview(changed_paths))
    return lines


def _build_drift_details(*, path: Path, current: str | None, expected: str) -> list[str]:
    if current is None:
        return ["Tracked file is missing."]

    current_json = _load_json_document(current)
    expected_json = _load_json_document(expected)
    details: list[str] = []
    if current_json is not None and expected_json is not None and current_json == expected_json:
        details.append("Semantic JSON matches; drift is formatting-only.")

    if path.name == "control-plane.openapi.json":
        details.extend(_build_openapi_path_summary(current_json=current_json, expected_json=expected_json))

    details.extend(_build_diff_preview(current=current, expected=expected, path=path))
    return details


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export or validate generated contract artifacts.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check whether generated artifacts match tracked files without rewriting them.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifacts = _render_artifact_texts()

    if args.check:
        drifted = _find_drift(artifacts)
        if not drifted:
            print("Contract artifacts are current.")
            return 0
        print("Contract artifact drift detected:", file=sys.stderr)
        for path in drifted:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            print(f"- {path}", file=sys.stderr)
            for line in _build_drift_details(path=path, current=current, expected=artifacts[path]):
                print(f"  {line}", file=sys.stderr)
        print(
            "Run `python scripts/automation/export_contract_artifacts.py`, review api/contracts/*, and stage the regenerated files.",
            file=sys.stderr,
        )
        return 1

    _write_artifact_texts(artifacts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
