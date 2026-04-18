from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from asset_allocation_contracts.ui_config import UiRuntimeConfig


def _repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


ROOT = _repo_root()
OUTPUT_DIR = ROOT / "api" / "contracts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("TEST_MODE", "1")
os.environ.setdefault("LOG_FORMAT", "TEXT")
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "localdev")
os.environ.setdefault("AZURE_CONTAINER_COMMON", "local")

from api.service.app import create_app


def _serialize_json(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _render_artifact_texts() -> dict[Path, str]:
    app = create_app()
    return {
        OUTPUT_DIR / "control-plane.openapi.json": _serialize_json(app.openapi()),
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
            print(f"- {path}", file=sys.stderr)
        print(
            "Run `python scripts/automation/export_contract_artifacts.py`, review api/contracts/*, and stage the regenerated files.",
            file=sys.stderr,
        )
        return 1

    _write_artifact_texts(artifacts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
