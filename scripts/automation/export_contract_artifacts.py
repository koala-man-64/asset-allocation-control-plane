from __future__ import annotations

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


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    app = create_app()
    _write_json(OUTPUT_DIR / "control-plane.openapi.json", app.openapi())
    _write_json(OUTPUT_DIR / "ui-runtime-config.schema.json", UiRuntimeConfig.model_json_schema())


if __name__ == "__main__":
    main()
