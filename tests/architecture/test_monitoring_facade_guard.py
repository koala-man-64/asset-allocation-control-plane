from __future__ import annotations

import ast
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _top_level_callables(path: Path) -> set[str]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }


def test_system_health_facade_defines_no_top_level_callables() -> None:
    facade_path = _repo_root() / "monitoring" / "system_health.py"
    assert _top_level_callables(facade_path) == set()


def test_snapshot_runtime_seam_uses_helper_module() -> None:
    snapshot_path = _repo_root() / "monitoring" / "system_health_modules" / "snapshot.py"
    runtime_helper_path = _repo_root() / "monitoring" / "system_health_modules" / "runtime.py"

    snapshot_text = snapshot_path.read_text(encoding="utf-8")
    assert "from monitoring.system_health_modules import runtime as system_health_runtime" in snapshot_text
    assert "import_module(\"monitoring.system_health\")" not in snapshot_text
    assert "_runtime_module = system_health_runtime.module" in snapshot_text
    assert runtime_helper_path.exists()
