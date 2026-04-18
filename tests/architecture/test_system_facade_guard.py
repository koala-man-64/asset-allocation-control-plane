from __future__ import annotations

import ast
from pathlib import Path


_ALLOWED_FACADE_FUNCTIONS = {
    "_system_runtime",
    "_reject_removed_query_params",
    "_emit_realtime",
    "_get_actor",
    "_job_control_context",
    "_split_csv",
}


def _system_module_path() -> Path:
    return Path(__file__).resolve().parents[2] / "api" / "endpoints" / "system.py"


def _top_level_callables(path: Path) -> set[str]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }


def test_system_facade_defines_only_allowed_top_level_callables() -> None:
    assert _top_level_callables(_system_module_path()) == _ALLOWED_FACADE_FUNCTIONS


def test_system_facade_routes_remaining_purge_runtime_through_system_modules() -> None:
    text = _system_module_path().read_text(encoding="utf-8")
    assert "from api.endpoints.system_modules import purge_runtime as system_purge_runtime" in text
    assert "_run_purge_operation = system_purge_runtime._run_purge_operation" in text
