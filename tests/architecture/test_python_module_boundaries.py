from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _python_files_under(package: str) -> list[Path]:
    base = REPO_ROOT / package
    return sorted(
        path
        for path in base.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _task_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(
                alias.name
                for alias in node.names
                if alias.name.startswith("tasks.")
            )
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("tasks."):
            imports.append(node.module)
    return sorted(set(imports))


def test_api_has_no_direct_tasks_imports() -> None:
    offenders: list[str] = []
    for path in _python_files_under("api"):
        imports = _task_imports(path)
        if imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {imports}")
    assert offenders == []


def test_monitoring_has_no_direct_tasks_imports() -> None:
    offenders: list[str] = []
    for path in _python_files_under("monitoring"):
        imports = _task_imports(path)
        if imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {imports}")
    assert offenders == []


def test_core_has_no_direct_tasks_imports() -> None:
    offenders: list[str] = []
    for path in _python_files_under("core"):
        imports = _task_imports(path)
        if imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {imports}")
    assert offenders == []
