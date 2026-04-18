#!/usr/bin/env python3
"""Run repo quality gates with deterministic local tool resolution."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys


def _repo_root() -> pathlib.Path:
    current = pathlib.Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


REPO_ROOT = _repo_root()


def resolve_python() -> str:
    candidates = [
        REPO_ROOT
        / ".venv"
        / ("Scripts" if os.name == "nt" else "bin")
        / ("python.exe" if os.name == "nt" else "python"),
        REPO_ROOT
        / "venv"
        / ("Scripts" if os.name == "nt" else "bin")
        / ("python.exe" if os.name == "nt" else "python"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def run(argv: list[str], cwd: pathlib.Path) -> int:
    if os.name == "nt" and pathlib.Path(argv[0]).suffix.lower() == ".cmd":
        command = subprocess.list2cmdline([str(part) for part in argv])
        completed = subprocess.run(["cmd.exe", "/d", "/s", "/c", command], cwd=str(cwd), check=False)
        return completed.returncode

    completed = subprocess.run([str(part) for part in argv], cwd=str(cwd), check=False)
    return completed.returncode


def build_command(gate: str) -> tuple[list[str], pathlib.Path]:
    python = resolve_python()
    gates: dict[str, tuple[list[str], pathlib.Path]] = {
        "lint-python": ([python, "-m", "ruff", "check", "."], REPO_ROOT),
        "format-python": ([python, "-m", "ruff", "format", "."], REPO_ROOT),
        "lint-fix-python": ([python, "-m", "ruff", "check", "--fix", "."], REPO_ROOT),
        "test-fast-api": (
            [
                python,
                "-m",
                "pytest",
                "-q",
                "tests/architecture/test_python_module_boundaries.py",
                "tests/architecture/test_system_facade_guard.py",
                "tests/architecture/test_monitoring_facade_guard.py",
                "tests/test_multirepo_dependency_contract.py",
                "tests/test_workflow_runtime_ownership.py",
                "tests/test_deploy_manifests.py",
                "tests/api/test_config_js_contract.py",
                "tests/api/test_internal_endpoints.py",
            ],
            REPO_ROOT,
        ),
        "contract-artifacts": ([python, "scripts/automation/export_contract_artifacts.py", "--check"], REPO_ROOT),
        "test-full-api": ([python, "-m", "pytest", "-q"], REPO_ROOT),
    }
    if gate not in gates:
        available = ", ".join(sorted(gates))
        raise SystemExit(f"Unknown gate '{gate}'. Expected one of: {available}")
    return gates[gate]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("Usage: python3 scripts/automation/run_quality_gate.py <gate>")
    command, cwd = build_command(argv[1])
    return run(command, cwd)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
