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
UI_ROOT = REPO_ROOT / "ui"


def resolve_python() -> str:
    candidates = [
        REPO_ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin") / ("python.exe" if os.name == "nt" else "python"),
        REPO_ROOT / "venv" / ("Scripts" if os.name == "nt" else "bin") / ("python.exe" if os.name == "nt" else "python"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def resolve_ui_bin(name: str) -> pathlib.Path:
    suffixes = [".CMD", ".cmd"] if os.name == "nt" else [""]
    for suffix in suffixes:
        candidate = UI_ROOT / "node_modules" / ".bin" / f"{name}{suffix}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Unable to find UI tool '{name}' under {UI_ROOT / 'node_modules' / '.bin'}")


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
        "lint-ui": ([str(resolve_ui_bin("eslint")), "src", "--report-unused-disable-directives"], UI_ROOT),
        "typecheck-ui": ([str(resolve_ui_bin("tsc")), "--noEmit"], UI_ROOT),
        "test-fast-api": (
            [
                python,
                "-m",
                "pytest",
                "-q",
                "tests/tasks",
                "tests/market_data",
                "tests/finance_data",
                "tests/earnings_data",
                "tests/price_target_data",
            ],
            REPO_ROOT,
        ),
        "test-full-api": ([python, "-m", "pytest", "-q"], REPO_ROOT),
        "test-ui": ([str(resolve_ui_bin("vitest")), "run"], UI_ROOT),
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
