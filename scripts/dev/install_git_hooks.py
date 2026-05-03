#!/usr/bin/env python3
from __future__ import annotations

import subprocess
from pathlib import Path


def _repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists() and (candidate / ".githooks").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


def main() -> int:
    repo_root = _repo_root()
    hooks_path = ".githooks"
    subprocess.run(
        ["git", "config", "core.hooksPath", hooks_path],
        cwd=str(repo_root),
        check=True,
    )
    print(f"Configured git hooks for this clone: core.hooksPath={hooks_path}")
    print("The pre-commit hook now checks that api/contracts/* is current before commits to main.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
