from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass


def _repo_root() -> pathlib.Path:
    current = pathlib.Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


REPO_ROOT = _repo_root()


@dataclass(frozen=True)
class ScanTarget:
    root: pathlib.Path
    suffixes: tuple[str, ...]
    excluded_parts: tuple[str, ...] = ()
    excluded_names: tuple[str, ...] = ()


def iter_files(target: ScanTarget) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for path in target.root.rglob("*"):
        if not path.is_file():
            continue
        if target.suffixes and path.suffix not in target.suffixes:
            continue
        relative_parts = path.relative_to(REPO_ROOT).parts
        if any(part in target.excluded_parts for part in relative_parts):
            continue
        if path.name in target.excluded_names:
            continue
        files.append(path)
    return files


def scan_forbidden_literals(
    label: str,
    target: ScanTarget,
    literals: tuple[str, ...],
) -> list[str]:
    findings: list[str] = []
    for path in iter_files(target):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_number, line in enumerate(text.splitlines(), start=1):
            for literal in literals:
                if literal not in line:
                    continue
                findings.append(
                    f"{label}: {path.relative_to(REPO_ROOT)}:{line_number}: contains {literal!r}"
                )
    return findings


def scan_forbidden_regex(
    label: str,
    target: ScanTarget,
    pattern: re.Pattern[str],
) -> list[str]:
    findings: list[str] = []
    for path in iter_files(target):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not pattern.search(line):
                continue
            findings.append(
                f"{label}: {path.relative_to(REPO_ROOT)}:{line_number}: matches {pattern.pattern!r}"
            )
    return findings


def main() -> int:
    findings: list[str] = []

    ui_source = ScanTarget(
        root=REPO_ROOT / "ui" / "src",
        suffixes=(".ts", ".tsx"),
        excluded_parts=("__tests__",),
    )
    findings.extend(
        scan_forbidden_literals(
            "legacy-ui-config",
            ui_source,
            (
                "__BACKTEST_UI_CONFIG__",
                "backtestApiBaseUrl",
                "VITE_BACKTEST_API_BASE_URL",
            ),
        )
    )
    findings.extend(
        scan_forbidden_literals(
            "legacy-ui-route",
            ui_source,
            (
                "/run-configurations",
                "/universe-configurations",
                "/ranking-configurations",
                "/data-admin/symbol-purge",
                "/strategy-exploration/data-catalog",
            ),
        )
    )

    services_source = ScanTarget(
        root=REPO_ROOT / "ui" / "src" / "services",
        suffixes=(".ts", ".tsx"),
        excluded_parts=("__tests__",),
        excluded_names=("apiService.ts",),
    )
    findings.extend(
        scan_forbidden_regex(
            "duplicate-ui-transport",
            services_source,
            re.compile(r"\bfetch\s*\("),
        )
    )

    findings.extend(
        scan_forbidden_literals(
            "legacy-api-route-doc",
            ScanTarget(
                root=REPO_ROOT / "api",
                suffixes=(".py", ".md"),
                excluded_parts=("__pycache__",),
            ),
            ("/finance/{report}",),
        )
    )
    findings.extend(
        scan_forbidden_literals(
            "legacy-runtime-auth-marker",
            ScanTarget(
                root=REPO_ROOT,
                suffixes=(".py", ".ps1", ".md", ".csv", ".template", ".js"),
                excluded_parts=(".git", ".venv", "node_modules", "__pycache__", "tests"),
                excluded_names=("check_ui_api_drift_guards.py", "validate_deploy_inputs.py"),
            ),
            (
                "ASSET_ALLOCATION_API_KEY",
                "__BACKTEST_UI_CONFIG__",
                "backtestApiBaseUrl",
                "VITE_BACKTEST_API_BASE_URL",
                "X-API-Key",
            ),
        )
    )
    findings.extend(
        scan_forbidden_regex(
            "legacy-runtime-auth-name",
            ScanTarget(
                root=REPO_ROOT,
                suffixes=(".py", ".ps1", ".md", ".csv", ".template", ".js"),
                excluded_parts=(".git", ".venv", "node_modules", "__pycache__", "tests"),
                excluded_names=("check_ui_api_drift_guards.py", "validate_deploy_inputs.py"),
            ),
            re.compile(r"\bAPI_KEY\b"),
        )
    )

    if findings:
        print("UI/API drift guard failed:")
        for item in findings:
            print(f" - {item}")
        return 1

    print("UI/API drift guard passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
