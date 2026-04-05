#!/usr/bin/env python3
"""List and validate Code Drift Sentinel acceptance scenarios."""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Any

import yaml

REQUIRED_TOP_LEVEL = [
    "id",
    "title",
    "setup",
    "run",
    "expected_findings",
    "expected_remediation",
]

REQUIRED_FINDING_FIELDS = ["category", "severity"]

ALLOWED_SEVERITY = {"low", "medium", "high", "critical"}


def load_scenarios(root: pathlib.Path) -> list[tuple[pathlib.Path, dict[str, Any]]]:
    scenarios: list[tuple[pathlib.Path, dict[str, Any]]] = []
    for path in sorted(root.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            data = {}
        scenarios.append((path, data))
    return scenarios


def validate_scenario(path: pathlib.Path, scenario: dict[str, Any]) -> list[str]:
    issues: list[str] = []

    for field in REQUIRED_TOP_LEVEL:
        if field not in scenario:
            issues.append(f"missing field `{field}`")

    findings = scenario.get("expected_findings", [])
    if not isinstance(findings, list) or not findings:
        issues.append("`expected_findings` must be a non-empty list")
    else:
        for index, finding in enumerate(findings):
            if not isinstance(finding, dict):
                issues.append(f"expected_findings[{index}] must be an object")
                continue
            for field in REQUIRED_FINDING_FIELDS:
                if field not in finding:
                    issues.append(f"expected_findings[{index}] missing `{field}`")
            severity = str(finding.get("severity", "")).lower()
            if severity and severity not in ALLOWED_SEVERITY:
                issues.append(f"expected_findings[{index}] invalid severity `{severity}`")

    run_steps = scenario.get("run", [])
    if isinstance(run_steps, list):
        if not any("codedrift_sentinel.py" in str(step) for step in run_steps):
            issues.append("`run` does not invoke codedrift_sentinel.py")
    else:
        issues.append("`run` must be a list of commands")

    return [f"{path.name}: {issue}" for issue in issues]


def print_scenario(path: pathlib.Path, scenario: dict[str, Any]) -> None:
    print(f"- {scenario.get('id', '<missing-id>')} :: {scenario.get('title', '<missing-title>')} ({path.name})")


def main() -> int:
    parser = argparse.ArgumentParser(description="List/validate Code Drift Sentinel scenario definitions.")
    parser.add_argument(
        "--scenarios-dir",
        default=".codex/skills/code-drift-sentinel/codedrift_scenarios",
        help="Path to scenario yaml directory",
    )
    parser.add_argument("--list", action="store_true", help="List scenarios")
    parser.add_argument("--validate", action="store_true", help="Validate scenario structure")
    args = parser.parse_args()

    scenario_root = pathlib.Path(args.scenarios_dir).resolve()
    if not scenario_root.exists():
        print(f"[ERROR] scenarios dir not found: {scenario_root}", file=sys.stderr)
        return 1

    scenarios = load_scenarios(scenario_root)
    if not scenarios:
        print("[ERROR] no scenario yaml files found", file=sys.stderr)
        return 1

    if args.list or not args.validate:
        print(f"Scenarios ({len(scenarios)}):")
        for path, scenario in scenarios:
            print_scenario(path, scenario)

    if args.validate:
        issues: list[str] = []
        for path, scenario in scenarios:
            issues.extend(validate_scenario(path, scenario))
        if issues:
            print("Validation failed:")
            for issue in issues:
                print(f"- {issue}")
            return 1
        print("Scenario validation passed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
