#!/usr/bin/env python3
"""Dependency governance utilities.

Enforces a single runtime dependency source-of-truth (pyproject.toml),
keeps requirements manifests aligned, and emits machine-readable reports
for CI gates.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

PINNED_REQ_RE = re.compile(r"^([A-Za-z0-9_.-]+)==([^\s;#]+)$")
QUOTED_VALUE_RE = re.compile(r'"([^"]+)"')


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_requirements_file(path: Path) -> Tuple[Dict[str, str], List[str], List[str], List[str]]:
    pinned: Dict[str, str] = {}
    duplicates: List[str] = []
    malformed: List[str] = []
    unpinned: List[str] = []

    for lineno, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        candidate = raw_line.split("#", 1)[0].strip()
        if not candidate:
            continue

        match = PINNED_REQ_RE.match(candidate)
        if not match:
            if "==" in candidate:
                malformed.append(f"{path}:{lineno}: {candidate}")
            else:
                unpinned.append(f"{path}:{lineno}: {candidate}")
            continue

        package_name = normalize_name(match.group(1))
        package_version = match.group(2).strip()

        if package_name in pinned:
            duplicates.append(
                f"{path}:{lineno}: {package_name}=={package_version} duplicates {package_name}=={pinned[package_name]}"
            )
            continue

        pinned[package_name] = package_version

    return pinned, duplicates, malformed, unpinned


def _extract_project_dependencies_block(pyproject_text: str, pyproject_path: Path) -> List[str]:
    lines = pyproject_text.splitlines()
    in_project = False
    in_dependencies = False
    dependency_entries: List[str] = []

    for raw_line in lines:
        line = raw_line.strip()

        if line.startswith("[") and line.endswith("]"):
            if line == "[project]":
                in_project = True
                continue
            if in_project and in_dependencies:
                break
            in_project = False

        if not in_project:
            continue

        if not in_dependencies:
            if line.startswith("dependencies") and "[" in line:
                in_dependencies = True
                after_bracket = raw_line.split("[", 1)[1]
                if "]" in after_bracket:
                    segment = after_bracket.split("]", 1)[0]
                    dependency_entries.extend(QUOTED_VALUE_RE.findall(segment))
                    break
                dependency_entries.extend(QUOTED_VALUE_RE.findall(after_bracket))
            continue

        if "]" in raw_line:
            segment = raw_line.split("]", 1)[0]
            dependency_entries.extend(QUOTED_VALUE_RE.findall(segment))
            break

        dependency_entries.extend(QUOTED_VALUE_RE.findall(raw_line))

    if not dependency_entries:
        raise ValueError(f"Unable to locate [project].dependencies in {pyproject_path}")

    return dependency_entries


def parse_pyproject_runtime_dependencies(pyproject_path: Path) -> Tuple[List[str], Dict[str, str], List[str], List[str]]:
    raw_entries = _extract_project_dependencies_block(pyproject_path.read_text(encoding="utf-8"), pyproject_path)

    ordered_entries: List[str] = []
    pinned: Dict[str, str] = {}
    duplicates: List[str] = []
    malformed: List[str] = []

    for entry in raw_entries:
        candidate = entry.strip()
        match = PINNED_REQ_RE.match(candidate)
        if not match:
            malformed.append(f"{pyproject_path}: dependency must be pinned with == : {candidate}")
            continue

        package_name = normalize_name(match.group(1))
        package_version = match.group(2).strip()

        if package_name in pinned:
            duplicates.append(
                f"{pyproject_path}: {package_name}=={package_version} duplicates {package_name}=={pinned[package_name]}"
            )
            continue

        pinned[package_name] = package_version
        ordered_entries.append(f"{match.group(1)}=={package_version}")

    return ordered_entries, pinned, duplicates, malformed


def diff_dependency_sets(expected: Dict[str, str], observed: Dict[str, str], expected_label: str, observed_label: str) -> List[str]:
    issues: List[str] = []

    missing = sorted(set(expected) - set(observed))
    extra = sorted(set(observed) - set(expected))

    for package_name in missing:
        issues.append(
            f"Missing in {observed_label}: {package_name}=={expected[package_name]} (present in {expected_label})"
        )

    for package_name in extra:
        issues.append(
            f"Unexpected in {observed_label}: {package_name}=={observed[package_name]} (not in {expected_label})"
        )

    common = sorted(set(expected) & set(observed))
    for package_name in common:
        if expected[package_name] != observed[package_name]:
            issues.append(
                f"Version mismatch for {package_name}: {expected_label}={expected[package_name]} vs {observed_label}={observed[package_name]}"
            )

    return issues


def write_runtime_requirements(runtime_entries: List[str], requirements_path: Path, lock_path: Path) -> List[str]:
    content = "\n".join(runtime_entries) + "\n"
    changed: List[str] = []

    for path in (requirements_path, lock_path):
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current != content:
            path.write_text(content, encoding="utf-8")
            changed.append(str(path))

    return changed


def build_report(
    status: str,
    summary: Dict[str, int],
    findings: List[str],
    runtime_entries: List[str],
    requirements_path: Path,
    lock_path: Path,
    dev_lock_path: Path,
) -> Dict[str, object]:
    return {
        "status": status,
        "summary": summary,
        "findings": findings,
        "runtime_source_of_truth": "pyproject.toml:[project].dependencies",
        "runtime_dependency_count": len(runtime_entries),
        "files": {
            "requirements": str(requirements_path),
            "runtime_lock": str(lock_path),
            "dev_lock": str(dev_lock_path),
        },
    }


def command_check(args: argparse.Namespace) -> int:
    runtime_entries, pyproject_pinned, pyproject_duplicates, pyproject_malformed = parse_pyproject_runtime_dependencies(
        args.pyproject
    )
    req_pinned, req_duplicates, req_malformed, req_unpinned = parse_requirements_file(args.requirements)
    lock_pinned, lock_duplicates, lock_malformed, lock_unpinned = parse_requirements_file(args.lock)
    dev_lock_pinned, dev_lock_duplicates, dev_lock_malformed, dev_lock_unpinned = parse_requirements_file(args.dev_lock)

    findings: List[str] = []
    findings.extend(pyproject_duplicates)
    findings.extend(pyproject_malformed)
    findings.extend(req_duplicates)
    findings.extend(req_malformed)
    findings.extend(req_unpinned)
    findings.extend(lock_duplicates)
    findings.extend(lock_malformed)
    findings.extend(lock_unpinned)
    findings.extend(dev_lock_duplicates)
    findings.extend(dev_lock_malformed)
    findings.extend(dev_lock_unpinned)

    findings.extend(
        diff_dependency_sets(
            expected=pyproject_pinned,
            observed=req_pinned,
            expected_label="pyproject.toml [project].dependencies",
            observed_label=str(args.requirements),
        )
    )
    findings.extend(
        diff_dependency_sets(
            expected=req_pinned,
            observed=lock_pinned,
            expected_label=str(args.requirements),
            observed_label=str(args.lock),
        )
    )

    status = "pass" if not findings else "fail"
    summary = {
        "finding_count": len(findings),
        "pyproject_dependencies": len(pyproject_pinned),
        "requirements_dependencies": len(req_pinned),
        "runtime_lock_dependencies": len(lock_pinned),
        "dev_lock_dependencies": len(dev_lock_pinned),
    }

    report = build_report(
        status=status,
        summary=summary,
        findings=findings,
        runtime_entries=runtime_entries,
        requirements_path=args.requirements,
        lock_path=args.lock,
        dev_lock_path=args.dev_lock,
    )

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if status == "pass":
        print(
            f"Dependency governance check passed. Runtime dependencies={len(pyproject_pinned)}; dev lock dependencies={len(dev_lock_pinned)}"
        )
        return 0

    print("Dependency governance check failed with findings:")
    for finding in findings:
        print(f"- {finding}")
    return 1


def command_sync(args: argparse.Namespace) -> int:
    runtime_entries, pyproject_pinned, pyproject_duplicates, pyproject_malformed = parse_pyproject_runtime_dependencies(
        args.pyproject
    )

    findings: List[str] = []
    findings.extend(pyproject_duplicates)
    findings.extend(pyproject_malformed)

    if findings:
        print("Cannot sync runtime requirements due to pyproject issues:")
        for finding in findings:
            print(f"- {finding}")
        return 1

    changed = write_runtime_requirements(runtime_entries, args.requirements, args.lock)
    if changed:
        print(f"Synchronized runtime requirement manifests from pyproject ({len(pyproject_pinned)} dependencies):")
        for path in changed:
            print(f"- {path}")
    else:
        print("Runtime requirement manifests already synchronized.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Runtime dependency governance checks and sync utilities.")
    parser.set_defaults(func=None)

    common_parent = argparse.ArgumentParser(add_help=False)
    common_parent.add_argument(
        "--pyproject",
        type=Path,
        default=Path("pyproject.toml"),
        help="Path to pyproject.toml containing [project].dependencies",
    )
    common_parent.add_argument(
        "--requirements",
        type=Path,
        default=Path("requirements.txt"),
        help="Path to runtime requirements file",
    )
    common_parent.add_argument(
        "--lock",
        type=Path,
        default=Path("requirements.lock.txt"),
        help="Path to runtime lock requirements file",
    )

    check_parser = parser.add_subparsers(dest="command", required=True)

    check = check_parser.add_parser("check", parents=[common_parent], help="Validate dependency governance invariants")
    check.add_argument(
        "--dev-lock",
        type=Path,
        default=Path("requirements-dev.lock.txt"),
        help="Path to development lock requirements file",
    )
    check.add_argument(
        "--report",
        type=Path,
        default=Path("artifacts/dependency_governance_report.json"),
        help="Path to JSON report output",
    )
    check.set_defaults(func=command_check)

    sync = check_parser.add_parser("sync", parents=[common_parent], help="Sync runtime requirements from pyproject")
    sync.set_defaults(func=command_sync)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        return args.func(args)
    except FileNotFoundError as exc:
        print(f"Missing file: {exc}")
        return 2
    except ValueError as exc:
        print(str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
