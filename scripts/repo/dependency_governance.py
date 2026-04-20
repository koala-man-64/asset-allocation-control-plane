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
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Dict, List, Tuple
import zipfile

PINNED_REQ_RE = re.compile(r"^([A-Za-z0-9_.-]+)==([^\s;#]+)$")
QUOTED_VALUE_RE = re.compile(r'"([^"]+)"')
FIRST_PARTY_SHARED_PREFIX = "asset-allocation-"


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def is_first_party_shared_package(name: str) -> bool:
    return normalize_name(name).startswith(FIRST_PARTY_SHARED_PREFIX)


def filter_installable_runtime_entries(runtime_entries: List[str]) -> List[str]:
    installable_entries: List[str] = []
    for entry in runtime_entries:
        package_name = entry.split("==", 1)[0].strip()
        if not is_first_party_shared_package(package_name):
            installable_entries.append(entry)
    return installable_entries


def filter_installable_runtime_pins(runtime_pins: Dict[str, str]) -> Dict[str, str]:
    return {
        package_name: version
        for package_name, version in runtime_pins.items()
        if not is_first_party_shared_package(package_name)
    }


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


def read_shared_version_matrix(pyproject_path: Path) -> Dict[str, str]:
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8")).get("project", {})
    dependencies = project.get("dependencies", [])
    shared_versions: Dict[str, str] = {}

    for dependency in dependencies:
        if not dependency.startswith("asset-allocation-"):
            continue
        name, version = dependency.split("==", 1)
        shared_versions[name] = version

    contracts_version = shared_versions.get("asset-allocation-contracts")
    runtime_common_version = shared_versions.get("asset-allocation-runtime-common")
    control_plane_version = project.get("version")

    if not contracts_version or not runtime_common_version or not control_plane_version:
        raise ValueError(
            "Unable to resolve shared version matrix from pyproject.toml. "
            "Expected project.version plus exact pins for asset-allocation-contracts "
            "and asset-allocation-runtime-common."
        )

    return {
        "contracts_version": contracts_version,
        "runtime_common_version": runtime_common_version,
        "control_plane_version": str(control_plane_version),
    }


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
    installable_runtime_entries: List[str],
    requirements_path: Path,
    lock_path: Path,
    dev_lock_path: Path,
) -> Dict[str, object]:
    excluded_runtime_entries = [entry for entry in runtime_entries if entry not in installable_runtime_entries]
    return {
        "status": status,
        "summary": summary,
        "findings": findings,
        "runtime_source_of_truth": "pyproject.toml:[project].dependencies",
        "runtime_dependency_count": len(runtime_entries),
        "installable_runtime_dependency_count": len(installable_runtime_entries),
        "requirements_excluded_first_party_packages": excluded_runtime_entries,
        "files": {
            "requirements": str(requirements_path),
            "runtime_lock": str(lock_path),
            "dev_lock": str(dev_lock_path),
        },
    }


def read_wheel_metadata(wheel_path: Path) -> str:
    with zipfile.ZipFile(wheel_path) as wheel_archive:
        for archive_name in wheel_archive.namelist():
            if archive_name.endswith(".dist-info/METADATA"):
                return wheel_archive.read(archive_name).decode("utf-8")
    raise ValueError(f"Wheel metadata not found in {wheel_path}")


def get_exact_requires_dist_version(metadata_text: str, package_name: str) -> str | None:
    prefix = f"Requires-Dist: {package_name}=="
    for line in metadata_text.splitlines():
        if not line.startswith(prefix):
            continue
        version = line[len(prefix) :].split(";", 1)[0].strip()
        if version:
            return version
    return None


def validate_shared_dependency_compatibility(shared_pins: Dict[str, str], runtime_common_metadata: str) -> str | None:
    contracts_version = shared_pins.get("asset-allocation-contracts")
    runtime_common_version = shared_pins.get("asset-allocation-runtime-common")

    if not contracts_version or not runtime_common_version:
        return None

    required_contracts_version = get_exact_requires_dist_version(
        runtime_common_metadata,
        "asset-allocation-contracts",
    )
    if required_contracts_version is None:
        return (
            "Shared package compatibility check failed: "
            f"asset-allocation-runtime-common=={runtime_common_version} does not declare an exact "
            "asset-allocation-contracts requirement in its wheel metadata."
        )

    if required_contracts_version != contracts_version:
        return (
            "Shared package compatibility check failed: "
            f"pyproject pins asset-allocation-contracts=={contracts_version}, "
            f"but asset-allocation-runtime-common=={runtime_common_version} requires "
            f"asset-allocation-contracts=={required_contracts_version}. "
            "Publish or adopt a compatible runtime-common version before installing both packages together."
        )

    return None


def download_exact_wheel_metadata(requirement: str) -> str:
    with tempfile.TemporaryDirectory(prefix="shared-deps-preflight-") as temp_dir:
        download_dir = Path(temp_dir)
        command = [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--disable-pip-version-check",
            "--no-deps",
            "--dest",
            str(download_dir),
            requirement,
        ]
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        if completed.returncode != 0:
            raise ValueError(
                f"Unable to download {requirement} for shared package compatibility validation:\n"
                f"{completed.stdout}{completed.stderr}"
            )

        wheel_paths = sorted(download_dir.glob("*.whl"))
        if len(wheel_paths) != 1:
            raise ValueError(
                f"Expected exactly one wheel for {requirement}, found {len(wheel_paths)} in {download_dir}"
            )
        return read_wheel_metadata(wheel_paths[0])


def command_check_shared_compat(args: argparse.Namespace) -> int:
    shared_pins, duplicates, malformed, unpinned = parse_requirements_file(args.requirements)

    findings: List[str] = []
    findings.extend(duplicates)
    findings.extend(malformed)
    findings.extend(unpinned)
    if findings:
        print("Cannot validate shared package compatibility due to malformed requirements:")
        for finding in findings:
            print(f"- {finding}")
        return 1

    contracts_version = shared_pins.get("asset-allocation-contracts")
    runtime_common_version = shared_pins.get("asset-allocation-runtime-common")
    if not contracts_version or not runtime_common_version:
        print("Shared package compatibility check skipped: both contracts and runtime-common pins are not in scope.")
        return 0

    try:
        runtime_common_metadata = download_exact_wheel_metadata(
            f"asset-allocation-runtime-common=={runtime_common_version}"
        )
    except ValueError as exc:
        print(str(exc))
        return 1

    incompatibility = validate_shared_dependency_compatibility(shared_pins, runtime_common_metadata)
    if incompatibility:
        print(incompatibility)
        return 1

    print(
        "Verified shared package compatibility: "
        f"asset-allocation-runtime-common=={runtime_common_version} requires "
        f"asset-allocation-contracts=={contracts_version}."
    )
    return 0


def command_check(args: argparse.Namespace) -> int:
    runtime_entries, pyproject_pinned, pyproject_duplicates, pyproject_malformed = parse_pyproject_runtime_dependencies(
        args.pyproject
    )
    installable_runtime_entries = filter_installable_runtime_entries(runtime_entries)
    installable_pyproject_pinned = filter_installable_runtime_pins(pyproject_pinned)
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
            expected=installable_pyproject_pinned,
            observed=req_pinned,
            expected_label="pyproject.toml [project].dependencies excluding first-party shared packages",
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
        installable_runtime_entries=installable_runtime_entries,
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
    installable_runtime_entries = filter_installable_runtime_entries(runtime_entries)
    installable_pyproject_pinned = filter_installable_runtime_pins(pyproject_pinned)

    findings: List[str] = []
    findings.extend(pyproject_duplicates)
    findings.extend(pyproject_malformed)

    if findings:
        print("Cannot sync runtime requirements due to pyproject issues:")
        for finding in findings:
            print(f"- {finding}")
        return 1

    changed = write_runtime_requirements(installable_runtime_entries, args.requirements, args.lock)
    if changed:
        print(
            "Synchronized installable runtime requirement manifests from pyproject "
            f"({len(installable_pyproject_pinned)} dependencies):"
        )
        for path in changed:
            print(f"- {path}")
    else:
        print("Installable runtime requirement manifests already synchronized.")

    return 0


def command_emit_shared_versions(args: argparse.Namespace) -> int:
    version_matrix = read_shared_version_matrix(args.pyproject)

    if args.format == "json":
        print(json.dumps(version_matrix, sort_keys=True))
        return 0

    for key in ("contracts_version", "runtime_common_version", "control_plane_version"):
        print(f"{key}={version_matrix[key]}")
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

    shared_compat = check_parser.add_parser(
        "check-shared-compat",
        help="Validate first-party shared package pins against published wheel metadata",
    )
    shared_compat.add_argument(
        "--requirements",
        type=Path,
        default=Path("shared-python-deps.txt"),
        help="Path to the generated shared package requirements file",
    )
    shared_compat.set_defaults(func=command_check_shared_compat)

    emit_shared_versions = check_parser.add_parser(
        "emit-shared-versions",
        help="Emit the shared package and control-plane version matrix from pyproject.toml",
    )
    emit_shared_versions.add_argument(
        "--pyproject",
        type=Path,
        default=Path("pyproject.toml"),
        help="Path to pyproject.toml containing project.version and shared package pins",
    )
    emit_shared_versions.add_argument(
        "--format",
        choices=("env", "json"),
        default="env",
        help="Output format for the shared version matrix",
    )
    emit_shared_versions.set_defaults(func=command_emit_shared_versions)

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
