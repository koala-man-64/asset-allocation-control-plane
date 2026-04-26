from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

try:
    from packaging.requirements import Requirement
    from packaging.version import Version
except ModuleNotFoundError:
    from pip._vendor.packaging.requirements import Requirement
    from pip._vendor.packaging.version import Version


STABLE_SEMVER_PATTERN = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
SHARED_PACKAGES = (
    "asset-allocation-contracts",
    "asset-allocation-runtime-common",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve exact shared package versions from supported specs.")
    parser.add_argument("--pyproject", default="pyproject.toml", help="Path to the repo pyproject.toml.")
    parser.add_argument("--contracts-override", default="", help="Optional exact contracts version override.")
    parser.add_argument("--runtime-common-override", default="", help="Optional exact runtime-common version override.")
    return parser.parse_args()


def stable_key(version: str) -> tuple[int, int, int] | None:
    match = STABLE_SEMVER_PATTERN.fullmatch(version)
    if not match:
        return None
    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))


def list_published_versions(package_name: str) -> list[str]:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "index",
            "versions",
            package_name,
            "--json",
            "--disable-pip-version-check",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        output = result.stderr.strip() or result.stdout.strip() or "pip index failed without output."
        raise RuntimeError(f"Could not resolve published versions for {package_name}.\npip output:\n{output}")

    payload = json.loads(result.stdout)
    versions = payload.get("versions")
    if not isinstance(versions, list):
        raise RuntimeError(f"Configured package index did not return a versions list for {package_name}.")
    return [str(version) for version in versions]


def load_shared_dependency_specs(pyproject_path: Path) -> tuple[str, dict[str, str]]:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = pyproject["project"]
    dependencies: dict[str, str] = {}
    for dependency in project["dependencies"]:
        requirement = Requirement(dependency)
        if requirement.name in SHARED_PACKAGES:
            dependencies[requirement.name] = dependency

    missing = [name for name in SHARED_PACKAGES if name not in dependencies]
    if missing:
        raise RuntimeError(f"Missing shared dependency specs in {pyproject_path}: {', '.join(missing)}")

    return str(project["version"]), dependencies


def resolve_version(spec: str, override: str) -> str:
    requirement = Requirement(spec)
    stable_versions = [
        version for version in list_published_versions(requirement.name) if stable_key(version) is not None
    ]
    if not stable_versions:
        raise RuntimeError(f"No stable published versions were found for {requirement.name}.")

    if override:
        if stable_key(override) is None:
            raise RuntimeError(f"Override for {requirement.name} must be stable semver, got '{override}'.")
        if override not in stable_versions:
            raise RuntimeError(f"Override for {requirement.name} is not published: {override}.")
        if not requirement.specifier.contains(Version(override), prereleases=False):
            raise RuntimeError(f"Override for {requirement.name} does not satisfy supported spec {spec}: {override}.")
        return override

    compatible_versions = [
        version
        for version in stable_versions
        if requirement.specifier.contains(Version(version), prereleases=False)
    ]
    if not compatible_versions:
        raise RuntimeError(f"No published stable versions satisfy {spec}.")

    return max(compatible_versions, key=lambda version: stable_key(version) or (-1, -1, -1))


def main() -> None:
    args = parse_args()
    project_version, specs = load_shared_dependency_specs(Path(args.pyproject))
    contracts_version = resolve_version(specs["asset-allocation-contracts"], args.contracts_override.strip())
    runtime_common_version = resolve_version(
        specs["asset-allocation-runtime-common"],
        args.runtime_common_override.strip(),
    )

    print(f"contracts_version={contracts_version}")
    print(f"runtime_common_version={runtime_common_version}")
    print(f"control_plane_version={project_version}")


if __name__ == "__main__":
    main()
