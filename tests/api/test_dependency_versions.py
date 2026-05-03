from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path


def test_installed_shared_packages_match_declared_versions() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]
    declared = {
        dependency.split("==", 1)[0]: dependency.split("==", 1)[1]
        for dependency in dependencies
        if dependency.startswith("asset-allocation-contracts==")
        or dependency.startswith("asset-allocation-runtime-common==")
    }

    assert importlib.metadata.version("asset-allocation-contracts") == declared["asset-allocation-contracts"]
    assert importlib.metadata.version("asset-allocation-runtime-common") == declared["asset-allocation-runtime-common"]
