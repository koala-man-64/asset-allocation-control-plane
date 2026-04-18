from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_dependency_governance_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "repo" / "dependency_governance.py"
    spec = importlib.util.spec_from_file_location("dependency_governance_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_get_exact_requires_dist_version_returns_exact_pin() -> None:
    dependency_governance = _load_dependency_governance_module()
    metadata_text = "\n".join(
        [
            "Metadata-Version: 2.4",
            "Requires-Dist: asset-allocation-contracts==2.1.0",
            "Requires-Dist: pytest==8.4.2; extra == 'test'",
        ]
    )

    version = dependency_governance.get_exact_requires_dist_version(
        metadata_text,
        "asset-allocation-contracts",
    )

    assert version == "2.1.0"


def test_validate_shared_dependency_compatibility_reports_version_skew() -> None:
    dependency_governance = _load_dependency_governance_module()

    incompatibility = dependency_governance.validate_shared_dependency_compatibility(
        {
            "asset-allocation-contracts": "2.1.0",
            "asset-allocation-runtime-common": "2.0.5",
        },
        "Requires-Dist: asset-allocation-contracts==0.0.0\n",
    )

    assert incompatibility is not None
    assert "asset-allocation-contracts==2.1.0" in incompatibility
    assert "asset-allocation-runtime-common==2.0.5" in incompatibility
    assert "asset-allocation-contracts==0.0.0" in incompatibility


def test_validate_shared_dependency_compatibility_accepts_matching_versions() -> None:
    dependency_governance = _load_dependency_governance_module()

    incompatibility = dependency_governance.validate_shared_dependency_compatibility(
        {
            "asset-allocation-contracts": "2.1.0",
            "asset-allocation-runtime-common": "2.0.5",
        },
        "Requires-Dist: asset-allocation-contracts==2.1.0\n",
    )

    assert incompatibility is None
