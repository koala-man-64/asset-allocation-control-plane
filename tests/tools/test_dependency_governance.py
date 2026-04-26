from __future__ import annotations

import contextlib
import io
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace


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
            "Requires-Dist: asset-allocation-contracts==3.0.0",
            "Requires-Dist: pytest==8.4.2; extra == 'test'",
        ]
    )

    version = dependency_governance.get_exact_requires_dist_version(
        metadata_text,
        "asset-allocation-contracts",
    )

    assert version == "3.0.0"


def test_validate_shared_dependency_compatibility_reports_version_skew() -> None:
    dependency_governance = _load_dependency_governance_module()

    incompatibility = dependency_governance.validate_shared_dependency_compatibility(
        {
            "asset-allocation-contracts": "3.0.0",
            "asset-allocation-runtime-common": "3.1.0",
        },
        "Requires-Dist: asset-allocation-contracts==0.0.0\n",
    )

    assert incompatibility is not None
    assert "asset-allocation-contracts==3.0.0" in incompatibility
    assert "asset-allocation-runtime-common==3.1.0" in incompatibility
    assert "asset-allocation-contracts==0.0.0" in incompatibility


def test_validate_shared_dependency_compatibility_can_allow_newer_contracts() -> None:
    dependency_governance = _load_dependency_governance_module()

    incompatibility = dependency_governance.validate_shared_dependency_compatibility(
        {
            "asset-allocation-contracts": "3.10.0",
            "asset-allocation-runtime-common": "3.4.5",
        },
        "Requires-Dist: asset-allocation-contracts==3.7.0\n",
        allow_newer_contracts=True,
    )

    assert incompatibility is None


def test_validate_shared_dependency_compatibility_accepts_matching_versions() -> None:
    dependency_governance = _load_dependency_governance_module()

    incompatibility = dependency_governance.validate_shared_dependency_compatibility(
        {
            "asset-allocation-contracts": "3.0.0",
            "asset-allocation-runtime-common": "3.1.0",
        },
        "Requires-Dist: asset-allocation-contracts==3.0.0\n",
    )

    assert incompatibility is None


def test_validate_shared_dependency_compatibility_reports_runtime_pin_skew() -> None:
    dependency_governance = _load_dependency_governance_module()

    incompatibility = dependency_governance.validate_shared_dependency_compatibility(
        {
            "asset-allocation-contracts": "3.0.0",
            "asset-allocation-runtime-common": "3.1.0",
        },
        "\n".join(
            [
                "Requires-Dist: asset-allocation-contracts==3.0.0",
                "Requires-Dist: python-dotenv==1.2.2",
                "Requires-Dist: pytest==9.0.3; extra == 'test'",
            ]
        ),
        runtime_pins={
            "python-dotenv": "1.2.1",
        },
    )

    assert incompatibility is not None
    assert "python-dotenv==1.2.1" in incompatibility
    assert "asset-allocation-runtime-common==3.1.0" in incompatibility
    assert "python-dotenv==1.2.2" in incompatibility


def test_read_shared_version_matrix_reads_exact_versions(tmp_path: Path) -> None:
    dependency_governance = _load_dependency_governance_module()
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        "\n".join(
            [
                "[project]",
                'name = "asset-allocation-control-plane"',
                'version = "0.1.0"',
                "dependencies = [",
                '    "asset-allocation-contracts==3.0.0",',
                '    "asset-allocation-runtime-common==3.1.0",',
                '    "fastapi==0.133.1",',
                "]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    version_matrix = dependency_governance.read_shared_version_matrix(pyproject_path)

    assert version_matrix == {
        "contracts_version": "3.0.0",
        "runtime_common_version": "3.1.0",
        "control_plane_version": "0.1.0",
    }


def test_command_emit_shared_versions_writes_env_lines(tmp_path: Path) -> None:
    dependency_governance = _load_dependency_governance_module()
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        "\n".join(
            [
                "[project]",
                'name = "asset-allocation-control-plane"',
                'version = "0.2.0"',
                "dependencies = [",
                '    "asset-allocation-contracts==9.9.9",',
                '    "asset-allocation-runtime-common==8.8.8",',
                "]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = dependency_governance.command_emit_shared_versions(
            SimpleNamespace(pyproject=pyproject_path, format="env")
        )

    assert exit_code == 0
    assert stdout.getvalue().strip().splitlines() == [
        "contracts_version=9.9.9",
        "runtime_common_version=8.8.8",
        "control_plane_version=0.2.0",
    ]
