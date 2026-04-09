from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_control_plane_has_only_current_runtime_workflows() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    expected = {
        "ci.yml",
        "contracts-compat.yml",
        "deploy-prod.yml",
        "infra-shared-prod.yml",
        "release.yml",
        "runtime-common-compat.yml",
        "security.yml",
    }
    assert {path.name for path in workflow_dir.glob("*.yml")} == expected


def test_deploy_setup_references_local_control_plane_bootstrap_paths() -> None:
    text = (repo_root() / "DEPLOYMENT_SETUP.md").read_text(encoding="utf-8")
    assert "..\\asset-allocation\\scripts\\provision_azure.ps1" not in text
    assert ".\\scripts\\ops\\provision\\provision_azure.ps1" in text
    assert ".\\scripts\\ops\\provision\\provision_entra_oidc.ps1" in text
    assert ".\\scripts\\ops\\validate\\validate_azure_permissions.ps1" in text
