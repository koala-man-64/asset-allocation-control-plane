from __future__ import annotations

from pathlib import Path
import yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_control_plane_has_only_current_runtime_workflows() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    expected = {
        "ci.yml",
        "compat.yml",
        "deploy-prod.yml",
        "infra-shared-prod.yml",
        "release.yml",
        "security.yml",
    }
    assert {path.name for path in workflow_dir.glob("*.yml")} == expected


def test_deploy_setup_references_local_control_plane_bootstrap_paths() -> None:
    text = (repo_root() / "DEPLOYMENT_SETUP.md").read_text(encoding="utf-8")
    assert "..\\asset-allocation\\scripts\\provision_azure.ps1" not in text
    assert ".\\scripts\\ops\\provision\\provision_azure.ps1" in text
    assert ".\\scripts\\ops\\provision\\provision_entra_oidc.ps1" in text
    assert ".\\scripts\\ops\\validate\\validate_azure_permissions.ps1" in text


def _load_dependabot_config() -> dict[str, object]:
    text = (repo_root() / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    loaded = yaml.safe_load(text)
    assert isinstance(loaded, dict)
    return loaded


def _resolve_dependabot_directory(directory: str) -> Path:
    normalized = directory.strip("/")
    return repo_root() / normalized if normalized else repo_root()


def _has_supported_manifest(target_dir: Path, ecosystem: str) -> bool:
    if ecosystem == "github-actions":
        workflow_dir = target_dir / ".github" / "workflows"
        return workflow_dir.is_dir() and any(workflow_dir.glob("*.yml"))
    if ecosystem == "pip":
        candidates = (
            "pyproject.toml",
            "requirements.txt",
            "requirements-dev.txt",
            "requirements.lock.txt",
            "requirements-dev.lock.txt",
        )
        return any((target_dir / candidate).exists() for candidate in candidates)
    if ecosystem == "npm":
        return (target_dir / "package.json").exists()
    raise AssertionError(f"Unsupported dependabot ecosystem in repo test: {ecosystem}")


def test_dependabot_updates_only_reference_existing_manifests() -> None:
    config = _load_dependabot_config()
    updates = config["updates"]
    assert isinstance(updates, list)

    for update in updates:
        assert isinstance(update, dict)
        ecosystem = update["package-ecosystem"]
        directory = update["directory"]
        assert isinstance(ecosystem, str)
        assert isinstance(directory, str)

        target_dir = _resolve_dependabot_directory(directory)
        assert target_dir.exists(), f"Dependabot directory does not exist: {directory}"
        assert _has_supported_manifest(target_dir, ecosystem), (
            f"Dependabot ecosystem {ecosystem} does not have a supported manifest in {directory}"
        )


def test_dependabot_ignores_cross_repo_owned_python_runtime_bumps() -> None:
    config = _load_dependabot_config()
    updates = config["updates"]
    assert isinstance(updates, list)

    pip_updates = [update for update in updates if isinstance(update, dict) and update.get("package-ecosystem") == "pip"]
    assert len(pip_updates) == 1

    ignored_dependencies = {
        entry["dependency-name"]
        for entry in pip_updates[0].get("ignore", [])
        if isinstance(entry, dict) and isinstance(entry.get("dependency-name"), str)
    }

    assert {"azure-identity", "pydantic"} <= ignored_dependencies


def test_ci_preserves_dependency_governance_gate() -> None:
    text = (repo_root() / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "dependency-governance:" in text
    assert "python scripts/repo/dependency_governance.py check" in text


def test_release_workflow_runs_preflight_before_export_and_build() -> None:
    text = (repo_root() / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    ordered_markers = [
        "- name: Validate required release configuration",
        "- name: Resolve shared package versions",
        "- name: Verify shared package availability",
        "- name: Azure login",
        "- name: Validate release Azure prerequisites",
        "- name: Export contract artifacts",
        "- name: Build and push API image",
    ]
    positions = [text.index(marker) for marker in ordered_markers]
    assert positions == sorted(positions)
    assert "-Scenario Release" in text
