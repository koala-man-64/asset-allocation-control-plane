from __future__ import annotations

from pathlib import Path
import yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_control_plane_has_only_current_runtime_workflows() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    expected = {
        "ci.yml",
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

    pip_updates = [
        update for update in updates if isinstance(update, dict) and update.get("package-ecosystem") == "pip"
    ]
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


def test_ci_runs_architecture_and_facade_guards() -> None:
    text = (repo_root() / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "tests/architecture/test_python_module_boundaries.py" in text
    assert "tests/architecture/test_system_facade_guard.py" in text
    assert "tests/architecture/test_monitoring_facade_guard.py" in text
    assert "tests/test_deploy_manifests.py" in text


def test_ci_uses_shared_contract_artifact_gate() -> None:
    text = (repo_root() / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "python scripts/automation/run_quality_gate.py contract-artifacts" in text
    assert "git diff --exit-code -- api/contracts" not in text


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


def test_deploy_workflow_manual_runs_auto_resolve_latest_release_digest() -> None:
    text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:\n  repository_dispatch:" in text
    assert 'trigger_source="repository_dispatch deploy_runtime"' in text
    assert ': "${image_digest:?repository_dispatch client_payload.image_digest is required}"' in text
    assert "az acr repository show-manifests" in text
    assert '--repository "${RELEASE_IMAGE_REPOSITORY}"' in text
    assert 'image_digest="${ACR_LOGIN_SERVER}/${RELEASE_IMAGE_REPOSITORY}@${manifest_digest}"' in text
    assert "No released ${RELEASE_IMAGE_REPOSITORY} image found in ACR ${ACR_NAME}." in text
    assert "API_DEPLOY_MANIFEST: ${{ vars.API_DEPLOY_MANIFEST || 'deploy/app_api_public.yaml' }}" in text
    assert "ACA_NETWORK_SMOKE_JOB_NAME: ${{ vars.ACA_NETWORK_SMOKE_JOB_NAME || 'asset-allocation-network-smoke' }}" in text
    assert "python scripts/automation/render_control_plane_manifest.py \\" in text
    assert '--template "${API_DEPLOY_MANIFEST}" \\' in text
    assert "--output rendered-control-plane.yaml" in text
    assert 'expect_status 401 "https://${fqdn}/config.js"' in text
    assert 'expect_status 401 "https://${fqdn}/api/openapi.json"' in text
    assert 'expect_status 401 "https://${fqdn}/api/docs"' in text
    assert 'az account get-access-token \\' in text
    assert '--scope "api://${API_OIDC_AUDIENCE}/.default" \\' in text
    assert 'expect_status 307 -H "Authorization: Bearer ${deploy_smoke_token}" "https://${fqdn}/docs"' in text
    assert 'expect_status 307 -H "Authorization: Bearer ${deploy_smoke_token}" "https://${fqdn}/openapi.json"' in text
    assert '-H "Authorization: Bearer ${deploy_smoke_token}" \\' in text
    assert 'if [ "${API_DEPLOY_MANIFEST}" = "deploy/app_api.yaml" ]; then' in text
    assert "az containerapp job start \\" in text
    assert "az containerapp job execution show \\" in text
    assert 'verification_mode="internal-smoke-job"' in text
    assert "/api/v1/openapi.json" not in text


def test_deploy_workflow_exports_subscription_id_for_manifest_rendering() -> None:
    text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")
    assert "AZURE_SUBSCRIPTION_ID: ${{ vars.AZURE_SUBSCRIPTION_ID }}" in text
    assert "CONTAINER_APPS_ENVIRONMENT_ID: ${{ steps.azure.outputs.environment_id }}" in text
    assert "ACR_PULL_IDENTITY_CLIENT_ID: ${{ steps.azure.outputs.identity_client_id }}" in text


def test_deploy_workflow_includes_ai_relay_runtime_env_and_smoke_checks() -> None:
    text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")
    assert "DEPLOY_SMOKE_BEARER_TOKEN" not in text
    assert "AI_RELAY_ENABLED: ${{ vars.AI_RELAY_ENABLED || 'false' }}" in text
    assert "AI_RELAY_API_KEY: ${{ secrets.AI_RELAY_API_KEY }}" in text
    assert "/api/ai/chat/stream" in text
    assert "AI_RELAY_SMOKE_BEARER_TOKEN" in text
    assert "AI_RELAY_SMOKE_FORBIDDEN_BEARER_TOKEN" in text
