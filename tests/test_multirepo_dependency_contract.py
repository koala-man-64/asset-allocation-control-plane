from __future__ import annotations

from pathlib import Path
import tomllib


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def shared_dependencies() -> dict[str, str]:
    pyproject = tomllib.loads((repo_root() / "pyproject.toml").read_text(encoding="utf-8"))
    shared: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        if dependency.startswith("asset-allocation-"):
            name, version = dependency.split("==", 1)
            shared[name] = version
    return shared


def test_pyproject_pins_shared_packages() -> None:
    shared = shared_dependencies()
    assert shared["asset-allocation-contracts"]
    assert shared["asset-allocation-runtime-common"]


def test_api_dockerfile_does_not_copy_sibling_repos() -> None:
    text = (repo_root() / "Dockerfile.asset_allocation_api").read_text(encoding="utf-8")
    assert "COPY asset-allocation-contracts/" not in text
    assert "COPY asset-allocation-runtime-common/" not in text
    assert '"asset-allocation-contracts==${CONTRACTS_VERSION}"' in text
    assert '"asset-allocation-runtime-common==${RUNTIME_COMMON_VERSION}"' in text


def test_normal_ci_and_release_workflows_do_not_checkout_sibling_repos() -> None:
    for name in ("ci.yml", "release.yml"):
        text = (repo_root() / ".github" / "workflows" / name).read_text(encoding="utf-8")
        assert "Checkout contracts repository" not in text
        assert "Checkout runtime-common repository" not in text


def test_compatibility_workflow_is_the_only_place_cross_repo_checkout_is_allowed() -> None:
    compat = (repo_root() / ".github" / "workflows" / "compat.yml").read_text(encoding="utf-8")
    assert "Checkout shared dependency repository" in compat
    assert "asset-allocation-contracts" in compat
    assert "asset-allocation-runtime-common" in compat


def test_contracts_release_dispatch_pins_current_manifest() -> None:
    compat = (repo_root() / ".github" / "workflows" / "compat.yml").read_text(encoding="utf-8")
    assert "DISPATCH_CONTRACTS_VERSION" in compat
    assert "Pin released contracts version" in compat
    assert "client_payload.contracts_version" in compat
    assert "git push origin HEAD:${{ steps.target.outputs.current_repo_ref }}" in compat


def test_compatibility_workflow_uses_defined_dispatch_action_context() -> None:
    compat = (repo_root() / ".github" / "workflows" / "compat.yml").read_text(encoding="utf-8")
    assert "GITHUB_EVENT_ACTION" not in compat
    assert "DISPATCH_EVENT_ACTION" in compat
    assert "github.event.action" in compat
