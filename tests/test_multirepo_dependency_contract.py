from __future__ import annotations

from pathlib import Path
import re
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


def test_python_dependency_manifests_stay_in_sync() -> None:
    shared = shared_dependencies()
    requirements = (repo_root() / "requirements.txt").read_text(encoding="utf-8")
    lockfile = (repo_root() / "requirements.lock.txt").read_text(encoding="utf-8")
    assert shared["asset-allocation-contracts"]
    assert shared["asset-allocation-runtime-common"]
    assert "asset-allocation-contracts==" not in requirements
    assert "asset-allocation-contracts==" not in lockfile
    assert "asset-allocation-runtime-common==" not in requirements
    assert "asset-allocation-runtime-common==" not in lockfile


def test_api_dockerfile_does_not_copy_sibling_repos() -> None:
    shared = shared_dependencies()
    text = (repo_root() / "Dockerfile.asset_allocation_api").read_text(encoding="utf-8")
    assert "COPY asset-allocation-contracts/" not in text
    assert "COPY asset-allocation-runtime-common/" not in text
    assert '"asset-allocation-contracts==${CONTRACTS_VERSION}"' in text
    assert '"asset-allocation-runtime-common==${RUNTIME_COMMON_VERSION}"' in text
    contracts_arg = re.search(r"^ARG CONTRACTS_VERSION=([^\r\n]+)$", text, re.MULTILINE)
    runtime_common_arg = re.search(r"^ARG RUNTIME_COMMON_VERSION=([^\r\n]+)$", text, re.MULTILINE)
    assert contracts_arg is not None
    assert runtime_common_arg is not None
    assert contracts_arg.group(1) == shared["asset-allocation-contracts"]
    assert runtime_common_arg.group(1) == shared["asset-allocation-runtime-common"]


def test_api_dockerfile_copies_first_party_packages_needed_at_boot() -> None:
    text = (repo_root() / "Dockerfile.asset_allocation_api").read_text(encoding="utf-8")

    required_copy_lines = (
        "COPY asset-allocation-control-plane/alpha_vantage/ alpha_vantage/",
        "COPY asset-allocation-control-plane/core/ core/",
        "COPY asset-allocation-control-plane/etrade_provider/ etrade_provider/",
        "COPY asset-allocation-control-plane/massive_provider/ massive_provider/",
        "COPY asset-allocation-control-plane/monitoring/ monitoring/",
        "COPY asset-allocation-control-plane/quiver_provider/ quiver_provider/",
        "COPY asset-allocation-control-plane/schwab/ schwab/",
        "COPY asset-allocation-control-plane/api/ api/",
    )
    for copy_line in required_copy_lines:
        assert copy_line in text


def test_api_dockerfile_runs_as_non_root() -> None:
    text = (repo_root() / "Dockerfile.asset_allocation_api").read_text(encoding="utf-8")

    assert "useradd --system" in text
    assert "chown -R app:app /app" in text
    assert re.search(r"^USER app$", text, re.MULTILINE)


def test_readme_shared_package_setup_examples_match_pyproject() -> None:
    shared = shared_dependencies()
    text = (repo_root() / "README.md").read_text(encoding="utf-8")

    assert f"asset-allocation-contracts=={shared['asset-allocation-contracts']}" in text
    assert f"asset-allocation-runtime-common=={shared['asset-allocation-runtime-common']}" in text


def test_release_workflow_uses_dependency_governance_for_shared_versions() -> None:
    text = (repo_root() / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")

    assert "scripts/repo/dependency_governance.py" in text
    assert "emit-shared-versions" in text


def test_normal_ci_and_release_workflows_do_not_checkout_sibling_repos() -> None:
    for path in (repo_root() / ".github" / "workflows").glob("*.yml"):
        text = path.read_text(encoding="utf-8")
        assert "Checkout contracts repository" not in text
        assert "Checkout runtime-common repository" not in text


def test_setup_action_validates_shared_package_compatibility_before_install() -> None:
    action = (repo_root() / ".github" / "actions" / "setup-control-plane-python" / "action.yml").read_text(
        encoding="utf-8"
    )
    assert "check-shared-compat" in action
    assert '--requirements "${repo_path}/shared-python-deps.txt"' in action
    assert '--pyproject "${repo_path}/pyproject.toml"' in action
    assert "--allow-newer-contracts" in action
    assert 'python -m pip install --no-deps -r "${repo_path}/shared-python-deps.txt"' in action
    assert "pip-check" in action


def test_control_plane_workflows_do_not_consume_shared_release_dispatches() -> None:
    forbidden_events = ("contracts" "_released", "runtime_common" "_released")
    for path in (repo_root() / ".github" / "workflows").glob("*.yml"):
        text = path.read_text(encoding="utf-8")
        for event_name in forbidden_events:
            assert event_name not in text
