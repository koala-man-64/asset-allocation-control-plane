from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_pyproject_pins_shared_packages() -> None:
    text = (repo_root() / "pyproject.toml").read_text(encoding="utf-8")
    assert 'asset-allocation-contracts==0.1.0' in text
    assert 'asset-allocation-runtime-common==0.1.0' in text


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


def test_compatibility_workflows_are_the_only_place_cross_repo_checkout_is_allowed() -> None:
    contracts_compat = (repo_root() / ".github" / "workflows" / "contracts-compat.yml").read_text(encoding="utf-8")
    runtime_common_compat = (
        repo_root() / ".github" / "workflows" / "runtime-common-compat.yml"
    ).read_text(encoding="utf-8")
    assert "Checkout contracts repository" in contracts_compat
    assert "Checkout runtime-common repository" in runtime_common_compat
