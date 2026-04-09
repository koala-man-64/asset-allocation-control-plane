from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_setup_env_does_not_backfill_azure_client_id_from_runtime_identities() -> None:
    text = (repo_root() / "scripts" / "setup-env.ps1").read_text(encoding="utf-8")
    assert '$map["AZURE_CLIENT_ID"] = $clientId' not in text
    assert "Get-ContainerAppIdentityClientId -App $app -IdentityName $identityResolution.Value" not in text


def test_provision_azure_does_not_target_the_old_monolith_repo_subject() -> None:
    text = (repo_root() / "scripts" / "ops" / "provision" / "provision_azure.ps1").read_text(encoding="utf-8")
    assert "repo:koala-man-64/asset-allocation:environment:production" not in text
