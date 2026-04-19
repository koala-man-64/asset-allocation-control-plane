from __future__ import annotations

import csv
import os
import re
import stat
import subprocess
from pathlib import Path


ALLOWED_CLASSES = {"deploy_var", "secret"}
ALLOWED_STORAGE = {"var", "secret"}
ALLOWED_SOURCES = {"deploy_config", "secret_store"}
WORKFLOW_VAR_PATTERN = re.compile(r"\bvars\.([A-Z][A-Z0-9_]+)\b")
WORKFLOW_SECRET_PATTERN = re.compile(r"\bsecrets\.([A-Z][A-Z0-9_]+)\b")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def contract_rows() -> list[dict[str, str]]:
    path = repo_root() / "docs" / "ops" / "env-contract.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def contract_map() -> dict[str, dict[str, str]]:
    return {row["name"]: row for row in contract_rows()}


def env_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def workflow_refs(pattern: re.Pattern[str]) -> set[str]:
    refs: set[str] = set()
    for path in (repo_root() / ".github" / "workflows").glob("*.yml"):
        refs.update(pattern.findall(path.read_text(encoding="utf-8")))
    return refs


def powershell_exe() -> str:
    for candidate in ("pwsh", "powershell"):
        try:
            subprocess.run(
                [candidate, "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"],
                check=True,
                capture_output=True,
                text=True,
            )
            return candidate
        except Exception:
            continue
    raise AssertionError("PowerShell executable not found for setup-env dry-run test")


def write_stub_command(directory: Path, name: str, python_body: str) -> None:
    script_path = directory / f"{name}_stub.py"
    script_path.write_text(
        "import json\n"
        "import sys\n"
        "args = sys.argv[1:]\n"
        f"{python_body}\n",
        encoding="utf-8",
    )

    if os.name == "nt":
        wrapper_path = directory / f"{name}.cmd"
        wrapper_path.write_text(
            f'@echo off\r\npython "%~dp0{name}_stub.py" %*\r\n',
            encoding="utf-8",
        )
        return

    wrapper_path = directory / name
    wrapper_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f'python3 "$(dirname "$0")/{name}_stub.py" "$@"\n',
        encoding="utf-8",
    )
    wrapper_path.chmod(wrapper_path.stat().st_mode | stat.S_IEXEC)


def test_contract_rows_are_well_formed() -> None:
    rows = contract_rows()
    names = [row["name"] for row in rows]
    assert len(names) == len(set(names))
    for row in rows:
        assert row["class"] in ALLOWED_CLASSES
        assert row["github_storage"] in ALLOWED_STORAGE
        assert row["source_of_truth"] in ALLOWED_SOURCES
        assert row["template"] == "true"


def test_template_matches_contract_surface() -> None:
    assert env_keys(repo_root() / ".env.template") == set(contract_map())


def test_workflow_refs_are_documented() -> None:
    contract = contract_map()
    for name in workflow_refs(WORKFLOW_VAR_PATTERN):
        assert name in contract
        assert contract[name]["github_storage"] == "var"

    for name in workflow_refs(WORKFLOW_SECRET_PATTERN):
        assert name in contract
        assert contract[name]["github_storage"] == "secret"


def test_sync_script_reads_repo_local_contract() -> None:
    text = (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8")
    assert 'Join-Path $repoRoot "docs\\ops\\env-contract.csv"' in text
    assert 'Join-Path $repoRoot ".env.web"' in text
    assert "AI_RELAY_ENABLED" in text
    assert "AI_RELAY_API_KEY" in text


def test_sync_script_is_powershell_parseable() -> None:
    script = repo_root() / "scripts" / "sync-all-to-github.ps1"
    escaped_path = str(script).replace("'", "''")
    subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-Command",
            (
                f"$path='{escaped_path}'; "
                "$tokens=$null; $errors=$null; "
                "[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors) > $null; "
                "if ($errors.Count -gt 0) { $errors | ForEach-Object { $_.ToString() }; exit 1 }"
            ),
        ],
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )


def test_setup_env_dry_run_reports_sources_without_prompting() -> None:
    script = repo_root() / "scripts" / "setup-env.ps1"
    completed = subprocess.run(
        [powershell_exe(), "-NoProfile", "-File", str(script), "-DryRun"],
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout
    assert "requirement=" in stdout
    assert "suggested=" in stdout
    assert "source=" in stdout
    assert "prompt_required=" in stdout


def test_setup_env_uses_github_and_runtime_discovery_paths() -> None:
    text = (repo_root() / "scripts" / "setup-env.ps1").read_text(encoding="utf-8")
    assert '"repo", "view"' in text
    assert '"variable", "list"' in text
    assert '"containerapp", "show"' in text
    assert '"postgres", "flexible-server", "list"' in text
    assert '"postgres", "flexible-server", "db", "list"' in text
    assert 'function Get-ContainerAppRuntimeEnvMap' in text
    assert 'function Get-PreferredPostgresServer' in text
    assert 'function Get-PreferredPostgresDatabaseName' in text
    assert 'function Get-RequirementLevel' in text
    assert 'function Format-SuggestedDisplayValue' in text
    assert 'function Get-UiContainerAppName' in text
    assert 'Get-ContainerApp -AppName (Get-UiContainerAppName)' in text
    assert 'DispatchAppPrivateKeyFilePath' in text
    assert 'function Read-SecretFileValue' in text


def test_postgres_bootstrap_keys_are_part_of_env_contract() -> None:
    contract = contract_map()
    assert contract["POSTGRES_SERVER_NAME"]["github_storage"] == "var"
    assert contract["POSTGRES_DATABASE_NAME"]["github_storage"] == "var"
    assert contract["POSTGRES_ADMIN_USER"]["github_storage"] == "var"
    assert contract["POSTGRES_ADMIN_PASSWORD"]["github_storage"] == "secret"


def test_setup_env_can_read_dispatch_private_key_from_file(tmp_path: Path) -> None:
    key_path = tmp_path / "dispatch-app.pem"
    key_path.write_text(
        "-----BEGIN PRIVATE KEY-----\nline1\nline2\n-----END PRIVATE KEY-----\n",
        encoding="utf-8",
    )
    script = repo_root() / "scripts" / "setup-env.ps1"
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(script),
            "-DryRun",
            "-DispatchAppPrivateKeyFilePath",
            str(key_path),
        ],
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout
    assert "DISPATCH_APP_PRIVATE_KEY=<redacted>" in stdout
    assert "source=file" in stdout
    assert "prompt_required=false" in stdout


def test_setup_env_makes_ai_requirements_conditional(tmp_path: Path) -> None:
    script = repo_root() / "scripts" / "setup-env.ps1"
    env_file = tmp_path / "ai.env.web"
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(script),
            "-DryRun",
            "-EnvFilePath",
            str(env_file),
            "-Set",
            "AI_RELAY_ENABLED=true",
        ],
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout
    assert "AI_RELAY_API_KEY=<redacted> [requirement=required;" in stdout
    assert "AI_RELAY_REQUIRED_ROLES=AssetAllocation.AiRelay.Use [requirement=required;" in stdout


def test_ai_relay_smoke_tokens_are_documented_as_secrets() -> None:
    contract = contract_map()
    assert contract["DEPLOY_SMOKE_BEARER_TOKEN"]["github_storage"] == "secret"
    assert contract["AI_RELAY_SMOKE_BEARER_TOKEN"]["github_storage"] == "secret"
    assert contract["AI_RELAY_SMOKE_FORBIDDEN_BEARER_TOKEN"]["github_storage"] == "secret"


def test_setup_env_dry_run_tolerates_mixed_shape_discovery_results(tmp_path: Path) -> None:
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()

    write_stub_command(
        stub_dir,
        "gh",
        """
if args[:2] == ["repo", "view"]:
    print(json.dumps({
        "name": "asset-allocation-control-plane",
        "nameWithOwner": "koala-man-64/asset-allocation-control-plane",
        "owner": {"login": "koala-man-64"},
        "defaultBranchRef": {"name": "main"},
    }))
elif args[:2] == ["variable", "list"]:
    print("[]")
else:
    print("[]")
""".strip(),
    )

    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["account", "show"]:
    print(json.dumps({"tenantId": "tenant-id", "id": "subscription-id"}))
elif args[:2] == ["group", "show"]:
    print(json.dumps({"name": "AssetAllocationRG", "location": "eastus"}))
elif args[:2] == ["acr", "list"]:
    print(json.dumps([{"name": "assetallocationacr"}]))
elif args[:2] == ["identity", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-acr-pull-mi"}]))
elif args[:3] == ["containerapp", "env", "show"]:
    print(json.dumps({
        "name": "asset-allocation-env",
        "properties": {
            "appLogsConfiguration": {
                "logAnalyticsConfiguration": {"customerId": "workspace-id"}
            }
        },
    }))
elif args[:3] == ["containerapp", "env", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-env"}]))
elif args[:2] == ["containerapp", "show"]:
    app_name = args[args.index("--name") + 1] if "--name" in args else "asset-allocation-api"
    fqdn = f"{app_name}.example.test"
    payload = {
        "name": app_name,
        "properties": {
            "configuration": {"ingress": {"fqdn": fqdn}},
            "template": {"containers": [{"env": [{"name": "API_ROOT_PREFIX", "value": "asset-allocation"}]}]},
        },
    }
    print(json.dumps(payload))
elif args[:2] == ["containerapp", "list"]:
    print(json.dumps([
        {"resourceGroup": "ignored"},
        {"name": "asset-allocation-api"},
        {"name": "asset-allocation-ui"},
    ]))
elif args[:4] == ["monitor", "log-analytics", "workspace", "list"]:
    print(json.dumps([{"resourceGroup": "ignored"}, {"name": "asset-allocation-law", "customerId": "workspace-id"}]))
elif args[:3] == ["storage", "account", "list"]:
    print(json.dumps([{"name": "assetallocstorage001"}]))
elif args[:4] == ["postgres", "flexible-server", "db", "list"]:
    print(json.dumps([{"name": "asset_allocation"}]))
elif args[:3] == ["postgres", "flexible-server", "list"]:
    print(json.dumps([{"name": "pg-asset-allocation", "administratorLogin": "assetallocadmin"}]))
elif args[:3] == ["ad", "app", "list"]:
    display_name = args[args.index("--display-name") + 1] if "--display-name" in args else "unknown"
    print(json.dumps([{"id": "missing-display-name"}, {"displayName": display_name, "appId": f"{display_name}-app-id"}]))
else:
    print("[]")
""".strip(),
    )

    missing_env_file = tmp_path / "missing.env.web"
    script = repo_root() / "scripts" / "setup-env.ps1"
    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")

    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(script),
            "-DryRun",
            "-EnvFilePath",
            str(missing_env_file),
        ],
        cwd=repo_root(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout
    assert "API_APP_NAME=" in stdout
    assert "UI_OIDC_CLIENT_ID=" in stdout
    assert "LOG_ANALYTICS_WORKSPACE_NAME=" in stdout
    assert "UI_OIDC_REDIRECT_URI=" in stdout
