from __future__ import annotations

import csv
import json
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


def env_map(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value
    return values


def env_keys(path: Path) -> set[str]:
    return set(env_map(path))


def build_contract_env_values(overrides: dict[str, str] | None = None) -> dict[str, str]:
    template_values = env_map(repo_root() / ".env.template")
    values: dict[str, str] = {}
    for row in contract_rows():
        name = row["name"]
        template_value = template_values.get(name, "")
        if template_value:
            values[name] = template_value
        elif row["github_storage"] == "secret":
            values[name] = f"{name.lower()}-secret"
        else:
            values[name] = f"{name.lower()}-value"
    values["AI_RELAY_ENABLED"] = "false"
    if overrides:
        values.update(overrides)
    return values


def write_env_file(path: Path, values: dict[str, str]) -> None:
    lines = [f"{row['name']}={values.get(row['name'], '')}" for row in contract_rows()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def test_deploy_workflow_maps_auth_session_config_to_correct_github_storage() -> None:
    text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "API_AUTH_SESSION_MODE: ${{ vars.API_AUTH_SESSION_MODE }}" in text
    assert "API_AUTH_SESSION_IDLE_TTL_SECONDS: ${{ vars.API_AUTH_SESSION_IDLE_TTL_SECONDS }}" in text
    assert "API_AUTH_SESSION_ABSOLUTE_TTL_SECONDS: ${{ vars.API_AUTH_SESSION_ABSOLUTE_TTL_SECONDS }}" in text
    assert "API_AUTH_SESSION_SECRET_KEYS: ${{ secrets.API_AUTH_SESSION_SECRET_KEYS }}" in text
    assert "API_AUTH_SESSION_SECRET_KEYS: ${{ vars.API_AUTH_SESSION_SECRET_KEYS }}" not in text


def test_sync_script_reads_repo_local_contract() -> None:
    text = (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8")
    assert 'Join-Path $repoRoot "docs\\ops\\env-contract.csv"' in text
    assert 'Join-Path $repoRoot ".env.web"' in text
    assert "AI_RELAY_ENABLED" in text
    assert "AI_RELAY_API_KEY" in text
    assert "Assert-DeployAzureClientIdIsNotAcrPullIdentity" in text
    assert "AZURE_CLIENT_ID points at the ACR pull managed identity" in text


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
    assert "value_present=" in stdout


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
    assert 'function Get-ApiCorsAllowOrigins' in text
    assert '"API_CORS_ALLOW_ORIGINS" {' in text
    assert 'Convert-ToAbsoluteOrigin -Value $uiPublicHostname -AssumeHttpsHostname' in text
    assert 'Convert-ToAbsoluteOrigin -Value $uiRedirectUri' in text
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
    print(json.dumps([
        {"name": "AI_RELAY_REQUIRED_ROLES", "value": "\\"AssetAllocation.AiRelay.Use\\""},
        {"name": "UI_OIDC_CLIENT_ID", "value": "\\"ui-client-id\\""},
    ]))
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

    script = repo_root() / "scripts" / "setup-env.ps1"
    env_file = tmp_path / "ai.env.web"
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
            str(env_file),
            "-Set",
            "AI_RELAY_ENABLED=true",
        ],
        cwd=repo_root(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout
    combined_output = completed.stdout + completed.stderr
    assert "AI_RELAY_API_KEY=<redacted> [requirement=required;" in stdout
    assert "AI_RELAY_REQUIRED_ROLES=AssetAllocation.AiRelay.Use [requirement=required;" in stdout
    assert "UI_OIDC_CLIENT_ID=ui-client-id [requirement=required;" in stdout
    assert "Normalized quoted scalar value for AI_RELAY_REQUIRED_ROLES from github." in combined_output
    assert "Normalized quoted scalar value for UI_OIDC_CLIENT_ID from github." in combined_output


def test_setup_env_skips_prompt_for_secret_already_present_in_github(tmp_path: Path) -> None:
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()

    write_stub_command(
        stub_dir,
        "gh",
        """
if args[:2] == ["secret", "list"]:
    print(json.dumps([{"name": "ALPHA_VANTAGE_API_KEY"}]))
else:
    print("[]")
""".strip(),
    )

    env_file = tmp_path / "existing.env.web"
    write_env_file(
        env_file,
        build_contract_env_values({"ALPHA_VANTAGE_API_KEY": ""}),
    )

    script = repo_root() / "scripts" / "setup-env.ps1"
    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(script),
            "-EnvFilePath",
            str(env_file),
        ],
        cwd=repo_root(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout
    values = env_map(env_file)

    assert "ALPHA_VANTAGE_API_KEY=<redacted> [requirement=required;" in stdout
    assert "source=github-secret" in stdout
    assert "prompt_required=false" in stdout
    assert "value_present=true" in stdout
    assert values["ALPHA_VANTAGE_API_KEY"] == ""


def test_setup_env_generates_api_auth_session_secret_when_missing(tmp_path: Path) -> None:
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()

    write_stub_command(stub_dir, "gh", 'print("[]")')
    write_stub_command(stub_dir, "az", 'print("[]")')

    env_file = tmp_path / "generated.env.web"
    write_env_file(
        env_file,
        build_contract_env_values({"API_AUTH_SESSION_SECRET_KEYS": ""}),
    )

    script = repo_root() / "scripts" / "setup-env.ps1"
    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(script),
            "-EnvFilePath",
            str(env_file),
        ],
        cwd=repo_root(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout
    values = env_map(env_file)
    generated = values["API_AUTH_SESSION_SECRET_KEYS"]

    assert "API_AUTH_SESSION_SECRET_KEYS=<redacted> [requirement=required;" in stdout
    assert "source=generated" in stdout
    assert re.fullmatch(r"[0-9a-f]{64}", generated)


def test_setup_env_treats_existing_placeholder_values_as_unset(tmp_path: Path) -> None:
    env_file = tmp_path / "placeholder.env.web"
    write_env_file(
        env_file,
        build_contract_env_values(
            {
                "API_PUBLIC_BASE_URL": "${API_PUBLIC_BASE_URL}",
                "ETRADE_CALLBACK_URL": "${ETRADE_CALLBACK_URL}",
                "SYMBOL_ENRICHMENT_ALLOWED_JOBS": "${SYMBOL_ENRICHMENT_ALLOWED_JOBS}",
            }
        ),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    write_stub_command(stub_dir, "gh", 'print("[]")')
    write_stub_command(stub_dir, "az", 'print("[]")')

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
            str(env_file),
        ],
        cwd=repo_root(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    combined_output = completed.stdout + completed.stderr
    assert "${API_PUBLIC_BASE_URL}" not in combined_output
    assert "${ETRADE_CALLBACK_URL}" not in combined_output
    assert "${SYMBOL_ENRICHMENT_ALLOWED_JOBS}" not in combined_output
    assert "API_PUBLIC_BASE_URL=" in completed.stdout
    assert "ETRADE_CALLBACK_URL=" in completed.stdout
    assert "SYMBOL_ENRICHMENT_ALLOWED_JOBS=" in completed.stdout
    assert "Ignored unresolved placeholder value for API_PUBLIC_BASE_URL from placeholder.env.web." in combined_output
    assert "Ignored unresolved placeholder value for ETRADE_CALLBACK_URL from placeholder.env.web." in combined_output
    assert "Ignored unresolved placeholder value for SYMBOL_ENRICHMENT_ALLOWED_JOBS from placeholder.env.web." in combined_output


def test_ai_relay_smoke_tokens_are_documented_as_secrets() -> None:
    contract = contract_map()
    assert contract["AI_RELAY_SMOKE_BEARER_TOKEN"]["github_storage"] == "secret"
    assert contract["AI_RELAY_SMOKE_FORBIDDEN_BEARER_TOKEN"]["github_storage"] == "secret"


def test_broker_runtime_secrets_and_trade_gates_are_documented() -> None:
    contract = contract_map()
    for name in (
        "ETRADE_ENABLED",
        "ETRADE_TRADING_ENABLED",
        "SCHWAB_ENABLED",
        "SCHWAB_TRADING_ENABLED",
        "SCHWAB_TIMEOUT_SECONDS",
        "SCHWAB_REQUIRED_ROLES",
        "SCHWAB_TRADING_REQUIRED_ROLES",
        "KALSHI_ENABLED",
        "KALSHI_TRADING_ENABLED",
        "KALSHI_TIMEOUT_SECONDS",
        "KALSHI_READ_RETRY_ATTEMPTS",
        "KALSHI_READ_RETRY_BASE_DELAY_SECONDS",
        "KALSHI_REQUIRED_ROLES",
        "KALSHI_TRADING_REQUIRED_ROLES",
        "KALSHI_DEMO_BASE_URL",
        "KALSHI_LIVE_BASE_URL",
    ):
        assert contract[name]["github_storage"] == "var"

    for name in (
        "ETRADE_SANDBOX_CONSUMER_KEY",
        "ETRADE_SANDBOX_CONSUMER_SECRET",
        "ETRADE_LIVE_CONSUMER_KEY",
        "ETRADE_LIVE_CONSUMER_SECRET",
        "KALSHI_DEMO_API_KEY_ID",
        "KALSHI_DEMO_PRIVATE_KEY_PEM",
        "KALSHI_LIVE_API_KEY_ID",
        "KALSHI_LIVE_PRIVATE_KEY_PEM",
        "SCHWAB_CLIENT_ID",
        "SCHWAB_CLIENT_SECRET",
    ):
        assert contract[name]["github_storage"] == "secret"

    assert "SCHWAB_ACCESS_TOKEN" not in contract
    assert "SCHWAB_REFRESH_TOKEN" not in contract


def test_sync_script_normalizes_quoted_scalar_values_before_github_sync(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values(
            {
                "AI_RELAY_ENABLED": "true",
                "AI_RELAY_REQUIRED_ROLES": '"AssetAllocation.AiRelay.Use"',
                "UI_OIDC_CLIENT_ID": '"ui-client-id"',
                "AZURE_CLIENT_ID": '"deploy-client-id"',
                "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON": '"{}"',
            }
        ),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gh_log_path = tmp_path / "gh-sync-log.jsonl"
    write_stub_command(
        stub_dir,
        "gh",
        f"""
from pathlib import Path

log_path = Path(r\"\"\"{gh_log_path}\"\"\")
payload = sys.stdin.read().rstrip("\\r\\n")
if args[:2] == ["variable", "set"] or args[:2] == ["secret", "set"]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({{"kind": args[0], "name": args[2], "value": payload}}) + "\\n")
elif args[:2] == ["variable", "list"] or args[:2] == ["secret", "list"]:
    print("")
else:
    print("")
""".strip(),
    )
    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["identity", "show"]:
    print("other-client-id")
else:
    print("")
""".strip(),
    )

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    records = [
        json.loads(line)
        for line in gh_log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    values_by_name = {record["name"]: record["value"] for record in records}
    combined_output = completed.stdout + completed.stderr

    assert values_by_name["AI_RELAY_REQUIRED_ROLES"] == "AssetAllocation.AiRelay.Use"
    assert values_by_name["UI_OIDC_CLIENT_ID"] == "ui-client-id"
    assert values_by_name["AZURE_CLIENT_ID"] == "deploy-client-id"
    assert values_by_name["SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON"] == '"{}"'
    assert "Normalized quoted scalar value for AI_RELAY_REQUIRED_ROLES from .env.web before GitHub sync." in combined_output
    assert "Normalized quoted scalar value for UI_OIDC_CLIENT_ID from .env.web before GitHub sync." in combined_output
    assert "Normalized quoted scalar value for AZURE_CLIENT_ID from .env.web before GitHub sync." in combined_output


def test_sync_script_treats_unresolved_placeholders_as_blank_values(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values(
            {
                "API_PUBLIC_BASE_URL": "${API_PUBLIC_BASE_URL}",
                "ETRADE_CALLBACK_URL": "${ETRADE_CALLBACK_URL}",
                "SYMBOL_ENRICHMENT_ALLOWED_JOBS": "${SYMBOL_ENRICHMENT_ALLOWED_JOBS}",
            }
        ),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gh_log_path = tmp_path / "gh-placeholder-log.jsonl"
    write_stub_command(
        stub_dir,
        "gh",
        f"""
from pathlib import Path

log_path = Path(r\"\"\"{gh_log_path}\"\"\")
payload = sys.stdin.read().rstrip("\\r\\n")
if args[:2] == ["variable", "set"] or args[:2] == ["secret", "set"]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({{"kind": args[0], "name": args[2], "value": payload}}) + "\\n")
elif args[:2] == ["variable", "list"] or args[:2] == ["secret", "list"]:
    print("")
else:
    print("")
""".strip(),
    )
    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["identity", "show"]:
    print("other-client-id")
else:
    print("")
""".strip(),
    )

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    combined_output = completed.stdout + completed.stderr
    records = []
    if gh_log_path.exists():
        records = [
            json.loads(line)
            for line in gh_log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    assert all(record["name"] != "API_PUBLIC_BASE_URL" for record in records)
    assert all(record["name"] != "ETRADE_CALLBACK_URL" for record in records)
    assert all(record["name"] != "SYMBOL_ENRICHMENT_ALLOWED_JOBS" for record in records)
    assert "Skipping empty var: API_PUBLIC_BASE_URL" in combined_output
    assert "Skipping empty var: ETRADE_CALLBACK_URL" in combined_output
    assert "Skipping empty var: SYMBOL_ENRICHMENT_ALLOWED_JOBS" in combined_output
    assert "Ignored unresolved placeholder value for API_PUBLIC_BASE_URL from .env.web before GitHub sync." in combined_output
    assert "Ignored unresolved placeholder value for ETRADE_CALLBACK_URL from .env.web before GitHub sync." in combined_output
    assert "Ignored unresolved placeholder value for SYMBOL_ENRICHMENT_ALLOWED_JOBS from .env.web before GitHub sync." in combined_output


def test_sync_script_deletes_blank_placeholder_variables(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values(
            {
                "API_PUBLIC_BASE_URL": "${API_PUBLIC_BASE_URL}",
                "ETRADE_CALLBACK_URL": "${ETRADE_CALLBACK_URL}",
                "SYMBOL_ENRICHMENT_ALLOWED_JOBS": "${SYMBOL_ENRICHMENT_ALLOWED_JOBS}",
            }
        ),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gh_log_path = tmp_path / "gh-delete-log.jsonl"
    write_stub_command(
        stub_dir,
        "gh",
        f"""
from pathlib import Path

log_path = Path(r\"\"\"{gh_log_path}\"\"\")
payload = sys.stdin.read().rstrip("\\r\\n")
if args[:2] == ["variable", "list"]:
    names = ["API_PUBLIC_BASE_URL", "ETRADE_CALLBACK_URL", "SYMBOL_ENRICHMENT_ALLOWED_JOBS"]
    if "--jq" in args:
        print("\\n".join(names))
    else:
        print(json.dumps([{{"name": name}} for name in names]))
elif args[:2] == ["secret", "list"]:
    print("")
elif args[:2] == ["variable", "delete"] or args[:2] == ["variable", "set"] or args[:2] == ["secret", "set"]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({{"kind": args[0], "action": args[1], "name": args[2], "value": payload}}) + "\\n")
else:
    print("")
""".strip(),
    )
    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["identity", "show"]:
    print("other-client-id")
else:
    print("")
""".strip(),
    )

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    records = [
        json.loads(line)
        for line in gh_log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    deleted_names = {record["name"] for record in records if record["action"] == "delete"}
    assert deleted_names >= {
        "API_PUBLIC_BASE_URL",
        "ETRADE_CALLBACK_URL",
        "SYMBOL_ENRICHMENT_ALLOWED_JOBS",
    }
    assert all(
        not (
            record["action"] == "set"
            and record["name"] in {"API_PUBLIC_BASE_URL", "ETRADE_CALLBACK_URL", "SYMBOL_ENRICHMENT_ALLOWED_JOBS"}
        )
        for record in records
    )


def test_setup_env_prompt_functions_reject_blank_required_values() -> None:
    text = (repo_root() / "scripts" / "setup-env.ps1").read_text(encoding="utf-8")
    assert '# {0} is required and cannot be blank.' in text
    assert 'if ($Requirement -ne "required") { return "" }' in text
    assert 'if (-not [string]::IsNullOrWhiteSpace($value) -or $Requirement -ne "required") { return $value }' in text


def test_sync_script_fails_fast_when_required_values_are_blank(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values({"ALPHA_VANTAGE_API_KEY": ""}),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    write_stub_command(stub_dir, "gh", 'print("[]")')

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    error_output = completed.stdout + completed.stderr
    assert ".env.web is missing required values" in error_output
    assert "ALPHA_VANTAGE_API_KEY" in error_output


def test_sync_script_preserves_existing_required_github_secret_when_env_value_blank(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values({"ALPHA_VANTAGE_API_KEY": ""}),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    gh_log_path = tmp_path / "gh-preserve-log.jsonl"
    write_stub_command(
        stub_dir,
        "gh",
        f"""
from pathlib import Path

log_path = Path(r\"\"\"{gh_log_path}\"\"\")
payload = sys.stdin.read().rstrip("\\r\\n")
if args[:2] == ["secret", "list"]:
    if "--jq" in args:
        print("ALPHA_VANTAGE_API_KEY")
    else:
        print(json.dumps([{{"name": "ALPHA_VANTAGE_API_KEY"}}]))
elif args[:2] == ["variable", "list"] or args[:2] == ["secret", "list"]:
    print("")
elif args[:2] == ["variable", "set"] or args[:2] == ["secret", "set"]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({{"kind": args[0], "name": args[2], "value": payload}}) + "\\n")
else:
    print("")
""".strip(),
    )
    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["identity", "show"]:
    print("other-client-id")
else:
    print("")
""".strip(),
    )

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    combined_output = completed.stdout + completed.stderr
    records = []
    if gh_log_path.exists():
        records = [
            json.loads(line)
            for line in gh_log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    assert "Preserving existing GitHub secret: ALPHA_VANTAGE_API_KEY" in combined_output
    assert all(record["name"] != "ALPHA_VANTAGE_API_KEY" for record in records)


def test_sync_script_requires_local_api_auth_session_secret_even_when_github_secret_exists(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values({"API_AUTH_SESSION_SECRET_KEYS": ""}),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    write_stub_command(
        stub_dir,
        "gh",
        """
if args[:2] == ["secret", "list"]:
    if "--jq" in args:
        print("API_AUTH_SESSION_SECRET_KEYS")
    else:
        print(json.dumps([{"name": "API_AUTH_SESSION_SECRET_KEYS"}]))
elif args[:2] == ["variable", "list"]:
    print("")
else:
    print("")
""".strip(),
    )
    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["identity", "show"]:
    print("other-client-id")
else:
    print("")
""".strip(),
    )

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    error_output = completed.stdout + completed.stderr
    assert ".env.web is missing required values" in error_output
    assert "API_AUTH_SESSION_SECRET_KEYS" in error_output


def test_sync_script_rejects_acr_pull_identity_for_azure_client_id(tmp_path: Path) -> None:
    temp_repo = tmp_path / "repo"
    (temp_repo / "scripts").mkdir(parents=True)
    (temp_repo / "docs" / "ops").mkdir(parents=True)

    (temp_repo / "scripts" / "sync-all-to-github.ps1").write_text(
        (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / "docs" / "ops" / "env-contract.csv").write_text(
        (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    write_env_file(
        temp_repo / ".env.web",
        build_contract_env_values(
            {
                "AZURE_CLIENT_ID": "managed-identity-client-id",
                "RESOURCE_GROUP": "AssetAllocationRG",
                "ACR_PULL_IDENTITY_NAME": "asset-allocation-acr-pull-mi",
            }
        ),
    )

    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    write_stub_command(stub_dir, "gh", 'print("[]")')
    write_stub_command(
        stub_dir,
        "az",
        """
if args[:2] == ["identity", "show"]:
    print("managed-identity-client-id")
else:
    print("")
""".strip(),
    )

    env = os.environ.copy()
    env["PATH"] = str(stub_dir) + os.pathsep + env.get("PATH", "")
    completed = subprocess.run(
        [
            powershell_exe(),
            "-NoProfile",
            "-File",
            str(temp_repo / "scripts" / "sync-all-to-github.ps1"),
        ],
        cwd=temp_repo,
        env=env,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    error_output = completed.stdout + completed.stderr
    assert "AZURE_CLIENT_ID points at the ACR pull managed identity" in error_output
    assert "asset-allocation-acr-pull-mi" in error_output


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
    assert "API_CORS_ALLOW_ORIGINS=https://asset-allocation-ui.example.test" in stdout
    assert "UI_OIDC_CLIENT_ID=" in stdout
    assert "LOG_ANALYTICS_WORKSPACE_NAME=" in stdout
    assert "UI_OIDC_REDIRECT_URI=" in stdout
    assert "API_OIDC_JWKS_URL= [requirement=optional; suggested=<blank>; source=default; prompt_required=true; value_present=false]" in stdout
