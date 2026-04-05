from __future__ import annotations

import csv
import re
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
