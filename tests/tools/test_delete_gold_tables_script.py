from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_delete_gold_tables_script_resolves_postgres_dsn_from_env_or_parameter() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "delete_gold_tables.ps1"
    text = script.read_text(encoding="utf-8")

    assert "[string]$Dsn" in text
    assert 'Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"' in text
    assert "POSTGRES_DSN is not configured. Set POSTGRES_DSN in `.env` or pass -Dsn." in text
    assert "Invalid or incomplete POSTGRES_DSN" in text


def test_delete_gold_tables_script_supports_dry_run_listing_for_gold_schema_tables() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "delete_gold_tables.ps1"
    text = script.read_text(encoding="utf-8")

    assert "[switch]$DryRun" in text
    assert "FROM pg_catalog.pg_tables" in text
    assert "WHERE schemaname = 'gold'" in text
    assert "Found $($tables.Count) gold table(s):" in text
    assert "Dry run only. No tables were dropped." in text


def test_delete_gold_tables_script_requires_confirmation_and_drops_tables_with_cascade() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "delete_gold_tables.ps1"
    text = script.read_text(encoding="utf-8")

    assert "[switch]$Force" in text
    assert 'Read-Host "Are you sure you want to continue? (y/N)"' in text
    assert "Dependent objects such as views may also be removed because tables are dropped with CASCADE." in text
    assert "DROP TABLE IF EXISTS gold.%I CASCADE" in text
    assert "Dropped $($tables.Count) gold table(s) from schema gold." in text


def test_delete_gold_tables_script_can_use_dockerized_psql() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "delete_gold_tables.ps1"
    text = script.read_text(encoding="utf-8")

    assert "[switch]$UseDockerPsql" in text
    assert "Local psql is not installed; falling back to Dockerized psql." in text
    assert '@("run", "--rm", "postgres:16-alpine", "psql")' in text
    assert '& docker @cmd 2>&1' in text
