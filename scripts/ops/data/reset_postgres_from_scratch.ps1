param(
  [string]$Dsn,
  [string]$MigrationsDir,
  [switch]$Force,
  [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-RepoRoot {
  $current = (Resolve-Path $PSScriptRoot -ErrorAction Stop).Path
  while (-not [string]::IsNullOrWhiteSpace($current)) {
    if ((Test-Path (Join-Path $current "pyproject.toml")) -or (Test-Path (Join-Path $current ".codex"))) {
      return $current
    }
    $parent = Split-Path $current -Parent
    if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $current) {
      break
    }
    $current = $parent
  }
  throw "Unable to locate repo root from $PSScriptRoot"
}

function Confirm-Reset {
  if ($Force) {
    return
  }

  Write-Warning "This operation will DESTROY all data in the target database."
  $response = Read-Host "Are you sure you want to continue? (y/N)"
  if (($response.Trim().ToLowerInvariant()) -ne "y") {
    throw "Operation aborted by user."
  }
}

function Is-CompleteDsn {
  param([string]$Value)
  return ($Value -match "^[a-zA-Z][a-zA-Z0-9+.-]*://")
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Key
  )

  if (-not (Test-Path $Path)) {
    return ""
  }

  $keyPattern = "^{0}\s*=" -f [regex]::Escape($Key)
  foreach ($line in Get-Content $Path) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
      continue
    }

    if ($trimmed -notmatch $keyPattern) {
      continue
    }

    $parts = $trimmed -split "=", 2
    if ($parts.Count -ne 2) {
      continue
    }

    $value = $parts[1].Trim()
    if ($value.StartsWith('"') -and $value.EndsWith('"') -and $value.Length -ge 2) {
      $value = $value.Substring(1, $value.Length - 2)
    }
    return $value
  }

  return ""
}

function Invoke-Psql {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args
  )

  if ($UseDockerPsql.IsPresent) {
    Assert-CommandExists -Name "docker"
    $cmd = @("run", "--rm", "postgres:16-alpine", "psql") + $Args
    & docker @cmd
    if (-not $?) { throw "psql (docker) failed." }
    return
  }

  Assert-CommandExists -Name "psql"
  & psql @Args
  if (-not $?) { throw "psql failed." }
}

$RepoRoot = Resolve-RepoRoot
if (-not $MigrationsDir) {
  $MigrationsDir = Join-Path $RepoRoot "deploy/sql/postgres/migrations"
}

if (-not $Dsn) {
  $DsnFromEnv = Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"
  if (-not $DsnFromEnv) {
    $DsnFromEnv = $env:POSTGRES_DSN
    if (-not $DsnFromEnv) {
      $DsnFromEnv = Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"
    }
  }
  if ($DsnFromEnv) {
    $Dsn = $DsnFromEnv
  }
}

if (-not $Dsn) {
  throw "POSTGRES_DSN is not configured. Set POSTGRES_DSN in `.env` or pass -Dsn."
}

if (-not (Is-CompleteDsn -Value $Dsn)) {
  throw "Invalid or incomplete POSTGRES_DSN: '$Dsn'. Expected full DSN format, e.g. postgresql://user:pass@host:5432/db?sslmode=require"
}

$resolvedDir = (Resolve-Path $MigrationsDir -ErrorAction Stop).Path
$migrationScript = Join-Path $RepoRoot "scripts\ops\data\apply_postgres_migrations.ps1"
if (-not (Test-Path $migrationScript)) {
  throw "Migration apply script not found at $migrationScript"
}

Confirm-Reset

$resetSql = @'
DO $$
DECLARE
  schema_name text;
BEGIN
  FOR schema_name IN
    SELECT n.nspname
    FROM pg_namespace n
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'public')
      AND n.nspname NOT LIKE 'pg_toast%'
      AND n.nspname NOT LIKE 'pg_temp_%'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_name);
  END LOOP;
END $$;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO CURRENT_USER;
GRANT USAGE ON SCHEMA public TO PUBLIC;
'@

Write-Host "Resetting database objects (destructive)..."
Invoke-Psql -Args @($Dsn, "-v", "ON_ERROR_STOP=1", "-c", $resetSql)

Write-Host "Reapplying repo-owned migrations..."
& $migrationScript -Dsn $Dsn -MigrationsDir $resolvedDir -UseDockerPsql:$UseDockerPsql
if (-not $?) {
  throw "apply_postgres_migrations.ps1 failed."
}
