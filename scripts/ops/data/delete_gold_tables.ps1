param(
    [string]$Dsn,
    [switch]$DryRun,
    [switch]$Force,
    [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path

function Get-EnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Key
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
        if ($line -match "^\s*$([regex]::Escape($Key))\s*=\s*(.*)\s*$") {
            return $Matches[1].Trim().Trim('"').Trim("'")
        }
    }

    return $null
}

if (-not $Dsn) {
    $Dsn = Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"
}

if (-not $Dsn) {
    throw "POSTGRES_DSN is not configured. Set POSTGRES_DSN in `.env` or pass -Dsn."
}

if ($Dsn -notmatch "^postgres(ql)?://[^:]+:.+@[^:/]+:\d+/.+") {
    throw "Invalid or incomplete POSTGRES_DSN"
}

if (-not $UseDockerPsql -and -not (Get-Command psql -ErrorAction SilentlyContinue)) {
    Write-Host "Local psql is not installed; falling back to Dockerized psql."
    $UseDockerPsql = $true
}

function Invoke-Psql {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [switch]$TuplesOnly
    )

    $extraArgs = @("-v", "ON_ERROR_STOP=1")
    if ($TuplesOnly) {
        $extraArgs += @("-t", "-A")
    }

    if ($UseDockerPsql) {
        $cmd = @("run", "--rm", "postgres:16-alpine", "psql")
        $cmd += $Dsn
        $cmd += $extraArgs
        $cmd += @("-c", $Sql)
        return & docker @cmd 2>&1
    }

    return & psql $Dsn @extraArgs -c $Sql 2>&1
}

$listSql = @"
SELECT tablename
FROM pg_catalog.pg_tables
WHERE schemaname = 'gold'
ORDER BY tablename;
"@

$tables = @(Invoke-Psql -Sql $listSql -TuplesOnly | Where-Object { $_ -and $_.Trim() })
Write-Host "Found $($tables.Count) gold table(s):"
$tables | ForEach-Object { Write-Host " - $_" }

if ($DryRun) {
    Write-Host "Dry run only. No tables were dropped."
    exit 0
}

if ($tables.Count -eq 0) {
    Write-Host "No gold tables found."
    exit 0
}

Write-Warning "Dependent objects such as views may also be removed because tables are dropped with CASCADE."
if (-not $Force) {
    $confirmation = Read-Host "Are you sure you want to continue? (y/N)"
    if ($confirmation -notin @("y", "Y", "yes", "YES")) {
        Write-Host "Canceled."
        exit 1
    }
}

$dropSql = @"
DO $$
DECLARE
    table_name text;
BEGIN
    FOR table_name IN
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'gold'
        ORDER BY tablename
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS gold.%I CASCADE', table_name);
    END LOOP;
END
$$;
"@

Invoke-Psql -Sql $dropSql | Write-Host
Write-Host "Dropped $($tables.Count) gold table(s) from schema gold."
