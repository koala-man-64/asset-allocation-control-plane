param(
    [string]$Dsn = $env:POSTGRES_DSN,
    [Alias("MigrationsDir")]
    [string]$MigrationsPath = "deploy/sql/postgres/migrations",
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

function Resolve-MigrationRoot {
    param(
        [Parameter(Mandatory = $true)][string]$Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        $candidate = $Path
    }
    else {
        $candidate = Join-Path $RepoRoot $Path
    }

    if (-not (Test-Path -LiteralPath $candidate)) {
        throw "Migration path not found: $candidate"
    }

    return (Resolve-Path -LiteralPath $candidate -ErrorAction Stop).Path
}

if (-not $Dsn) {
    $Dsn = Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"
}

if (-not $Dsn) {
    $Dsn = Get-EnvValue -Path (Join-Path $RepoRoot ".env.web") -Key "POSTGRES_DSN"
}

if (-not $Dsn) {
    throw "POSTGRES_DSN is not configured. Set POSTGRES_DSN in `.env`, export it in the current shell, or pass -Dsn."
}

$MigrationRoot = Resolve-MigrationRoot -Path $MigrationsPath

if (-not (Test-Path -LiteralPath $MigrationRoot)) {
    throw "Migration path not found: $MigrationRoot"
}

function Invoke-PsqlFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path
    )

    if ($UseDockerPsql) {
        $dockerStdinPath = $Path
        $dockerArgs = @("run", "--rm", "-i", "postgres:16-alpine", "psql", $Dsn)
        $dockerArgs += "-v"
        $dockerArgs += "ON_ERROR_STOP=1"
        $dockerArgs += "-f"
        $dockerArgs += "-"
        $cmd = $dockerArgs
        Get-Content -Path $dockerStdinPath -Raw -Encoding UTF8 | & docker @cmd
        if ($LASTEXITCODE -ne 0) {
            throw "psql failed for migration $Path with exit code $LASTEXITCODE."
        }
        return
    }

    & psql $Dsn -v ON_ERROR_STOP=1 -f $Path
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed for migration $Path with exit code $LASTEXITCODE."
    }
}

Get-ChildItem -LiteralPath $MigrationRoot -Filter "*.sql" |
    Sort-Object Name |
    ForEach-Object {
        Write-Host "Applying migration $($_.Name)"
        Invoke-PsqlFile -Path $_.FullName
    }
