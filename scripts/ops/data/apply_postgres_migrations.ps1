param(
    [string]$Dsn = $env:POSTGRES_DSN,
    [string]$MigrationsPath = "deploy/sql/postgres/migrations",
    [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$MigrationRoot = Join-Path $RepoRoot $MigrationsPath

if (-not $Dsn) {
    throw "POSTGRES_DSN is not configured. Set POSTGRES_DSN or pass -Dsn."
}

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
        return
    }

    & psql $Dsn -v ON_ERROR_STOP=1 -f $Path
}

Get-ChildItem -LiteralPath $MigrationRoot -Filter "*.sql" |
    Sort-Object Name |
    ForEach-Object {
        Write-Host "Applying migration $($_.Name)"
        Invoke-PsqlFile -Path $_.FullName
    }
