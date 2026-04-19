param(
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $repoRoot ".env.web"
$contractPath = Join-Path $repoRoot "docs\ops\env-contract.csv"

function Parse-EnvFile {
    param([Parameter(Mandatory = $true)][string]$Path)
    $map = @{}
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or $line -notmatch "^([^=]+)=(.*)$") { continue }
        $map[$matches[1].Trim()] = $matches[2]
    }
    return $map
}

function Load-EnvContract {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path $Path)) { throw "Env contract not found at $Path" }
    $map = @{}
    foreach ($row in (Import-Csv -Path $Path)) {
        $name = (($row.name | Out-String).Trim())
        if ($name) { $map[$name] = $row }
    }
    return $map
}

function Test-TruthyValue {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return $false }
    return @("1", "true", "t", "yes", "y", "on") -contains $Value.Trim().ToLowerInvariant()
}

if (-not (Test-Path $envPath)) { throw ".env.web not found at $envPath. Run scripts/setup-env.ps1 first." }
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) { throw "GitHub CLI (gh) is required to sync vars and secrets." }

$envMap = Parse-EnvFile -Path $envPath
$contractMap = Load-EnvContract -Path $contractPath
$undocumented = @($envMap.Keys | Where-Object { -not $contractMap.ContainsKey($_) } | Sort-Object -Unique)
if ($undocumented.Count -gt 0) { throw ".env.web contains undocumented keys: $($undocumented -join ', ')" }

$aiRelayEnabled = $false
if ($envMap.ContainsKey("AI_RELAY_ENABLED")) {
    $aiRelayEnabled = Test-TruthyValue -Value $envMap["AI_RELAY_ENABLED"]
}
if ($aiRelayEnabled) {
    foreach ($requiredName in @("AI_RELAY_API_KEY", "AI_RELAY_REQUIRED_ROLES")) {
        $value = if ($envMap.ContainsKey($requiredName)) { $envMap[$requiredName] } else { "" }
        if ([string]::IsNullOrWhiteSpace($value)) {
            throw "AI relay is enabled, but required value '$requiredName' is blank in .env.web."
        }
    }
}

$expectedVars = New-Object System.Collections.Generic.List[string]
$expectedSecrets = New-Object System.Collections.Generic.List[string]
foreach ($key in ($contractMap.Keys | Sort-Object)) {
    $entry = $contractMap[$key]
    $storage = (($entry.github_storage | Out-String).Trim()).ToLowerInvariant()
    if ($storage -notin @("var", "secret")) { continue }

    $value = if ($envMap.ContainsKey($key)) { $envMap[$key] } else { "" }
    if ($storage -eq "var") { $expectedVars.Add($key) } else { $expectedSecrets.Add($key) }
    if ([string]::IsNullOrWhiteSpace($value)) {
        Write-Host ("Skipping empty {0}: {1}" -f $storage, $key) -ForegroundColor Yellow
        continue
    }
    if ($DryRun) {
        Write-Host ("[DRY RUN] Would set {0}: {1}" -f $storage, $key)
        continue
    }
    if ($storage -eq "var") { $value | gh variable set $key } else { $value | gh secret set $key }
    Write-Host ("Synced {0}: {1}" -f $storage, $key) -ForegroundColor Green
}

function Remove-UnexpectedItems {
    param([Parameter(Mandatory = $true)][string]$Kind, [Parameter(Mandatory = $true)][string[]]$Expected)
    $remote = @(gh $Kind list --json name --jq ".[].name" 2>$null)
    $unexpected = @($remote | Where-Object { $_ -and $_ -notin $Expected } | Sort-Object -Unique)
    foreach ($name in $unexpected) {
        if ($DryRun) {
            Write-Host ("[DRY RUN] Would delete unexpected {0}: {1}" -f $Kind, $name)
            continue
        }
        gh $Kind delete $name
        Write-Host ("Deleted unexpected {0}: {1}" -f $Kind, $name) -ForegroundColor Yellow
    }
}

Remove-UnexpectedItems -Kind "variable" -Expected $expectedVars.ToArray()
Remove-UnexpectedItems -Kind "secret" -Expected $expectedSecrets.ToArray()
