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

$script:RequiredEnvKeys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($requiredKey in @(
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_SUBSCRIPTION_ID",
    "RESOURCE_GROUP",
    "ACR_NAME",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "API_OIDC_ISSUER",
    "API_OIDC_AUDIENCE",
    "UI_OIDC_CLIENT_ID",
    "UI_OIDC_AUTHORITY",
    "UI_OIDC_SCOPES",
    "UI_OIDC_REDIRECT_URI",
    "DISPATCH_APP_ID",
    "ALPHA_VANTAGE_API_KEY",
    "DEPLOY_SMOKE_BEARER_TOKEN",
    "AZURE_STORAGE_CONNECTION_STRING",
    "DISPATCH_APP_PRIVATE_KEY",
    "MASSIVE_API_KEY",
    "POSTGRES_ADMIN_USER",
    "POSTGRES_ADMIN_PASSWORD"
)) {
    [void]$script:RequiredEnvKeys.Add($requiredKey)
}

function Get-RequirementLevel {
    param([Parameter(Mandatory = $true)][string]$Name)
    if ($Name -in @("AI_RELAY_API_KEY", "AI_RELAY_REQUIRED_ROLES")) {
        if ($script:AiRelayEnabled) { return "required" }
        return "optional"
    }
    if ($script:RequiredEnvKeys.Contains($Name)) { return "required" }
    return "optional"
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
$script:AiRelayEnabled = $aiRelayEnabled

$missingRequired = New-Object System.Collections.Generic.List[string]
foreach ($key in ($contractMap.Keys | Sort-Object)) {
    if ((Get-RequirementLevel -Name $key) -ne "required") { continue }
    $value = if ($envMap.ContainsKey($key)) { $envMap[$key] } else { "" }
    if ([string]::IsNullOrWhiteSpace($value)) { $missingRequired.Add($key) }
}
if ($missingRequired.Count -gt 0) {
    throw ".env.web is missing required values: $($missingRequired -join ', '). Run scripts/setup-env.ps1 and provide the missing values before syncing."
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
