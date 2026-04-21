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
    $source = Split-Path -Leaf $Path
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or $line -notmatch "^([^=]+)=(.*)$") { continue }
        $key = $matches[1].Trim()
        $map[$key] = Resolve-UnresolvedPlaceholderValue -Key $key -Value $matches[2] -Source $source
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

function Normalize-EnvValue {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return "" }
    return $Value.Trim()
}

function Register-IgnoredPlaceholderValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Source
    )
    if ([string]::IsNullOrWhiteSpace($Key) -or [string]::IsNullOrWhiteSpace($Source)) { return }
    if (-not $script:IgnoredPlaceholderValues.ContainsKey($Key)) {
        $script:IgnoredPlaceholderValues[$Key] = [pscustomobject]@{
            Key    = $Key
            Source = $Source
        }
    }
}

function Resolve-UnresolvedPlaceholderValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [AllowNull()][string]$Value,
        [Parameter(Mandatory = $true)][string]$Source
    )
    $candidate = Normalize-EnvValue -Value $Value
    if ([string]::IsNullOrWhiteSpace($candidate)) { return "" }
    if ($candidate -match '^(["'']?)\$\{([A-Z][A-Z0-9_]*)\}\1$') {
        Register-IgnoredPlaceholderValue -Key $Key -Source $Source
        return ""
    }
    return $candidate
}

function Register-NormalizedQuotedScalarValue {
    param([Parameter(Mandatory = $true)][string]$Key)
    if ([string]::IsNullOrWhiteSpace($Key)) { return }
    if (-not $script:NormalizedQuotedScalarValues.ContainsKey($Key)) {
        $script:NormalizedQuotedScalarValues[$Key] = $true
    }
}

function Normalize-QuotedScalarValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [AllowNull()][string]$Value
    )
    $candidate = Normalize-EnvValue -Value $Value
    if ([string]::IsNullOrWhiteSpace($candidate) -or $candidate.Length -lt 2) { return $candidate }

    $quote = $candidate.Substring(0, 1)
    if (($quote -ne '"' -and $quote -ne "'") -or $candidate.Substring($candidate.Length - 1, 1) -ne $quote) {
        return $candidate
    }

    $inner = $candidate.Substring(1, $candidate.Length - 2).Trim()
    if ([string]::IsNullOrWhiteSpace($inner)) { return $candidate }
    if ($inner.Contains("\n")) { return $candidate }
    if ($inner.StartsWith("{") -or $inner.StartsWith("[")) { return $candidate }

    Register-NormalizedQuotedScalarValue -Key $Key
    return $inner
}

function Write-NormalizedQuotedScalarWarnings {
    foreach ($key in ($script:NormalizedQuotedScalarValues.Keys | Sort-Object)) {
        Write-Warning ("Normalized quoted scalar value for {0} from .env.web before GitHub sync." -f $key)
    }
}

function Write-IgnoredPlaceholderWarnings {
    foreach ($key in ($script:IgnoredPlaceholderValues.Keys | Sort-Object)) {
        $entry = $script:IgnoredPlaceholderValues[$key]
        Write-Warning ("Ignored unresolved placeholder value for {0} from {1} before GitHub sync." -f $entry.Key, $entry.Source)
    }
}

function Get-RemoteGitHubItemNames {
    param([Parameter(Mandatory = $true)][string]$Kind)
    $names = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $output = @(gh $Kind list --json name --jq ".[].name" 2>$null)
    if ($LASTEXITCODE -ne 0) { return ,$names }
    foreach ($line in $output) {
        $name = Normalize-EnvValue -Value ([string]$line)
        if ([string]::IsNullOrWhiteSpace($name)) { continue }
        [void]$names.Add($name)
    }
    return ,$names
}

function Get-GitHubSecretNames {
    if ($null -eq $script:GitHubSecretNames) {
        $script:GitHubSecretNames = Get-RemoteGitHubItemNames -Kind "secret"
    }
    return ,$script:GitHubSecretNames
}

function Test-GitHubSecretExists {
    param([Parameter(Mandatory = $true)][string]$Name)
    return (Get-GitHubSecretNames).Contains($Name)
}

function Resolve-ManagedIdentityClientId {
    param(
        [Parameter(Mandatory = $true)][string]$IdentityName,
        [Parameter(Mandatory = $true)][string]$ResourceGroupName
    )

    if (-not (Get-Command az -ErrorAction SilentlyContinue)) { return "" }
    $output = & az identity show `
        --name $IdentityName `
        --resource-group $ResourceGroupName `
        --query clientId `
        -o tsv `
        --only-show-errors 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($output)) { return "" }
    return $output.Trim()
}

function Assert-DeployAzureClientIdIsNotAcrPullIdentity {
    param([Parameter(Mandatory = $true)][hashtable]$EnvMap)

    $deployClientId = if ($EnvMap.ContainsKey("AZURE_CLIENT_ID")) { Normalize-EnvValue -Value $EnvMap["AZURE_CLIENT_ID"] } else { "" }
    $acrPullIdentityName = if ($EnvMap.ContainsKey("ACR_PULL_IDENTITY_NAME")) { Normalize-EnvValue -Value $EnvMap["ACR_PULL_IDENTITY_NAME"] } else { "" }
    $resourceGroupName = if ($EnvMap.ContainsKey("RESOURCE_GROUP")) { Normalize-EnvValue -Value $EnvMap["RESOURCE_GROUP"] } else { "" }

    if ([string]::IsNullOrWhiteSpace($deployClientId) -or [string]::IsNullOrWhiteSpace($acrPullIdentityName) -or [string]::IsNullOrWhiteSpace($resourceGroupName)) {
        return
    }

    $acrPullClientId = Resolve-ManagedIdentityClientId -IdentityName $acrPullIdentityName -ResourceGroupName $resourceGroupName
    if ([string]::IsNullOrWhiteSpace($acrPullClientId)) { return }
    if ($deployClientId -ne $acrPullClientId) { return }

    throw ".env.web AZURE_CLIENT_ID points at the ACR pull managed identity '$acrPullIdentityName'. Set AZURE_CLIENT_ID to the GitHub Actions Azure app registration client id before syncing."
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

$script:NormalizedQuotedScalarValues = @{}
$script:IgnoredPlaceholderValues = @{}
$script:GitHubSecretNames = $null
$envMap = Parse-EnvFile -Path $envPath
$normalizedEnvMap = @{}
foreach ($key in $envMap.Keys) {
    $normalizedEnvMap[$key] = Normalize-QuotedScalarValue -Key $key -Value $envMap[$key]
}
$envMap = $normalizedEnvMap
$contractMap = Load-EnvContract -Path $contractPath
$undocumented = @($envMap.Keys | Where-Object { -not $contractMap.ContainsKey($_) } | Sort-Object -Unique)
if ($undocumented.Count -gt 0) { throw ".env.web contains undocumented keys: $($undocumented -join ', ')" }

Write-NormalizedQuotedScalarWarnings
Write-IgnoredPlaceholderWarnings

$aiRelayEnabled = $false
if ($envMap.ContainsKey("AI_RELAY_ENABLED")) {
    $aiRelayEnabled = Test-TruthyValue -Value $envMap["AI_RELAY_ENABLED"]
}
$script:AiRelayEnabled = $aiRelayEnabled

$missingRequired = New-Object System.Collections.Generic.List[string]
foreach ($key in ($contractMap.Keys | Sort-Object)) {
    if ((Get-RequirementLevel -Name $key) -ne "required") { continue }
    $value = if ($envMap.ContainsKey($key)) { $envMap[$key] } else { "" }
    if (-not [string]::IsNullOrWhiteSpace($value)) { continue }
    $entry = $contractMap[$key]
    $storage = (($entry.github_storage | Out-String).Trim()).ToLowerInvariant()
    if ($storage -eq "secret" -and (Test-GitHubSecretExists -Name $key)) { continue }
    $missingRequired.Add($key)
}
if ($missingRequired.Count -gt 0) {
    throw ".env.web is missing required values: $($missingRequired -join ', '). Run scripts/setup-env.ps1 and provide the missing values before syncing."
}

Assert-DeployAzureClientIdIsNotAcrPullIdentity -EnvMap $envMap

$expectedVars = New-Object System.Collections.Generic.List[string]
$expectedSecrets = New-Object System.Collections.Generic.List[string]
foreach ($key in ($contractMap.Keys | Sort-Object)) {
    $entry = $contractMap[$key]
    $storage = (($entry.github_storage | Out-String).Trim()).ToLowerInvariant()
    if ($storage -notin @("var", "secret")) { continue }

    $value = if ($envMap.ContainsKey($key)) { $envMap[$key] } else { "" }
    if ($storage -eq "var") { $expectedVars.Add($key) } else { $expectedSecrets.Add($key) }
    if ([string]::IsNullOrWhiteSpace($value)) {
        if ($storage -eq "secret" -and (Test-GitHubSecretExists -Name $key)) {
            Write-Host ("Preserving existing GitHub secret: {0}" -f $key) -ForegroundColor Cyan
            continue
        }
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
