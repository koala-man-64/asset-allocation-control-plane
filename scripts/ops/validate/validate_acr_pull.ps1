param(
  [string]$SubscriptionId = "",
  [string]$ResourceGroup = "",
  [string]$AcrName = "",
  [string]$AcrPullIdentityName = "",
  [string[]]$AppNames = @(),
  [string[]]$JobNames = @(),
  [string]$EnvFile = "",
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Usage {
  @"
Usage: validate_acr_pull.ps1 [-SubscriptionId <sub>] [-ResourceGroup <rg>] [-AcrName <name>] [-AcrPullIdentityName <mi-name>] [-AppNames <app1,app2>] [-JobNames <job1,job2>] [-EnvFile <path>]

Validates Azure Container Registry (ACR) pull configuration for Container Apps/Jobs:
- ACR exists and is reachable
- User-assigned managed identity exists
- Identity has AcrPull on the ACR
- Apps/Jobs are assigned the identity
- Apps/Jobs are configured to use the registry with that identity

If -AppNames is omitted, defaults to API app only (asset-allocation-api).
"@
}

if ($Help) {
  Write-Usage
  exit 0
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string[]]$Lines
  )

  foreach ($line in $Lines) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) { continue }
    if ($trimmed -match ("^" + [regex]::Escape($Key) + "=(.*)$")) {
      $value = $matches[1].Trim()
      if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
          ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        $value = $value.Substring(1, $value.Length - 2)
      }
      return $value
    }
  }
  return $null
}

function Get-EnvValueFirst {
  param(
    [Parameter(Mandatory = $true)][string[]]$Keys,
    [string[]]$Lines
  )

  foreach ($key in $Keys) {
    $value = Get-EnvValue -Key $key -Lines $Lines
    if ($value) { return $value }
  }
  return $null
}

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

function Resolve-EnvFilePath {
  param([string]$EnvFileOverride)

  if (-not [string]::IsNullOrWhiteSpace($EnvFileOverride)) {
    return $EnvFileOverride
  }

  $repoRoot = Resolve-RepoRoot
  $candidateWeb = Join-Path $repoRoot ".env.web"
  $candidateEnv = Join-Path $repoRoot ".env"

  if (Test-Path $candidateWeb) { return $candidateWeb }
  if (Test-Path $candidateEnv) { return $candidateEnv }
  return $candidateEnv
}

function Add-Result {
  param(
    [string]$Name,
    [bool]$Ok,
    [string]$Details,
    [string]$Remediation,
    [ValidateSet("Error", "Warning")][string]$Severity = "Error"
  )

  $script:results += [PSCustomObject]@{
    Name = $Name
    Ok = $Ok
    Details = $Details
    Remediation = $Remediation
    Severity = $Severity
  }
}

function Write-Results {
  $errors = @($results | Where-Object { -not $_.Ok -and $_.Severity -eq "Error" })
  $warnings = @($results | Where-Object { -not $_.Ok -and $_.Severity -eq "Warning" })

  foreach ($result in $results) {
    if ($result.Ok) {
      Write-Host "[OK] $($result.Name) - $($result.Details)"
    } else {
      $label = if ($result.Severity -eq "Warning") { "[WARN]" } else { "[ERROR]" }
      Write-Host "$label $($result.Name) - $($result.Details)"
      if ($result.Remediation) {
        Write-Host "       Remediation: $($result.Remediation)"
      }
    }
  }

  Write-Host ""
  Write-Host "Summary: $($results.Count) checks, $($errors.Count) error(s), $($warnings.Count) warning(s)."

  if ($errors.Count -gt 0) { exit 1 }
}

function Invoke-Az {
  # Avoid the reserved automatic variable name `$args` (case-insensitive) which can
  # interfere with parameter binding/splatting under StrictMode.
  param([string[]]$AzArgs)
  if (-not $script:AzCliPath) {
    throw "Azure CLI path not initialized."
  }

  # Under StrictMode, $LASTEXITCODE may be unset until a native command runs.
  # Initialize it so reads are safe even if the underlying command isn't native.
  $global:LASTEXITCODE = 0

  $output = & $script:AzCliPath @AzArgs 2>$null
  if ($global:LASTEXITCODE -ne 0) { return $null }
  if (-not $output) { return $null }
  return $output
}

function Invoke-AzJson {
  param(
    [string[]]$AzArgs,
    [string]$Context
  )

  $raw = Invoke-Az -AzArgs $AzArgs
  if (-not $raw) { return $null }
  try {
    return $raw | ConvertFrom-Json
  } catch {
    $snippet = $raw
    if ($snippet -is [array]) { $snippet = ($snippet -join "`n") }
    if ($snippet.Length -gt 240) { $snippet = $snippet.Substring(0, 240) + "..." }
    Add-Result -Name "$Context JSON parse" -Ok $false -Details "Failed to parse az output as JSON. Output starts with: $snippet" -Remediation "Run the az command manually to inspect output and ensure it returns JSON."
    Write-Results
  }
}

function Normalize-Names {
  param([string[]]$Names)
  if (-not $Names) { return @() }
  return @($Names | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim() } | Sort-Object -Unique)
}

function Resolve-AzCliPath {
  # On Windows, prefer az.cmd over the extensionless "az" shim (which is a bash
  # script intended for Git Bash/WSL and can break argument passing in PowerShell).
  if ($env:OS -eq "Windows_NT") {
    $azCmd = Get-Command az.cmd -ErrorAction SilentlyContinue
    if ($azCmd -and $azCmd.Source) { return $azCmd.Source }
  }

  $az = Get-Command az -ErrorAction SilentlyContinue
  if ($az -and $az.Source) { return $az.Source }
  return $null
}

$script:AzCliPath = Resolve-AzCliPath
if (-not $script:AzCliPath) {
  Write-Error "Azure CLI (az) not found. Install Azure CLI and retry."
  exit 2
}

try {
  $null = & $script:AzCliPath account show --query id -o tsv 2>$null
} catch {
  Write-Error "Azure CLI not logged in. Run 'az login' and retry."
  exit 2
}

$results = @()

$envPath = Resolve-EnvFilePath -EnvFileOverride $EnvFile
$envLines = @()
if (Test-Path $envPath) { $envLines = Get-Content $envPath }

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = ($env:AZURE_SUBSCRIPTION_ID, $env:SUBSCRIPTION_ID) | Where-Object { $_ } | Select-Object -First 1
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = (& $script:AzCliPath account show --query id -o tsv) -replace "`r", ""
}

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = Get-EnvValueFirst -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = $env:RESOURCE_GROUP
}
if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = "AssetAllocationRG"
}

if ([string]::IsNullOrWhiteSpace($AcrName)) {
  $AcrName = Get-EnvValueFirst -Keys @("ACR_NAME", "AZURE_ACR_NAME") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($AcrName)) {
  $AcrName = $env:ACR_NAME
}
if ([string]::IsNullOrWhiteSpace($AcrName)) {
  $AcrName = "assetallocationacr"
}

if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = Get-EnvValueFirst -Keys @("ACR_PULL_IDENTITY_NAME") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = $env:ACR_PULL_IDENTITY_NAME
}
if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = "asset-allocation-acr-pull-mi"
}

if (-not $AppNames -or $AppNames.Count -eq 0) {
  $apiApp = Get-EnvValueFirst -Keys @("API_APP_NAME") -Lines $envLines
  if (-not $apiApp) { $apiApp = $env:API_APP_NAME }
  if (-not $apiApp) { $apiApp = "asset-allocation-api" }

  # Default to the unified app only. If you still run a separate UI app,
  # pass it explicitly with -AppNames.
  $AppNames = @($apiApp)
}

$AppNames = Normalize-Names -Names $AppNames
$JobNames = Normalize-Names -Names $JobNames
$registryServer = "$AcrName.azurecr.io"

Write-Host "Using subscription: $SubscriptionId"
Write-Host "Using resource group: $ResourceGroup"
Write-Host "Using ACR: $AcrName ($registryServer)"
Write-Host "Using ACR pull identity: $AcrPullIdentityName"
Write-Host "Apps: $([string]::Join(', ', $AppNames))"
if ($JobNames) {
  Write-Host "Jobs: $([string]::Join(', ', $JobNames))"
}
Write-Host ""

$acr = Invoke-AzJson -AzArgs @("acr", "show", "--name", $AcrName, "--resource-group", $ResourceGroup, "--only-show-errors", "-o", "json") -Context "ACR show"
if (-not $acr) {
  Add-Result -Name "ACR exists" -Ok $false -Details "ACR '$AcrName' not found in RG '$ResourceGroup'." -Remediation "Create the ACR or correct -AcrName/-ResourceGroup."
  Write-Results
}

$acrId = $acr.id
$publicNetwork = $acr.publicNetworkAccess
if ($publicNetwork -and $publicNetwork.ToString().ToLowerInvariant() -eq "disabled") {
  Add-Result -Name "ACR public network access" -Ok $false -Details "ACR publicNetworkAccess is Disabled." -Remediation "Ensure Container Apps are VNet-integrated with ACR private endpoint and private DNS." -Severity "Warning"
} else {
  $publicNetworkLabel = if ($null -ne $publicNetwork -and "$publicNetwork" -ne "") { $publicNetwork } else { "unknown" }
  Add-Result -Name "ACR public network access" -Ok $true -Details ("publicNetworkAccess=" + $publicNetworkLabel) -Remediation ""
}

$identity = Invoke-AzJson -AzArgs @("identity", "show", "--name", $AcrPullIdentityName, "--resource-group", $ResourceGroup, "--only-show-errors", "-o", "json") -Context "Identity show"
if (-not $identity) {
    Add-Result -Name "ACR pull identity exists" -Ok $false -Details "Managed identity '$AcrPullIdentityName' not found in RG '$ResourceGroup'." -Remediation "Create it (scripts/ops/provision/provision_azure.ps1) and grant AcrPull."
  Write-Results
}

$identityId = $identity.id
$principalId = $identity.principalId

Add-Result -Name "ACR pull identity exists" -Ok $true -Details "Found identity id=$identityId." -Remediation ""

$acrPullAssignmentsRaw = Invoke-Az -AzArgs @(
  "role", "assignment", "list",
  "--assignee-object-id", $principalId,
  "--scope", $acrId,
  "--query", "[?roleDefinitionName=='AcrPull']",
  "--only-show-errors",
  "-o", "json"
)
$acrPullAssignments = @()
if ($acrPullAssignmentsRaw) {
  try {
    $acrPullAssignments = $acrPullAssignmentsRaw | ConvertFrom-Json
  } catch {
    $snippet = $acrPullAssignmentsRaw
    if ($snippet -is [array]) { $snippet = ($snippet -join "`n") }
    if ($snippet.Length -gt 240) { $snippet = $snippet.Substring(0, 240) + "..." }
    Add-Result -Name "AcrPull assignment JSON parse" -Ok $false -Details "Failed to parse az role assignment output as JSON. Output starts with: $snippet" -Remediation "Run the az command manually to inspect output."
    Write-Results
  }
}
$hasAcrPull = $acrPullAssignments.Count -gt 0
if ($hasAcrPull) {
  Add-Result -Name "AcrPull role assignment" -Ok $true -Details "AcrPull assigned to principalId=$principalId on $acrId." -Remediation ""
} else {
    Add-Result -Name "AcrPull role assignment" -Ok $false -Details "AcrPull missing for principalId=$principalId on $acrId." -Remediation "Run scripts/ops/provision/provision_azure.ps1 or grant AcrPull on the ACR."
}

function Test-AppConfig {
  param(
    [string]$Kind,
    [string]$Name,
    [object]$Resource,
    [string]$RegistryServer,
    [string]$IdentityId
  )

  $identity = $null
  $identityProp = $Resource.PSObject.Properties["identity"]
  if ($identityProp) { $identity = $identityProp.Value }

  $identityType = $null
  if ($identity) {
    $typeProp = $identity.PSObject.Properties["type"]
    if ($typeProp) { $identityType = $typeProp.Value }
  }

  $identityTypeLabel = if ($identityType) { [string]$identityType } else { "" }
  if ([string]::IsNullOrWhiteSpace($identityTypeLabel) -or $identityTypeLabel.ToLowerInvariant() -notmatch "userassigned") {
    Add-Result -Name "$Kind identity type" -Ok $false -Details "$Kind '$Name' identity.type is '$identityType'." -Remediation "Assign user-assigned identity $IdentityId to $Kind '$Name'."
  } else {
    Add-Result -Name "$Kind identity type" -Ok $true -Details "$Kind '$Name' identity.type is '$identityType'." -Remediation ""
  }

  $uami = $null
  if ($identity) {
    $uamiProp = $identity.PSObject.Properties["userAssignedIdentities"]
    if ($uamiProp) { $uami = $uamiProp.Value }
  }
  $hasIdentity = $false
  if ($uami -and $IdentityId) {
    $keys = $uami.PSObject.Properties.Name
    $hasIdentity = $keys -contains $IdentityId
  }

  if ($hasIdentity) {
    Add-Result -Name "$Kind identity assignment" -Ok $true -Details "$Kind '$Name' has user-assigned identity $IdentityId." -Remediation ""
  } else {
    Add-Result -Name "$Kind identity assignment" -Ok $false -Details "$Kind '$Name' missing user-assigned identity $IdentityId." -Remediation "Assign the identity and retry deployment."
  }

  $registries = $Resource.properties.configuration.registries
  $registryMatch = $null
  if ($registries) {
    $registryMatch = $registries | Where-Object { $_.server -eq $RegistryServer } | Select-Object -First 1
  }

  if (-not $registryMatch) {
    Add-Result -Name "$Kind registry config" -Ok $false -Details "$Kind '$Name' has no registry entry for $RegistryServer." -Remediation "Run 'az containerapp registry set' for the $Kind and redeploy."
  } else {
    $registryIdentity = $registryMatch.identity
    if ($registryIdentity -eq $IdentityId) {
      Add-Result -Name "$Kind registry config" -Ok $true -Details "$Kind '$Name' registry uses identity $IdentityId." -Remediation ""
    } else {
      Add-Result -Name "$Kind registry config" -Ok $false -Details "$Kind '$Name' registry identity is '$registryIdentity'." -Remediation "Update registry config to use the ACR pull identity."
    }
  }

  $image = $Resource.properties.template.containers[0].image
  if ($image -and $image.StartsWith($RegistryServer + "/")) {
    Add-Result -Name "$Kind image host" -Ok $true -Details "$Kind '$Name' image points to $RegistryServer." -Remediation ""
  } else {
    Add-Result -Name "$Kind image host" -Ok $false -Details "$Kind '$Name' image '$image' does not point to $RegistryServer." -Remediation "Ensure deployment uses images from the expected ACR." -Severity "Warning"
  }
}

foreach ($appName in $AppNames) {
  $app = Invoke-AzJson -AzArgs @("containerapp", "show", "--name", $appName, "--resource-group", $ResourceGroup, "--only-show-errors", "-o", "json") -Context "Container app show ($appName)"
  if (-not $app) {
    Add-Result -Name "App exists" -Ok $false -Details "Container app '$appName' not found." -Remediation "Create the app or correct -AppNames."
    continue
  }

  Test-AppConfig -Kind "App" -Name $appName -Resource $app -RegistryServer $registryServer -IdentityId $identityId
}

foreach ($jobName in $JobNames) {
  $job = Invoke-AzJson -AzArgs @("containerapp", "job", "show", "--name", $jobName, "--resource-group", $ResourceGroup, "--only-show-errors", "-o", "json") -Context "Container app job show ($jobName)"
  if (-not $job) {
    Add-Result -Name "Job exists" -Ok $false -Details "Container app job '$jobName' not found." -Remediation "Create the job or correct -JobNames."
    continue
  }

  Test-AppConfig -Kind "Job" -Name $jobName -Resource $job -RegistryServer $registryServer -IdentityId $identityId
}

Write-Results
