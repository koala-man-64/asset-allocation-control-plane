param(
  [string]$SubscriptionId = "",
  [string]$ResourceGroup = "",
  [string]$AcrName = "",
  [string]$StorageAccountName = "",
  [string]$AcrPullIdentityName = "",
  [string]$AzureClientId = "",
  [string]$OperatorUserObjectId = "",
  [string]$EnvFile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Usage {
  @"
Usage: validate_azure_permissions.ps1 [-SubscriptionId <sub>] [-ResourceGroup <rg>] [-AcrName <name>] [-StorageAccountName <name>] [-AcrPullIdentityName <name>] [-AzureClientId <clientId>] [-EnvFile <path>]

Validates the Azure RBAC permissions required for the GitHub Actions deploy workflow and
Container Apps managed identity operations, plus the Microsoft Graph read access needed by
the Entra OIDC provisioner.
"@
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
    Name        = $Name
    Ok          = $Ok
    Details     = $Details
    Remediation = $Remediation
    Severity    = $Severity
  }
}

function Write-Results {
  $errors = @($results | Where-Object { -not $_.Ok -and $_.Severity -eq "Error" })
  $warnings = @($results | Where-Object { -not $_.Ok -and $_.Severity -eq "Warning" })

  foreach ($result in $results) {
    if ($result.Ok) {
      Write-Host "[OK] $($result.Name) - $($result.Details)"
    }
    else {
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

function Invoke-AzCliRaw {
  param(
    [Parameter(Mandatory = $true)][string[]]$Arguments,
    [switch]$AllowFailure
  )

  $output = & az @Arguments 2>&1
  $exitCode = $LASTEXITCODE
  $text = ($output | Out-String).Trim()

  if ($exitCode -ne 0 -and (-not $AllowFailure)) {
    throw "Azure CLI command failed (exit=$exitCode): az $($Arguments -join ' ')`n$text"
  }

  return [pscustomobject]@{
    ExitCode = $exitCode
    Output   = $text
  }
}

function Get-RoleAssignments {
  param([string]$PrincipalId)

  try {
    $raw = az role assignment list --assignee-object-id $PrincipalId --all -o json --only-show-errors 2>$null
  }
  catch {
    return @()
  }

  if ([string]::IsNullOrWhiteSpace($raw)) { return @() }
  return $raw | ConvertFrom-Json
}

function Has-RoleAtScope {
  param(
    [object[]]$Assignments,
    [string[]]$RoleNames,
    [string]$Scope
  )

  if (-not $Assignments -or [string]::IsNullOrWhiteSpace($Scope)) { return $false }
  $target = $Scope.Trim().ToLowerInvariant()

  foreach ($assignment in $Assignments) {
    $roleName = $assignment.roleDefinitionName
    if (-not $roleName) { continue }
    if ($RoleNames -notcontains $roleName) { continue }

    $assignmentScope = ($assignment.scope -as [string])
    if ([string]::IsNullOrWhiteSpace($assignmentScope)) { continue }
    $assignmentScope = $assignmentScope.Trim().ToLowerInvariant()

    if ($target.StartsWith($assignmentScope)) {
      return $true
    }
  }

  return $false
}

function Add-GraphProbeResult {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$Url,
    [Parameter(Mandatory = $true)][string]$Remediation,
    [ValidateSet("Error", "Warning")][string]$Severity = "Error"
  )

  Write-Host "Checking $Name..." -ForegroundColor DarkGray

  $probe = Invoke-AzCliRaw -Arguments @(
    "rest",
    "--method", "get",
    "--url", $Url,
    "--only-show-errors"
  ) -AllowFailure

  if ($probe.ExitCode -eq 0) {
    Add-Result -Name $Name -Ok $true -Details "Graph read probe succeeded." -Remediation ""
    return
  }

  $details = if ([string]::IsNullOrWhiteSpace($probe.Output)) {
    "Graph read probe failed."
  } else {
    "Graph read probe failed: $($probe.Output)"
  }
  Add-Result -Name $Name -Ok $false -Details $details -Remediation $Remediation -Severity $Severity
}

function Resolve-SignedInUser {
  $probe = Invoke-AzCliRaw -Arguments @(
    "ad", "signed-in-user", "show",
    "--query", "{id:id,userPrincipalName:userPrincipalName}",
    "-o", "json",
    "--only-show-errors"
  ) -AllowFailure

  if ($probe.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($probe.Output)) {
    return $null
  }

  try {
    $resolved = $probe.Output | ConvertFrom-Json
  }
  catch {
    return $null
  }

  if ($null -eq $resolved -or [string]::IsNullOrWhiteSpace([string]$resolved.id)) {
    return $null
  }

  return $resolved
}

$results = @()

try {
  $null = az account show --query id -o tsv 2>$null
}
catch {
  Write-Error "Azure CLI not logged in. Run 'az login' or 'az login --tenant <tenant>' and retry."
  exit 2
}

$envPath = Resolve-EnvFilePath -EnvFileOverride $EnvFile
$envLines = @()
if (Test-Path $envPath) { $envLines = Get-Content $envPath }
$envLabel = Split-Path -Leaf $envPath

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = ($env:AZURE_SUBSCRIPTION_ID, $env:SUBSCRIPTION_ID) | Where-Object { $_ } | Select-Object -First 1
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = (az account show --query id -o tsv) -replace "`r", ""
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
  $AcrName = "assetallocationacr"
}

if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
  $StorageAccountName = Get-EnvValueFirst -Keys @("AZURE_STORAGE_ACCOUNT_NAME") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
  $StorageAccountName = "assetallocstorage001"
}

if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = Get-EnvValueFirst -Keys @("ACR_PULL_IDENTITY_NAME", "ACR_PULL_USER_ASSIGNED_IDENTITY_NAME") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = "asset-allocation-acr-pull-mi"
}

if ([string]::IsNullOrWhiteSpace($AzureClientId)) {
  $AzureClientId = Get-EnvValueFirst -Keys @("AZURE_CLIENT_ID", "CLIENT_ID") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($AzureClientId)) {
  $AzureClientId = $env:AZURE_CLIENT_ID
}

if ([string]::IsNullOrWhiteSpace($OperatorUserObjectId)) {
  $OperatorUserObjectId = Get-EnvValueFirst -Keys @("ENTRA_OPERATOR_USER_OBJECT_ID") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($OperatorUserObjectId)) {
  $OperatorUserObjectId = $env:ENTRA_OPERATOR_USER_OBJECT_ID
}

$signedInOperatorUser = $null
if ([string]::IsNullOrWhiteSpace($OperatorUserObjectId)) {
  $signedInOperatorUser = Resolve-SignedInUser
  if ($null -ne $signedInOperatorUser) {
    $OperatorUserObjectId = [string]$signedInOperatorUser.id
  }
}

Write-Host "Loaded env values from $envLabel" -ForegroundColor Cyan
Write-Host "SubscriptionId: $SubscriptionId"
Write-Host "ResourceGroup: $ResourceGroup"
Write-Host "ACR: $AcrName"
Write-Host "Storage: $StorageAccountName"
Write-Host "ACR Pull Identity: $AcrPullIdentityName"
Write-Host "Azure Client ID: $AzureClientId"
Write-Host "Operator User Object ID: $(if ($OperatorUserObjectId) { $OperatorUserObjectId } else { '<not set>' })"
Write-Host ""

$rgId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup"

$acrId = ""
try {
  $acrId = (az acr show --name $AcrName --resource-group $ResourceGroup --query id -o tsv --only-show-errors) -replace "`r", ""
  if ([string]::IsNullOrWhiteSpace($acrId)) { throw "ACR not found" }
  Add-Result -Name "ACR exists" -Ok $true -Details "$AcrName ($acrId)" -Remediation ""
}
catch {
  Add-Result -Name "ACR exists" -Ok $false -Details "ACR '$AcrName' not found in RG '$ResourceGroup'." -Remediation "Provision the ACR or update -AcrName/-ResourceGroup."
}

$storageId = ""
try {
  $storageId = (az storage account show --name $StorageAccountName --resource-group $ResourceGroup --query id -o tsv --only-show-errors) -replace "`r", ""
  if ([string]::IsNullOrWhiteSpace($storageId)) { throw "Storage not found" }
  Add-Result -Name "Storage account exists" -Ok $true -Details "$StorageAccountName ($storageId)" -Remediation ""
}
catch {
  Add-Result -Name "Storage account exists" -Ok $false -Details "Storage '$StorageAccountName' not found in RG '$ResourceGroup'." -Remediation "Provision the storage account or update -StorageAccountName/-ResourceGroup."
}

$identityId = ""
$acrPullPrincipalId = ""
$acrPullClientId = ""
try {
  $identityId = (az identity show --name $AcrPullIdentityName --resource-group $ResourceGroup --query id -o tsv --only-show-errors) -replace "`r", ""
  if ([string]::IsNullOrWhiteSpace($identityId)) { throw "Identity not found" }
  $acrPullPrincipalId = (az identity show --ids $identityId --query principalId -o tsv --only-show-errors) -replace "`r", ""
  $acrPullClientId = (az identity show --ids $identityId --query clientId -o tsv --only-show-errors) -replace "`r", ""
  Add-Result -Name "ACR pull identity exists" -Ok $true -Details "$AcrPullIdentityName ($identityId)" -Remediation ""
}
catch {
    Add-Result -Name "ACR pull identity exists" -Ok $false -Details "Managed identity '$AcrPullIdentityName' not found in RG '$ResourceGroup'." -Remediation "Run scripts/ops/provision/provision_azure.ps1 to create the identity."
}

if (-not [string]::IsNullOrWhiteSpace($acrPullPrincipalId) -and -not [string]::IsNullOrWhiteSpace($acrId)) {
  $assignments = Get-RoleAssignments -PrincipalId $acrPullPrincipalId
  $hasAcrPull = Has-RoleAtScope -Assignments $assignments -RoleNames @("AcrPull", "AcrPush", "Owner", "Contributor") -Scope $acrId
  Add-Result -Name "ACR pull identity has AcrPull" -Ok $hasAcrPull -Details "principalId=$acrPullPrincipalId" -Remediation "Grant AcrPull on $AcrName to $AcrPullIdentityName."

  $hasContributor = Has-RoleAtScope -Assignments $assignments -RoleNames @("Contributor", "Owner") -Scope $rgId
  Add-Result -Name "ACR pull identity has RG Contributor" -Ok $hasContributor -Details "principalId=$acrPullPrincipalId" -Remediation "Grant Contributor on $ResourceGroup to $AcrPullIdentityName for job start, API container-app wake, and system health actions." -Severity "Warning"

  if (-not [string]::IsNullOrWhiteSpace($storageId)) {
    $hasStorageData = Has-RoleAtScope -Assignments $assignments -RoleNames @("Storage Blob Data Contributor", "Storage Blob Data Owner") -Scope $storageId
    Add-Result -Name "ACR pull identity has storage data access" -Ok $hasStorageData -Details "principalId=$acrPullPrincipalId" -Remediation "Grant Storage Blob Data Contributor on $StorageAccountName to $AcrPullIdentityName."
  }
}

$azureSpObjectId = ""
if (-not [string]::IsNullOrWhiteSpace($AzureClientId)) {
  try {
    $azureSpObjectId = (az ad sp show --id $AzureClientId --query id -o tsv --only-show-errors) -replace "`r", ""
  }
  catch {
    $azureSpObjectId = ""
  }

  if ([string]::IsNullOrWhiteSpace($azureSpObjectId)) {
    Add-Result -Name "Azure client SP resolved" -Ok $false -Details "Failed to resolve service principal for clientId '$AzureClientId'." -Remediation "Ensure the Azure client ID is correct and you have permission to query AAD." -Severity "Warning"
  }
  else {
    Add-Result -Name "Azure client SP resolved" -Ok $true -Details "objectId=$azureSpObjectId" -Remediation ""
  }
}
else {
  Add-Result -Name "Azure client ID set" -Ok $false -Details "AZURE_CLIENT_ID not found in $envLabel or env." -Remediation "Set AZURE_CLIENT_ID to the GitHub Actions service principal client ID." -Severity "Warning"
}

if (-not [string]::IsNullOrWhiteSpace($azureSpObjectId)) {
  $assignments = Get-RoleAssignments -PrincipalId $azureSpObjectId

  $hasContributor = Has-RoleAtScope -Assignments $assignments -RoleNames @("Contributor", "Owner") -Scope $rgId
  Add-Result -Name "Deploy SP has RG Contributor" -Ok $hasContributor -Details "clientId=$AzureClientId" -Remediation "Grant Contributor on $ResourceGroup to the deployment service principal."

  if (-not [string]::IsNullOrWhiteSpace($acrId)) {
    $hasAcrPush = Has-RoleAtScope -Assignments $assignments -RoleNames @("AcrPush", "Owner", "Contributor") -Scope $acrId
    Add-Result -Name "Deploy SP has AcrPush" -Ok $hasAcrPush -Details "clientId=$AzureClientId" -Remediation "Grant AcrPush on $AcrName to the deployment service principal."
  }

  if (-not [string]::IsNullOrWhiteSpace($identityId)) {
    $hasMiOperator = Has-RoleAtScope -Assignments $assignments -RoleNames @("Managed Identity Operator", "Owner") -Scope $identityId
    Add-Result -Name "Deploy SP can assign managed identity" -Ok $hasMiOperator -Details "clientId=$AzureClientId" -Remediation "Grant Managed Identity Operator on $AcrPullIdentityName to the deployment service principal."
  }

  if (-not [string]::IsNullOrWhiteSpace($storageId)) {
    $hasStorageData = Has-RoleAtScope -Assignments $assignments -RoleNames @("Storage Blob Data Contributor", "Storage Blob Data Owner") -Scope $storageId
    Add-Result -Name "Deploy SP has storage data access" -Ok $hasStorageData -Details "clientId=$AzureClientId" -Remediation "Grant Storage Blob Data Contributor on $StorageAccountName to the deployment service principal (for 'az storage container create --auth-mode login')."
  }
}

if ([string]::IsNullOrWhiteSpace($OperatorUserObjectId)) {
  Add-Result -Name "Operator user object ID set" -Ok $false -Details "ENTRA_OPERATOR_USER_OBJECT_ID not found in $envLabel or env, and Azure CLI could not resolve a signed-in user." -Remediation "Run 'az ad signed-in-user show --query id -o tsv' and set ENTRA_OPERATOR_USER_OBJECT_ID before running the Entra OIDC provisioner." -Severity "Warning"
}
else {
  $details = $OperatorUserObjectId
  if ($null -ne $signedInOperatorUser) {
    $suffix = [string]$signedInOperatorUser.userPrincipalName
    if (-not [string]::IsNullOrWhiteSpace($suffix)) {
      $details = "$OperatorUserObjectId (auto-resolved from signed-in user $suffix)"
    }
    else {
      $details = "$OperatorUserObjectId (auto-resolved from signed-in user)"
    }
  }
  Add-Result -Name "Operator user object ID set" -Ok $true -Details $details -Remediation ""
}

Add-GraphProbeResult `
  -Name "Graph applications read" `
  -Url "https://graph.microsoft.com/v1.0/applications?`$top=1" `
      -Remediation "Ensure the signed-in Azure CLI principal can read Microsoft Entra applications before running scripts/ops/provision/provision_entra_oidc.ps1."

Add-GraphProbeResult `
  -Name "Graph service principals read" `
  -Url "https://graph.microsoft.com/v1.0/servicePrincipals?`$top=1" `
      -Remediation "Ensure the signed-in Azure CLI principal can read Microsoft Entra service principals before running scripts/ops/provision/provision_entra_oidc.ps1."

if (-not [string]::IsNullOrWhiteSpace($OperatorUserObjectId)) {
  Add-GraphProbeResult `
    -Name "Graph operator user read" `
    -Url "https://graph.microsoft.com/v1.0/users/${OperatorUserObjectId}?`$select=id,displayName,userPrincipalName" `
    -Remediation "Confirm ENTRA_OPERATOR_USER_OBJECT_ID is a valid user object ID and the signed-in principal can read directory users." `
    -Severity "Warning"
}

Write-Results
