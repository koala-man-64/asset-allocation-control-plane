param(
  [string]$SubscriptionId = "",

  [string]$Location = "eastus",
  [string]$ResourceGroup = "AssetAllocationRG",

  [string]$StorageAccountName = "assetallocstorage001",

  [string[]]$StorageContainers = @(),
  [string]$AcrName = "assetallocationacr",
  # User-assigned managed identity used by Container Apps/Jobs to pull from ACR on first create.
  [string]$AcrPullIdentityName = "asset-allocation-acr-pull-mi",
  [switch]$EnableAcrAdmin,
  [switch]$EmitSecrets,
  [switch]$GrantAcrPullToAcaResources,
  # Best-effort: assign the ACR pull user-assigned identity to existing apps/jobs and configure
  # their registry to use that identity (instead of username/password).
  [switch]$ConfigureAcrPullIdentityOnAcaResources,
  [switch]$GrantJobStartToAcaResources,

  [switch]$ProvisionPostgres,
  [switch]$SkipPostgresPrompt,
  [string]$PostgresServerName = "pg-asset-allocation",
  [string]$PostgresDatabaseName = "asset_allocation",
  [string]$PostgresAdminUser = "assetallocadmin",
  [string]$PostgresAdminPassword = "",
  [switch]$PostgresApplyMigrations,
  [switch]$PostgresUseDockerPsql,
  [switch]$PostgresCreateAppUsers,
  [string]$PostgresBacktestServiceUser = "backtest_service",
  [string]$PostgresBacktestServicePassword = "",
  [string]$PostgresSkuName = "standard_b1ms",
  [string]$PostgresTier = "",
  [int]$PostgresStorageSizeGiB = 32,
  [string]$PostgresVersion = "16",
  [ValidateSet("Disabled", "Enabled", "All", "None")]
  [string]$PostgresPublicAccess = "Enabled",
  [bool]$PostgresAllowAzureServices = $true,
  [string]$PostgresAllowIpRangeStart = "",
  [string]$PostgresAllowIpRangeEnd = "",
  [bool]$PostgresAllowCurrentClientIp = $true,
  [switch]$PostgresEmitSecrets,
  [string[]]$PostgresLocationFallback = @("eastus2", "centralus", "westus2"),

  [switch]$PromptForResources = $true,
  [switch]$NonInteractive,

  [string]$LogAnalyticsWorkspaceName = "asset-allocation-law",
  [ValidateRange(4, 730)]
  [int]$LogAnalyticsRetentionInDays = 30,
  [string]$ContainerAppsEnvironmentName = "asset-allocation-env",
  [switch]$CorrectApiStorageAuthMode,
  [ValidateSet("ManagedIdentity", "ConnectionString")]
  [string]$ApiStorageAuthMode = "ManagedIdentity",
  [string]$ApiContainerAppName = "",
  [string]$AzureClientId = "",
  [string]$AksClusterName = "",
  [string]$KubernetesNamespace = "k8se-apps",
  [string]$ServiceAccountName = "asset-allocation-sa",
  [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$githubSpObjectId = $null

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

$repoRoot = Resolve-RepoRoot

$envPath = $EnvFile
if ([string]::IsNullOrWhiteSpace($envPath)) {
  $candidateWeb = Join-Path $repoRoot ".env.web"
  $candidateEnv = Join-Path $repoRoot ".env"

  if (Test-Path $candidateWeb) {
    $envPath = $candidateWeb
  }
  elseif (Test-Path $candidateEnv) {
    $envPath = $candidateEnv
  }
  else {
    $envPath = $candidateWeb
  }
}
$envLabel = Split-Path -Leaf $envPath

$envLines = @()
if (Test-Path $envPath) {
  $envLines = Get-Content $envPath
}
else {
  throw "Env file not found at '$envPath'. Provide -EnvFile or create '.env' (recommended) or '.env.web'."
}

Write-Host "Loaded configuration from $envLabel" -ForegroundColor Cyan

function Get-YesNo {
  param(
    [Parameter(Mandatory = $true)][string]$Prompt,
    [bool]$DefaultYes = $true
  )

  if ($NonInteractive -or (-not $PromptForResources)) {
    return $true
  }

  $suffix = if ($DefaultYes) { "[Y/n]" } else { "[y/N]" }
  while ($true) {
    $input = Read-Host "$Prompt $suffix"
    if ([string]::IsNullOrWhiteSpace($input)) { return $DefaultYes }
    $value = $input.Trim().ToLowerInvariant()
    if ($value -in @("y", "yes")) { return $true }
    if ($value -in @("n", "no")) { return $false }
    Write-Host "Please enter y or n."
  }
}

$grantAcrPullPrompted = $false
$grantJobStartPrompted = $false

if (-not $PSBoundParameters.ContainsKey("ProvisionPostgres") -and (-not $SkipPostgresPrompt) -and $PromptForResources -and (-not $NonInteractive)) {
  $ProvisionPostgres = Get-YesNo "Provision Postgres Flexible Server?" $false
}

if (-not $PSBoundParameters.ContainsKey("GrantAcrPullToAcaResources") -and $PromptForResources -and (-not $NonInteractive)) {
  $GrantAcrPullToAcaResources = Get-YesNo "Grant AcrPull to existing Container Apps/Jobs?" $false
  $grantAcrPullPrompted = $true
}

if (-not $PSBoundParameters.ContainsKey("GrantJobStartToAcaResources") -and $PromptForResources -and (-not $NonInteractive)) {
  $GrantJobStartToAcaResources = Get-YesNo "Grant job/container-app start permissions to ACR pull identity?" $false
  $grantJobStartPrompted = $true
}

if (-not $PSBoundParameters.ContainsKey("ConfigureAcrPullIdentityOnAcaResources") -and $PromptForResources -and (-not $NonInteractive)) {
  $ConfigureAcrPullIdentityOnAcaResources = Get-YesNo "Configure existing Container Apps/Jobs to pull from ACR via the user-assigned identity ($AcrPullIdentityName)?" $false
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string[]]$Lines = $envLines
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
    [Parameter(Mandatory = $true)][string[]]$Keys
  )
  foreach ($key in $Keys) {
    $value = Get-EnvValue -Key $key
    if ($value) {
      return $value
    }
  }
  return $null
}

function Resolve-LogAnalyticsRetentionTarget {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$WorkspaceName,
    [Parameter(Mandatory = $true)][int]$RequestedRetentionInDays
  )

  $workspace = $null
  try {
    $rawWorkspace = az monitor log-analytics workspace show `
      --resource-group $ResourceGroupName `
      --workspace-name $WorkspaceName `
      --only-show-errors -o json 2>$null
    if (-not [string]::IsNullOrWhiteSpace($rawWorkspace)) {
      $workspace = $rawWorkspace | ConvertFrom-Json
    }
  }
  catch {
    $workspace = $null
  }

  $skuName = ""
  $currentRetentionInDays = $null
  if ($null -ne $workspace) {
    if ($workspace.sku -and $workspace.sku.name) {
      $skuName = [string]$workspace.sku.name
    }
    if ($workspace.PSObject.Properties.Name -contains "retentionInDays") {
      $currentRetentionInDays = [int]$workspace.retentionInDays
    }
  }

  $effectiveRetentionInDays = $RequestedRetentionInDays
  if ($skuName -eq "PerGB2018" -and $effectiveRetentionInDays -lt 30) {
    Write-Warning "Log Analytics workspace '$WorkspaceName' uses SKU '$skuName', so retention cannot be set below 30 days. Requested=$RequestedRetentionInDays; using 30."
    $effectiveRetentionInDays = 30
  }

  return [pscustomobject]@{
    WorkspaceSkuName          = $skuName
    CurrentRetentionInDays    = $currentRetentionInDays
    EffectiveRetentionInDays  = $effectiveRetentionInDays
  }
}

function Get-EnvBool {
  param(
    [Parameter(Mandatory = $true)][string]$Key
  )

  $raw = Get-EnvValue -Key $Key
  if ([string]::IsNullOrWhiteSpace($raw)) {
    return $null
  }

  $v = $raw.Trim().ToLowerInvariant()
  if ($v -in @("1", "true", "yes", "y", "on")) { return $true }
  if ($v -in @("0", "false", "no", "n", "off")) { return $false }

  throw "Invalid boolean value for $Key in ${envLabel}: '$raw'. Expected true/false."
}

# Load containers from .env.web if not specified
if ($StorageContainers.Count -eq 0 -and $envLines.Count -gt 0) {
  Write-Host "Reading container names from $envLabel..."
  $containers = @()
  foreach ($line in $envLines) {
    if ($line -match "^AZURE_CONTAINER_[^=]+=(.*)$") {
      $val = $matches[1].Trim('"').Trim("'")
      Write-Host "Found container: $val" -ForegroundColor Cyan
      $containers += $val
    }
  }
  if ($containers.Count -gt 0) {
    $StorageContainers = $containers | Select-Object -Unique
  }
}

if ((-not $PSBoundParameters.ContainsKey("SubscriptionId")) -or [string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $subscriptionFromEnv = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID")
  if ($subscriptionFromEnv) {
    Write-Host "Using AZURE_SUBSCRIPTION_ID from ${envLabel}: $subscriptionFromEnv"
    $SubscriptionId = $subscriptionFromEnv
  }
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  throw "SubscriptionId is required. Provide -SubscriptionId or set AZURE_SUBSCRIPTION_ID in $envLabel."
}

if ((-not $PSBoundParameters.ContainsKey("ResourceGroup")) -or [string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $resourceGroupFromEnv = Get-EnvValueFirst -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
  if ($resourceGroupFromEnv) {
    Write-Host "Using RESOURCE_GROUP from ${envLabel}: $resourceGroupFromEnv"
    $ResourceGroup = $resourceGroupFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("Location")) -or [string]::IsNullOrWhiteSpace($Location)) {
  $locationFromEnv = Get-EnvValueFirst -Keys @("AZURE_LOCATION", "AZURE_REGION", "LOCATION")
  if ($locationFromEnv) {
    Write-Host "Using AZURE_LOCATION from ${envLabel}: $locationFromEnv"
    $Location = $locationFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("StorageAccountName")) -or [string]::IsNullOrWhiteSpace($StorageAccountName)) {
  $storageFromEnv = Get-EnvValueFirst -Keys @("AZURE_STORAGE_ACCOUNT_NAME")
  if ($storageFromEnv) {
    Write-Host "Using AZURE_STORAGE_ACCOUNT_NAME from ${envLabel}: $storageFromEnv"
    $StorageAccountName = $storageFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("AcrName")) -or [string]::IsNullOrWhiteSpace($AcrName)) {
  $acrFromEnv = Get-EnvValueFirst -Keys @("ACR_NAME", "AZURE_ACR_NAME")
  if ($acrFromEnv) {
    Write-Host "Using ACR_NAME from ${envLabel}: $acrFromEnv"
    $AcrName = $acrFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("AzureClientId")) -or [string]::IsNullOrWhiteSpace($AzureClientId)) {
  $azureClientIdFromEnv = Get-EnvValueFirst -Keys @("AZURE_CLIENT_ID", "CLIENT_ID")
  if ($azureClientIdFromEnv) {
    Write-Host "Using AZURE_CLIENT_ID from ${envLabel}: $azureClientIdFromEnv"
    $AzureClientId = $azureClientIdFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("AcrPullIdentityName")) -or [string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $acrPullIdentityNameFromEnv = Get-EnvValueFirst -Keys @("ACR_PULL_IDENTITY_NAME", "ACR_PULL_USER_ASSIGNED_IDENTITY_NAME")
  if ($acrPullIdentityNameFromEnv) {
    Write-Host "Using ACR_PULL_IDENTITY_NAME from ${envLabel}: $acrPullIdentityNameFromEnv"
    $AcrPullIdentityName = $acrPullIdentityNameFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("LogAnalyticsWorkspaceName")) -or [string]::IsNullOrWhiteSpace($LogAnalyticsWorkspaceName)) {
  $lawFromEnv = Get-EnvValueFirst -Keys @("LOG_ANALYTICS_WORKSPACE_NAME", "LOG_ANALYTICS_WORKSPACE")
  if ($lawFromEnv) {
    Write-Host "Using LOG_ANALYTICS_WORKSPACE_NAME from ${envLabel}: $lawFromEnv"
    $LogAnalyticsWorkspaceName = $lawFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("ContainerAppsEnvironmentName")) -or [string]::IsNullOrWhiteSpace($ContainerAppsEnvironmentName)) {
  $envFromEnv = Get-EnvValueFirst -Keys @("CONTAINER_APPS_ENVIRONMENT_NAME", "CONTAINERAPPS_ENVIRONMENT_NAME", "ACA_ENVIRONMENT_NAME")
  if ($envFromEnv) {
    Write-Host "Using CONTAINER_APPS_ENVIRONMENT_NAME from ${envLabel}: $envFromEnv"
    $ContainerAppsEnvironmentName = $envFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("ApiContainerAppName")) -or [string]::IsNullOrWhiteSpace($ApiContainerAppName)) {
  $apiContainerAppFromEnv = Get-EnvValueFirst -Keys @("API_CONTAINER_APP_NAME", "CONTAINER_APP_API_NAME")
  if (-not [string]::IsNullOrWhiteSpace($apiContainerAppFromEnv)) {
    $ApiContainerAppName = $apiContainerAppFromEnv.Trim()
    Write-Host "Using API_CONTAINER_APP_NAME from ${envLabel}: $ApiContainerAppName"
  }
  else {
    $containerAppsRaw = Get-EnvValue -Key "SYSTEM_HEALTH_ARM_CONTAINERAPPS"
    if (-not [string]::IsNullOrWhiteSpace($containerAppsRaw)) {
      $containerApps = @(
        $containerAppsRaw.Split(",") |
          ForEach-Object { $_.Trim() } |
          Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
      )
      if ($containerApps.Count -gt 0) {
        $apiMatch = $containerApps | Where-Object { $_.ToLowerInvariant().Contains("api") } | Select-Object -First 1
        if (-not [string]::IsNullOrWhiteSpace($apiMatch)) {
          $ApiContainerAppName = $apiMatch
        }
        else {
          $ApiContainerAppName = $containerApps[0]
        }
        Write-Host "Using API container app inferred from SYSTEM_HEALTH_ARM_CONTAINERAPPS: $ApiContainerAppName"
      }
    }
  }
}

if ([string]::IsNullOrWhiteSpace($ApiContainerAppName)) {
  $ApiContainerAppName = "asset-allocation-api"
}

if ((-not $PSBoundParameters.ContainsKey("ServiceAccountName")) -or [string]::IsNullOrWhiteSpace($ServiceAccountName)) {
  $serviceAccountFromEnv = Get-EnvValue -Key "SERVICE_ACCOUNT_NAME"
  if ($serviceAccountFromEnv) {
    Write-Host "Using SERVICE_ACCOUNT_NAME from ${envLabel}: $serviceAccountFromEnv"
    $ServiceAccountName = $serviceAccountFromEnv
  }
}

if (-not $PSBoundParameters.ContainsKey("EnableAcrAdmin")) {
  $enableAcrAdminFromEnv = Get-EnvBool -Key "ENABLE_ACR_ADMIN"
  if ($enableAcrAdminFromEnv -ne $null) {
    Write-Host "Using ENABLE_ACR_ADMIN from ${envLabel}: $enableAcrAdminFromEnv"
    $EnableAcrAdmin = $enableAcrAdminFromEnv
  }
}

if (-not $PSBoundParameters.ContainsKey("EmitSecrets")) {
  $emitSecretsFromEnv = Get-EnvBool -Key "EMIT_SECRETS"
  if ($emitSecretsFromEnv -ne $null) {
    Write-Host "Using EMIT_SECRETS from ${envLabel}: $emitSecretsFromEnv"
    $EmitSecrets = $emitSecretsFromEnv
  }
}

if (-not $PSBoundParameters.ContainsKey("GrantAcrPullToAcaResources") -and (-not $grantAcrPullPrompted)) {
  $grantAcrPullFromEnv = Get-EnvBool -Key "GRANT_ACR_PULL_TO_ACA_RESOURCES"
  if ($grantAcrPullFromEnv -ne $null) {
    Write-Host "Using GRANT_ACR_PULL_TO_ACA_RESOURCES from ${envLabel}: $grantAcrPullFromEnv"
    $GrantAcrPullToAcaResources = $grantAcrPullFromEnv
  }
}

if (-not $PSBoundParameters.ContainsKey("GrantJobStartToAcaResources") -and (-not $grantJobStartPrompted)) {
  $grantJobStartFromEnv = Get-EnvBool -Key "GRANT_JOB_START_TO_ACA_RESOURCES"
  if ($grantJobStartFromEnv -ne $null) {
    Write-Host "Using GRANT_JOB_START_TO_ACA_RESOURCES from ${envLabel}: $grantJobStartFromEnv"
    $GrantJobStartToAcaResources = $grantJobStartFromEnv
  }
}

# If still empty, fall back to defaults (or error? original script had defaults)
if ($StorageContainers.Count -eq 0) {
  Write-Warning "No containers found in $envLabel and none provided. Using defaults."
  $StorageContainers = @("bronze", "silver", "gold", "platinum", "common")
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

Assert-CommandExists -Name "az"
if ($AksClusterName) {
  Assert-CommandExists -Name "kubectl"
}

function Set-ApiStorageAuthMode {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$ContainerAppName,
    [Parameter(Mandatory = $true)][ValidateSet("ManagedIdentity", "ConnectionString")][string]$AuthMode,
    [Parameter(Mandatory = $true)][string]$StorageAccount
  )

  if ([string]::IsNullOrWhiteSpace($ContainerAppName)) {
    throw "ApiContainerAppName is required when -CorrectApiStorageAuthMode is set."
  }

  $existingAppName = ""
  try {
    $existingAppName = az containerapp show `
      --name $ContainerAppName `
      --resource-group $ResourceGroupName `
      --query "name" -o tsv --only-show-errors 2>$null
  }
  catch {
    $existingAppName = ""
  }

  if ([string]::IsNullOrWhiteSpace($existingAppName)) {
    throw "Container App '$ContainerAppName' was not found in resource group '$ResourceGroupName'."
  }

  $setEnvVars = @("AZURE_STORAGE_ACCOUNT_NAME=$StorageAccount")
  $removeEnvVars = @("AZURE_STORAGE_ACCOUNT_KEY", "AZURE_STORAGE_ACCESS_KEY", "AZURE_STORAGE_SAS_TOKEN")

  if ($AuthMode -eq "ManagedIdentity") {
    $removeEnvVars += "AZURE_STORAGE_CONNECTION_STRING"
  }
  elseif ($AuthMode -eq "ConnectionString") {
    $connectionString = Get-EnvValue -Key "AZURE_STORAGE_CONNECTION_STRING"
    if ([string]::IsNullOrWhiteSpace($connectionString)) {
      $connectionString = az storage account show-connection-string `
        --name $StorageAccount `
        --resource-group $ResourceGroupName `
        --query connectionString -o tsv --only-show-errors 2>$null
    }
    if ([string]::IsNullOrWhiteSpace($connectionString)) {
      throw "ConnectionString auth mode requested, but no AZURE_STORAGE_CONNECTION_STRING was found in ${envLabel} and Azure CLI could not resolve one."
    }

    $secretName = "azure-storage-connection-string"
    Write-Host "Setting storage connection string secret on container app '$ContainerAppName'..."
    $secretArgs = @(
      "containerapp", "secret", "set",
      "--name", $ContainerAppName,
      "--resource-group", $ResourceGroupName,
      "--secrets", "$secretName=$connectionString",
      "--only-show-errors"
    )
    & az @secretArgs 1>$null
    if ($LASTEXITCODE -ne 0) {
      throw "Failed to set storage connection string secret on container app '$ContainerAppName'."
    }

    $setEnvVars += "AZURE_STORAGE_CONNECTION_STRING=secretref:$secretName"
  }

  Write-Host "Applying storage auth mode '$AuthMode' to container app '$ContainerAppName'..." -ForegroundColor Cyan
  $setArgs = @(
    "containerapp", "update",
    "--name", $ContainerAppName,
    "--resource-group", $ResourceGroupName,
    "--set-env-vars"
  )
  $setArgs += $setEnvVars
  $setArgs += "--only-show-errors"
  & az @setArgs 1>$null
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to update storage auth env vars on container app '$ContainerAppName'."
  }

  $removeTargets = $removeEnvVars | Sort-Object -Unique
  if ($removeTargets.Count -gt 0) {
    $removeArgs = @(
      "containerapp", "update",
      "--name", $ContainerAppName,
      "--resource-group", $ResourceGroupName,
      "--remove-env-vars"
    )
    $removeArgs += $removeTargets
    $removeArgs += "--only-show-errors"
    & az @removeArgs 1>$null
    if ($LASTEXITCODE -ne 0) {
      throw "Failed to remove conflicting storage auth env vars on container app '$ContainerAppName'."
    }
  }

  $authBindings = az containerapp show `
    --name $ContainerAppName `
    --resource-group $ResourceGroupName `
    --query "properties.template.containers[0].env[?name=='AZURE_STORAGE_ACCOUNT_NAME' || name=='AZURE_STORAGE_CONNECTION_STRING' || name=='AZURE_STORAGE_ACCOUNT_KEY' || name=='AZURE_STORAGE_ACCESS_KEY' || name=='AZURE_STORAGE_SAS_TOKEN'].{name:name,secretRef:secretRef,value:value}" `
    -o table --only-show-errors

  Write-Host "Effective storage auth env bindings for '$ContainerAppName':"
  Write-Host $authBindings
}

$acrLoginServer = ""
$acrId = ""
$acrPullIdentityId = ""
$acrPullIdentityClientId = ""
$acrPullIdentityPrincipalId = ""

if (-not $NonInteractive -and $PromptForResources) {
  Write-Host ""
  Write-Host "Resource provisioning prompts (set -NonInteractive to skip prompts)" -ForegroundColor Cyan
}

if ($AzureClientId) {
  $doFederatedCredential = Get-YesNo "Ensure GitHub Actions federated credential on Azure app ($AzureClientId)?" $true
}

if ($AzureClientId -and $doFederatedCredential) {
  Write-Host "Checking for existing Federated Credential 'github-actions-production'..."
  $paramsFile = "credential.json"
  $subject = "repo:koala-man-64/asset-allocation:environment:production"
    
  # Check if exists
  $creds = az ad app federated-credential list --id $AzureClientId --query "[?name=='github-actions-production']" -o json | ConvertFrom-Json
    
  if (-not $creds) {
    Write-Host "Creating Federated Credential 'github-actions-production'..."
    $json = @{
      name        = "github-actions-production"
      issuer      = "https://token.actions.githubusercontent.com"
      subject     = $subject
      description = "GitHub Actions Production Environment"
      audiences   = @("api://AzureADTokenExchange")
    } | ConvertTo-Json -Compress

    Set-Content -Path $paramsFile -Value $json
        
    try {
      az ad app federated-credential create --id $AzureClientId --parameters $paramsFile 2>&1
      Write-Host "Successfully created federated credential."
    }
    catch {
      Write-Error "Failed to create federated credential: $_"
      if (Test-Path $paramsFile) { Remove-Item $paramsFile }
      throw
    }
        
    if (Test-Path $paramsFile) { Remove-Item $paramsFile }
  }
  else {
    Write-Host "Federated Credential 'github-actions-production' already exists."
  }
}

Write-Host "Using subscription: $SubscriptionId"
az account set --subscription $SubscriptionId 1>$null

Write-Host "Ensuring required Azure resource providers are registered..."
$providers = @(
  "Microsoft.Storage",
  "Microsoft.ContainerRegistry",
  "Microsoft.ManagedIdentity",
  "Microsoft.OperationalInsights",
  "Microsoft.App"
)
foreach ($p in $providers) {
  az provider register --namespace $p 1>$null
}

Write-Host "Ensuring Azure CLI extensions are installed..."
az extension add --name containerapp --upgrade --only-show-errors 1>$null

$doResourceGroup = Get-YesNo "Ensure resource group exists: $ResourceGroup ($Location)?" $true
if ($doResourceGroup) {
  Write-Host "Ensuring resource group exists: $ResourceGroup ($Location)"
  az group create --name $ResourceGroup --location $Location --only-show-errors 1>$null
}

if ($ProvisionPostgres) {
  Write-Host ""
  Write-Host "Provisioning Postgres Flexible Server..."
  $postgresScript = Join-Path $repoRoot "scripts\ops\provision\provision_azure_postgres.ps1"
  if (-not (Test-Path $postgresScript)) {
    throw "Postgres provisioning script not found at $postgresScript"
  }

  $postgresArgs = @{
    Location             = $Location
    LocationFallback     = $PostgresLocationFallback
    SubscriptionId       = $SubscriptionId
    ResourceGroup        = $ResourceGroup
    ServerName           = $PostgresServerName
    DatabaseName         = $PostgresDatabaseName
    AdminUser            = $PostgresAdminUser
    SkuName              = $PostgresSkuName
    Tier                 = $PostgresTier
    StorageSizeGiB       = $PostgresStorageSizeGiB
    PostgresVersion      = $PostgresVersion
    PublicAccess         = $PostgresPublicAccess
    AllowAzureServices   = $PostgresAllowAzureServices
    AllowCurrentClientIp = $PostgresAllowCurrentClientIp
  }

  if ($PostgresAdminPassword) { $postgresArgs.AdminPassword = $PostgresAdminPassword }
  if ($PostgresAllowIpRangeStart) { $postgresArgs.AllowIpRangeStart = $PostgresAllowIpRangeStart }
  if ($PostgresAllowIpRangeEnd) { $postgresArgs.AllowIpRangeEnd = $PostgresAllowIpRangeEnd }
  if ($PostgresApplyMigrations) { $postgresArgs.ApplyMigrations = $true }
  if ($PostgresUseDockerPsql) { $postgresArgs.UseDockerPsql = $true }
  if ($PostgresCreateAppUsers) { $postgresArgs.CreateAppUsers = $true }
  if ($PostgresBacktestServiceUser) { $postgresArgs.BacktestServiceUser = $PostgresBacktestServiceUser }
  if ($PostgresBacktestServicePassword) { $postgresArgs.BacktestServicePassword = $PostgresBacktestServicePassword }
  if ($PostgresEmitSecrets) { $postgresArgs.EmitSecrets = $true }

  & $postgresScript @postgresArgs
  if (-not $?) { throw "Postgres provisioning failed." }
}

$doStorage = Get-YesNo ("Ensure storage account exists: {0}?" -f $StorageAccountName) $true
if ($doStorage) {
  Write-Host "Ensuring storage account exists: $StorageAccountName"
  $existingStorage = $null
  try {
    $existingStorage = az storage account show `
      --name $StorageAccountName `
      --resource-group $ResourceGroup `
      --only-show-errors -o json 2>$null | ConvertFrom-Json
  }
  catch {
    $existingStorage = $null
  }

  if ($null -eq $existingStorage) {
    $foundInSubscription = $null
    try {
      $foundInSubscription = az storage account show `
        --name $StorageAccountName `
        --only-show-errors -o json 2>$null | ConvertFrom-Json
    }
    catch {
      $foundInSubscription = $null
    }

    if ($null -ne $foundInSubscription) {
      throw "Storage account '$StorageAccountName' already exists in resource group '$($foundInSubscription.resourceGroup)'. Set -ResourceGroup to that value or choose a new -StorageAccountName."
    }

    $nameAvailable = az storage account check-name --name $StorageAccountName --query nameAvailable -o tsv --only-show-errors
    if ($nameAvailable -ne "true") {
      throw "Storage account name '$StorageAccountName' is not available. Choose a different -StorageAccountName."
    }

    az storage account create `
      --name $StorageAccountName `
      --resource-group $ResourceGroup `
      --location $Location `
      --sku Standard_LRS `
      --kind StorageV2 `
      --https-only true `
      --min-tls-version TLS1_2 `
      --allow-blob-public-access false `
      --hns true `
      --only-show-errors 1>$null
  }
  else {
    if (-not [bool]$existingStorage.isHnsEnabled) {
      Write-Warning "Storage account '$StorageAccountName' exists but Hierarchical Namespace (HNS) is disabled. This cannot be enabled after creation; continuing without updating HNS. To use ADLS Gen2, create a new storage account (or delete & recreate) with --hns true."
    }

    az storage account update `
      --name $StorageAccountName `
      --resource-group $ResourceGroup `
      --https-only true `
      --min-tls-version TLS1_2 `
      --allow-blob-public-access false `
      --only-show-errors 1>$null
  }

  $doContainers = Get-YesNo "Create/update blob containers?" $true
  if ($doContainers) {
    Write-Host "Creating blob containers (auth-mode=login)..."
    foreach ($c in $StorageContainers) {
      if (-not $c) { continue }
      az storage container create --name $c --account-name $StorageAccountName --auth-mode login --only-show-errors 1>$null
    }
  }
}

$storageAccountId = ""
try {
  $storageAccountId = az storage account show `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --query id -o tsv --only-show-errors 2>$null
}
catch {
  $storageAccountId = ""
}

if ($AzureClientId -and $storageAccountId) {
  Write-Host ""
  Write-Host "Ensuring GitHub Actions principal can access storage data (Storage Blob Data Contributor)..."

  if (-not $githubSpObjectId) {
    try {
      $githubSpObjectId = az ad sp show --id $AzureClientId --query id -o tsv --only-show-errors 2>$null
    }
    catch {
      $githubSpObjectId = $null
    }
  }

  if ($githubSpObjectId) {
    $storageDataExisting = "0"
    try {
      $storageDataExisting = az role assignment list `
        --assignee-object-id $githubSpObjectId `
        --scope $storageAccountId `
        --query "[?roleDefinitionName=='Storage Blob Data Contributor'] | length(@)" -o tsv --only-show-errors 2>$null
      if (-not $storageDataExisting) { $storageDataExisting = "0" }
    }
    catch {
      $storageDataExisting = "0"
    }

    if ([int]$storageDataExisting -eq 0) {
      az role assignment create `
        --assignee-object-id $githubSpObjectId `
        --assignee-principal-type ServicePrincipal `
        --role "Storage Blob Data Contributor" `
        --scope $storageAccountId `
        --only-show-errors 1>$null
      Write-Host "  Storage Blob Data Contributor granted to $AzureClientId on $StorageAccountName."
    }
    else {
      Write-Host "  Storage Blob Data Contributor already assigned to $AzureClientId on $StorageAccountName."
    }
  }
  else {
    Write-Warning "Could not resolve service principal for AzureClientId '$AzureClientId'. Skipping storage data role assignment."
  }
}

$doAcr = Get-YesNo "Ensure ACR exists: ${AcrName}?" $true
if ($doAcr) {
  Write-Host "Ensuring ACR exists: $AcrName"
  $acrAdmin = if ($EnableAcrAdmin) { "true" } else { "false" }
  az acr create `
    --name $AcrName `
    --resource-group $ResourceGroup `
    --location $Location `
    --sku Basic `
    --admin-enabled $acrAdmin `
    --only-show-errors 1>$null

  $acrLoginServer = az acr show --name $AcrName --resource-group $ResourceGroup --query loginServer -o tsv
  $acrId = az acr show --name $AcrName --resource-group $ResourceGroup --query id -o tsv --only-show-errors
}

$doLogAnalytics = Get-YesNo "Ensure Log Analytics workspace exists: ${LogAnalyticsWorkspaceName}?" $true
if ($doLogAnalytics) {
  Write-Host "Ensuring Log Analytics workspace exists: $LogAnalyticsWorkspaceName"
  az monitor log-analytics workspace create `
    --resource-group $ResourceGroup `
    --workspace-name $LogAnalyticsWorkspaceName `
    --location $Location `
    --only-show-errors 1>$null

  $logAnalyticsRetention = Resolve-LogAnalyticsRetentionTarget `
    -ResourceGroupName $ResourceGroup `
    -WorkspaceName $LogAnalyticsWorkspaceName `
    -RequestedRetentionInDays $LogAnalyticsRetentionInDays
  Write-Host ("Configuring Log Analytics retention: requested={0} effective={1} current={2} sku={3}" -f `
      $LogAnalyticsRetentionInDays, `
      $logAnalyticsRetention.EffectiveRetentionInDays, `
      $(if ($null -ne $logAnalyticsRetention.CurrentRetentionInDays) { $logAnalyticsRetention.CurrentRetentionInDays } else { "<unknown>" }), `
      $(if (-not [string]::IsNullOrWhiteSpace($logAnalyticsRetention.WorkspaceSkuName)) { $logAnalyticsRetention.WorkspaceSkuName } else { "<unknown>" })) -ForegroundColor Cyan
  az monitor log-analytics workspace update `
    --resource-group $ResourceGroup `
    --workspace-name $LogAnalyticsWorkspaceName `
    --retention-time $logAnalyticsRetention.EffectiveRetentionInDays `
    --only-show-errors 1>$null
}

$lawCustomerId = ""
$lawSharedKey = ""
if ($doLogAnalytics) {
  $lawCustomerId = az monitor log-analytics workspace show `
    --resource-group $ResourceGroup `
    --workspace-name $LogAnalyticsWorkspaceName `
    --query customerId -o tsv

  $lawSharedKey = az monitor log-analytics workspace get-shared-keys `
    --resource-group $ResourceGroup `
    --workspace-name $LogAnalyticsWorkspaceName `
    --query primarySharedKey -o tsv
}

$doContainerAppsEnv = Get-YesNo "Ensure Container Apps environment exists: ${ContainerAppsEnvironmentName}?" $true
if ($doContainerAppsEnv) {
  if (-not $lawCustomerId -or -not $lawSharedKey) {
    throw "Log Analytics workspace details missing; cannot create Container Apps environment. Enable Log Analytics or provide workspace info."
  }
  Write-Host "Ensuring Container Apps environment exists: $ContainerAppsEnvironmentName"
  az containerapp env create `
    --name $ContainerAppsEnvironmentName `
    --resource-group $ResourceGroup `
    --location $Location `
    --logs-workspace-id $lawCustomerId `
    --logs-workspace-key $lawSharedKey `
    --only-show-errors 1>$null
}

if ($AksClusterName) {
  $doAksServiceAccounts = Get-YesNo "Ensure AKS service accounts in $KubernetesNamespace?" $true
  if (-not $doAksServiceAccounts) {
    $AksClusterName = ""
  }
}

if ($AksClusterName) {
  Write-Host "Ensuring Kubernetes service account exists: $ServiceAccountName (namespace: $KubernetesNamespace)"
  az aks get-credentials --resource-group $ResourceGroup --name $AksClusterName --overwrite-existing --only-show-errors 1>$null
  kubectl get namespace $KubernetesNamespace 1>$null 2>$null
  if ($LASTEXITCODE -ne 0) {
    kubectl create namespace $KubernetesNamespace | Out-Null
  }
  $serviceAccountYaml = @"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $ServiceAccountName
  namespace: $KubernetesNamespace
"@
  $serviceAccountYaml | kubectl apply -f - | Out-Null

  $deployDir = Join-Path $repoRoot "deploy"
  if (Test-Path $deployDir) {
    $jobServiceAccounts = @()
    Get-ChildItem -Path $deployDir -Filter "job_*.yaml" | ForEach-Object {
      $nameLine = Select-String -Path $_.FullName -Pattern '^name:\s*(.+)$' | Select-Object -First 1
      if ($nameLine) {
        $jobName = $nameLine.Matches[0].Groups[1].Value.Trim()
        if ($jobName) {
          $jobServiceAccounts += "job-$jobName"
        }
      }
    }
    $jobServiceAccounts = $jobServiceAccounts | Sort-Object -Unique
    if ($jobServiceAccounts.Count -gt 0) {
      $namespaces = @($KubernetesNamespace)
      if ($KubernetesNamespace -ne "k8se-apps") {
        $namespaces += "k8se-apps"
      }
      $namespaces = $namespaces | Sort-Object -Unique
      foreach ($ns in $namespaces) {
        Write-Host "Ensuring job service accounts exist in $ns..."
        foreach ($saName in $jobServiceAccounts) {
          $jobSaYaml = @"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $saName
  namespace: $ns
"@
          $jobSaYaml | kubectl apply -f - | Out-Null
        }
      }
    }
  }
}

$storageConnectionString = ""
if ($EmitSecrets) {
  $storageConnectionString = az storage account show-connection-string `
    --name $StorageAccountName `
    --resource-group $ResourceGroup `
    --query connectionString -o tsv
}

$doManagedIdentity = Get-YesNo "Ensure user-assigned managed identity for ACR pull ($AcrPullIdentityName)?" $true
if ($doManagedIdentity) {
  Write-Host "Ensuring user-assigned managed identity exists (for ACR pull): $AcrPullIdentityName"
  $acrPullIdentity = $null
  try {
    $acrPullIdentity = az identity show --name $AcrPullIdentityName --resource-group $ResourceGroup --only-show-errors -o json 2>$null | ConvertFrom-Json
  }
  catch {
    $acrPullIdentity = $null
  }

  if ($null -eq $acrPullIdentity) {
    $acrPullIdentity = az identity create --name $AcrPullIdentityName --resource-group $ResourceGroup --location $Location --only-show-errors -o json | ConvertFrom-Json
  }

  $acrPullIdentityId = $acrPullIdentity.id
  $acrPullIdentityClientId = $acrPullIdentity.clientId
  $acrPullIdentityPrincipalId = $acrPullIdentity.principalId

  if (-not $acrPullIdentityId -or -not $acrPullIdentityPrincipalId) {
    throw "Failed to resolve AcrPull identity details for '$AcrPullIdentityName'."
  }

  if ($doAcr) {
    $doAcrPullRole = Get-YesNo "Assign AcrPull role to identity on ACR?" $true
    if ($doAcrPullRole) {
      Write-Host "Ensuring AcrPull role assignment exists for identity on ACR..."
      $acrPullExisting = "0"
      try {
        $acrPullExisting = az role assignment list `
          --assignee-object-id $acrPullIdentityPrincipalId `
          --scope $acrId `
          --query "[?roleDefinitionName=='AcrPull'] | length(@)" -o tsv --only-show-errors 2>$null
        if (-not $acrPullExisting) { $acrPullExisting = "0" }
      }
      catch {
        $acrPullExisting = "0"
      }

      if ([int]$acrPullExisting -eq 0) {
        az role assignment create `
          --assignee-object-id $acrPullIdentityPrincipalId `
          --assignee-principal-type ServicePrincipal `
          --role "AcrPull" `
          --scope $acrId `
          --only-show-errors 1>$null
        Write-Host "  AcrPull granted to $AcrPullIdentityName ($acrPullIdentityPrincipalId)"
      }
      else {
        Write-Host "  AcrPull already present for $AcrPullIdentityName ($acrPullIdentityPrincipalId)"
      }
    }
  }
  else {
    Write-Host "Skipping AcrPull role assignment (ACR not provisioned)."
  }
}

if ($doManagedIdentity) {
  if ($storageAccountId -and $acrPullIdentityPrincipalId) {
    Write-Host ""
    Write-Host "Ensuring ACR pull identity can access storage data (Storage Blob Data Contributor)..."
    $storageDataAcrExisting = "0"
    try {
      $storageDataAcrExisting = az role assignment list `
        --assignee-object-id $acrPullIdentityPrincipalId `
        --scope $storageAccountId `
        --query "[?roleDefinitionName=='Storage Blob Data Contributor'] | length(@)" -o tsv --only-show-errors 2>$null
      if (-not $storageDataAcrExisting) { $storageDataAcrExisting = "0" }
    }
    catch {
      $storageDataAcrExisting = "0"
    }

    if ([int]$storageDataAcrExisting -eq 0) {
      az role assignment create `
        --assignee-object-id $acrPullIdentityPrincipalId `
        --assignee-principal-type ServicePrincipal `
        --role "Storage Blob Data Contributor" `
        --scope $storageAccountId `
        --only-show-errors 1>$null
      Write-Host "  Storage Blob Data Contributor granted to $AcrPullIdentityName on $StorageAccountName."
    }
    else {
      Write-Host "  Storage Blob Data Contributor already assigned to $AcrPullIdentityName on $StorageAccountName."
    }
  }
}

if ($AzureClientId -and $doManagedIdentity) {
  Write-Host ""
  Write-Host "Ensuring GitHub Actions principal can assign the ACR pull identity..."
  if (-not $githubSpObjectId) {
    try {
      $githubSpObjectId = az ad sp show --id $AzureClientId --query id -o tsv --only-show-errors 2>$null
    }
    catch {
      $githubSpObjectId = $null
    }
  }

  if ($githubSpObjectId) {
    $miOperatorExisting = "0"
    try {
      $miOperatorExisting = az role assignment list `
        --assignee-object-id $githubSpObjectId `
        --scope $acrPullIdentityId `
        --query "[?roleDefinitionName=='Managed Identity Operator'] | length(@)" -o tsv --only-show-errors 2>$null
      if (-not $miOperatorExisting) { $miOperatorExisting = "0" }
    }
    catch {
      $miOperatorExisting = "0"
    }

    if ([int]$miOperatorExisting -eq 0) {
      az role assignment create `
        --assignee-object-id $githubSpObjectId `
        --assignee-principal-type ServicePrincipal `
        --role "Managed Identity Operator" `
        --scope $acrPullIdentityId `
        --only-show-errors 1>$null
      Write-Host "  Managed Identity Operator granted to $AzureClientId on $AcrPullIdentityName."
    }
    else {
      Write-Host "  Managed Identity Operator already assigned to $AzureClientId on $AcrPullIdentityName."
    }
  }
  else {
    Write-Warning "Could not resolve service principal for AzureClientId '$AzureClientId'. Skipping Managed Identity Operator grant."
  }
}

Write-Host ""
Write-Host "ACR Pull identity resource ID:"
if ($doManagedIdentity) {
  Write-Host "  $acrPullIdentityId"
  Write-Host "Set ACR_PULL_IDENTITY_NAME to '$AcrPullIdentityName' (workflow default) or supply the resource ID as ACR_PULL_IDENTITY_RESOURCE_ID for deployments."
}
else {
  Write-Host "  <not_created>"
}

function Ensure-AcrPullRoleAssignment {
  param(
    [Parameter(Mandatory = $true)][string]$PrincipalId,
    [Parameter(Mandatory = $true)][string]$Scope
  )

  if (-not $PrincipalId -or $PrincipalId -eq "None") {
    return $false
  }

  $existing = "0"
  try {
    $existing = az role assignment list `
      --assignee $PrincipalId `
      --scope $Scope `
      --query "[?roleDefinitionName=='AcrPull'] | length(@)" -o tsv --only-show-errors 2>$null
    if (-not $existing) { $existing = "0" }
  }
  catch {
    $existing = "0"
  }

  if ([int]$existing -gt 0) {
    return $false
  }

  az role assignment create --assignee $PrincipalId --role "AcrPull" --scope $Scope --only-show-errors 1>$null
  return $true
}

$acrPullAssignmentsCreated = 0
$acrPullAssignmentsSkipped = 0
$jobStartAssignmentsCreated = 0
$jobStartAssignmentsSkipped = 0

if ($GrantAcrPullToAcaResources) {
  if (-not $doAcr -or -not $doManagedIdentity) {
    Write-Warning "Skipping AcrPull grants to existing apps/jobs (ACR or managed identity not provisioned)."
  }
  else {
    Write-Host ""
    Write-Host "Granting AcrPull on ACR to existing Container Apps + Jobs (best-effort)..."
    Write-Host "  ACR: $AcrName"
    Write-Host "  Scope: $acrId"

    $appNames = @()
    $jobNames = @()

    try {
      $appNames = @(az containerapp list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors)
    }
    catch {
      Write-Warning "Could not list Container Apps in RG '$ResourceGroup'."
    }

    foreach ($name in $appNames) {
      if (-not $name) { continue }
      try {
        $principalId = az containerapp show --name $name --resource-group $ResourceGroup --query identity.principalId -o tsv --only-show-errors
        if (-not [string]::IsNullOrWhiteSpace($principalId)) {
          if (Ensure-AcrPullRoleAssignment -PrincipalId $principalId -Scope $acrId) {
            $acrPullAssignmentsCreated += 1
            Write-Host "  AcrPull granted (app): $name"
          }
          else {
            $acrPullAssignmentsSkipped += 1
          }
        }
      }
      catch {
        Write-Warning "Failed to grant AcrPull (app '$name'): $($_.Exception.Message)"
      }
    }

    try {
      $jobNames = @(az containerapp job list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors)
    }
    catch {
      Write-Warning "Could not list Container App Jobs in RG '$ResourceGroup'."
    }

    foreach ($name in $jobNames) {
      if (-not $name) { continue }
      try {
        $principalId = az containerapp job show --name $name --resource-group $ResourceGroup --query identity.principalId -o tsv --only-show-errors
        if (-not [string]::IsNullOrWhiteSpace($principalId)) {
          if (Ensure-AcrPullRoleAssignment -PrincipalId $principalId -Scope $acrId) {
            $acrPullAssignmentsCreated += 1
            Write-Host "  AcrPull granted (job): $name"
          }
          else {
            $acrPullAssignmentsSkipped += 1
          }
        }
      }
      catch {
        Write-Warning "Failed to grant AcrPull (job '$name'): $($_.Exception.Message)"
      }
    }

    Write-Host "AcrPull role assignment summary: created=$acrPullAssignmentsCreated skipped=$acrPullAssignmentsSkipped"
  }
}
else {
  Write-Host ""
  Write-Host "NOTE: This repo's Container Apps/Jobs are configured to pull ACR images via managed identity."
  Write-Host "To grant pull permissions, re-run this script after deployment with -GrantAcrPullToAcaResources (requires RBAC permissions to create role assignments)."
}

if ($GrantJobStartToAcaResources) {
  if (-not $doManagedIdentity) {
    Write-Warning "Skipping job-start grants (managed identity not provisioned)."
  }
  else {
    Write-Host ""
    Write-Host "Granting Container App job/container-app start permissions to the ACR pull identity (best-effort)..."
    Write-Host "  Assignee: $AcrPullIdentityName ($acrPullIdentityPrincipalId)"
    Write-Host "  Scope: Resource group $ResourceGroup"
    Write-Host "  Role: Contributor (resource group scope, covers jobs/start and containerApps/start)"

    $rgScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup"
    $existing = "0"
    try {
      $existing = az role assignment list `
        --assignee-object-id $acrPullIdentityPrincipalId `
        --scope $rgScope `
        --query "[?roleDefinitionName=='Contributor'] | length(@)" -o tsv --only-show-errors 2>$null
      if (-not $existing) { $existing = "0" }
    }
    catch {
      $existing = "0"
    }

    if ([int]$existing -eq 0) {
      try {
        az role assignment create `
          --assignee-object-id $acrPullIdentityPrincipalId `
          --assignee-principal-type ServicePrincipal `
          --role "Contributor" `
          --scope $rgScope `
          --only-show-errors 1>$null
        $jobStartAssignmentsCreated += 1
        Write-Host "  Job start role granted at resource group scope." -ForegroundColor Cyan
      }
      catch {
        Write-Warning "Failed to grant job start role at RG scope: $($_.Exception.Message)"
      }
    }
    else {
      $jobStartAssignmentsSkipped += 1
    }

    Write-Host "Job start role assignment summary: created=$jobStartAssignmentsCreated skipped=$jobStartAssignmentsSkipped"
  }
}
else {
  Write-Host ""
  Write-Host "NOTE: Jobs may trigger downstream jobs and wake API container apps via ARM."
  Write-Host "To grant the required permissions, re-run this script after deployment with -GrantJobStartToAcaResources."
}

if ($ConfigureAcrPullIdentityOnAcaResources) {
  if (-not $doManagedIdentity -or -not $acrPullIdentityId) {
    Write-Warning "Skipping ACR pull identity configuration (managed identity not provisioned)."
  }
  elseif (-not $doAcr -or -not $acrLoginServer) {
    Write-Warning "Skipping ACR pull identity configuration (ACR not provisioned)."
  }
  else {
    Write-Host ""
    Write-Host "Configuring existing Container Apps/Jobs to pull from ACR via managed identity (best-effort)..." -ForegroundColor Cyan
    Write-Host "  ACR: $AcrName ($acrLoginServer)"
    Write-Host "  Identity: $AcrPullIdentityName ($acrPullIdentityId)"

    $appsConfigured = 0
    $appsFailed = 0
    $jobsConfigured = 0
    $jobsFailed = 0

    $appNames = @()
    try {
      $appNames = @(az containerapp list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors 2>$null)
    }
    catch {
      $appNames = @()
    }

    foreach ($name in $appNames) {
      if (-not $name) { continue }
      try {
        $out = az containerapp identity assign --name $name --resource-group $ResourceGroup --user-assigned $acrPullIdentityId --only-show-errors 2>&1
        if ($LASTEXITCODE -ne 0) { throw $out }

        $out = az containerapp registry set --name $name --resource-group $ResourceGroup --server $acrLoginServer --identity $acrPullIdentityId --only-show-errors 2>&1
        if ($LASTEXITCODE -ne 0) { throw $out }

        $appsConfigured += 1
        Write-Host "  Configured app: $name"
      }
      catch {
        $appsFailed += 1
        Write-Warning "Failed to configure app '$name': $($_.Exception.Message)"
        Write-Warning "  If this mentions ACR UNAUTHORIZED, re-run the deploy workflow (it bootstraps to a public image to break ACR auth deadlocks)."
      }
    }

    $jobNames = @()
    try {
      $jobNames = @(az containerapp job list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors 2>$null)
    }
    catch {
      $jobNames = @()
    }

    foreach ($name in $jobNames) {
      if (-not $name) { continue }
      try {
        $out = az containerapp job identity assign --name $name --resource-group $ResourceGroup --user-assigned $acrPullIdentityId --only-show-errors 2>&1
        if ($LASTEXITCODE -ne 0) { throw $out }

        $out = az containerapp job registry set --name $name --resource-group $ResourceGroup --server $acrLoginServer --identity $acrPullIdentityId --only-show-errors 2>&1
        if ($LASTEXITCODE -ne 0) { throw $out }

        $jobsConfigured += 1
        Write-Host "  Configured job: $name"
      }
      catch {
        $jobsFailed += 1
        Write-Warning "Failed to configure job '$name': $($_.Exception.Message)"
      }
    }

    Write-Host "ACR pull identity configuration summary: appsConfigured=$appsConfigured appsFailed=$appsFailed jobsConfigured=$jobsConfigured jobsFailed=$jobsFailed"
  }
}

if ($CorrectApiStorageAuthMode) {
  Write-Host ""
  Write-Host "Correcting API storage auth mode..." -ForegroundColor Cyan
  Set-ApiStorageAuthMode `
    -ResourceGroupName $ResourceGroup `
    -ContainerAppName $ApiContainerAppName `
    -AuthMode $ApiStorageAuthMode `
    -StorageAccount $StorageAccountName

  Write-Host ""
  Write-Host "Storage auth mode correction complete for '$ApiContainerAppName'." -ForegroundColor Green
  Write-Host "Suggested verification:"
  Write-Host "  az containerapp logs show --resource-group $ResourceGroup --name $ApiContainerAppName --tail 300 | rg 'Delta storage auth resolved|AuthenticationFailed|MAC signature'"
}

$outputs = [ordered]@{
  subscriptionId                          = $SubscriptionId
  location                                = $Location
  resourceGroup                           = $ResourceGroup
  storageAccountName                      = $StorageAccountName
  storageConnectionString                 = if ($EmitSecrets) { $storageConnectionString } else { "<redacted>" }
  storageContainers                       = $StorageContainers
  acrName                                 = $AcrName
  acrId                                   = $acrId
  acrLoginServer                          = $acrLoginServer
  acrAdminEnabled                         = [bool]$EnableAcrAdmin
  acrPullAuthMode                         = "managedIdentity"
  acrPullUserAssignedIdentityName         = $AcrPullIdentityName
  acrPullUserAssignedIdentityId           = $acrPullIdentityId
  acrPullUserAssignedIdentityResourceId   = $acrPullIdentityId
  acrPullUserAssignedIdentityClientId     = $acrPullIdentityClientId
  acrPullUserAssignedIdentityPrincipalId  = $acrPullIdentityPrincipalId
  acrPullIdentityOperatorAssigneeObjectId = $githubSpObjectId
  acrPullAssignmentsCreated               = $acrPullAssignmentsCreated
  acrPullAssignmentsSkipped               = $acrPullAssignmentsSkipped
  jobStartAssignmentsCreated              = $jobStartAssignmentsCreated
  jobStartAssignmentsSkipped              = $jobStartAssignmentsSkipped
  apiStorageAuthCorrectionRequested       = [bool]$CorrectApiStorageAuthMode
  apiStorageAuthMode                      = $ApiStorageAuthMode
  apiContainerAppName                     = $ApiContainerAppName
  logAnalyticsWorkspaceName               = $LogAnalyticsWorkspaceName
  logAnalyticsCustomerId                  = $lawCustomerId
  containerAppsEnvironmentName            = $ContainerAppsEnvironmentName
  kubernetesServiceAccountName            = $ServiceAccountName
  kubernetesNamespace                     = $KubernetesNamespace
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 4
