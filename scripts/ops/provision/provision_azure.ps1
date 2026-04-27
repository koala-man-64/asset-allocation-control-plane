param(
  [string]$SubscriptionId = "",

  [string]$Location = "eastus",
  [string]$ResourceGroup = "AssetAllocationRG",

  [string]$StorageAccountName = "assetallocstorage001",

  [string[]]$StorageContainers = @(),
  [string]$AcrName = "assetallocationacr",
  # User-assigned managed identity used by Container Apps/Jobs to pull from ACR on first create.
  [string]$AcrPullIdentityName = "asset-allocation-acr-pull-mi",
  [string]$ApiRuntimeIdentityName = "asset-allocation-api-runtime-mi",
  [string]$JobControlIdentityName = "asset-allocation-job-control-mi",
  [switch]$EnableAcrAdmin,
  [switch]$EnableAcrPrivateLink,
  [switch]$DisableAcrPublicNetworkAccess,
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
  [string]$VnetContainerAppsEnvironmentName = "asset-allocation-env-vnet",
  [string]$VnetName = "asset-allocation-vnet-prod",
  [string]$VnetAddressSpace = "10.64.0.0/24",
  [string]$AcaInfrastructureSubnetName = "aca-infra-snet",
  [string]$AcaInfrastructureSubnetPrefix = "10.64.0.0/26",
  [string]$PrivateEndpointSubnetName = "private-endpoints-snet",
  [string]$PrivateEndpointSubnetPrefix = "10.64.0.64/27",
  [string]$ReservedSubnetPrefix = "10.64.0.96/27",
  [string]$NatGatewayName = "asset-allocation-nat-prod",
  [string]$NatPublicIpName = "asset-allocation-egress-ip-prod",
  [string]$NetworkSmokeJobName = "asset-allocation-network-smoke",
  [switch]$SkipParallelPrivateRuntime,
  [switch]$CorrectApiStorageAuthMode,
  [ValidateSet("ManagedIdentity", "ConnectionString")]
  [string]$ApiStorageAuthMode = "ManagedIdentity",
  [string]$ApiContainerAppName = "",
  [string]$VnetApiAppName = "asset-allocation-api-vnet",
  [string]$VnetUiAppName = "asset-allocation-ui-vnet",
  [string]$UiPublicHostname = "",
  [switch]$DisablePublicDataPlaneAccess,
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

function Get-GitHubRepositorySlug {
  $originUrl = git -C $repoRoot remote get-url origin 2>$null
  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($originUrl)) {
    return ""
  }

  $trimmed = $originUrl.Trim()
  if ($trimmed -match "github\.com[:/](?<slug>[^/]+/[^/.]+?)(?:\.git)?$") {
    return $Matches["slug"]
  }

  return ""
}

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

function Get-AzTsvOrEmpty {
  param([Parameter(Mandatory = $true)][string[]]$Arguments)

  try {
    $output = & az @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) {
      return ""
    }
    return (($output | Out-String).Trim())
  }
  catch {
    return ""
  }
}

function Ensure-UserAssignedIdentity {
  param(
    [Parameter(Mandatory = $true)][string]$IdentityName,
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$LocationName
  )

  $identity = $null
  try {
    $identity = az identity show --name $IdentityName --resource-group $ResourceGroupName --only-show-errors -o json 2>$null | ConvertFrom-Json
  }
  catch {
    $identity = $null
  }

  if ($null -eq $identity) {
    $identity = az identity create --name $IdentityName --resource-group $ResourceGroupName --location $LocationName --only-show-errors -o json | ConvertFrom-Json
  }

  if (-not $identity.id -or -not $identity.principalId) {
    throw "Failed to resolve managed identity details for '$IdentityName'."
  }

  return $identity
}

function Ensure-RoleAssignment {
  param(
    [Parameter(Mandatory = $true)][string]$PrincipalId,
    [Parameter(Mandatory = $true)][string]$RoleName,
    [Parameter(Mandatory = $true)][string]$Scope,
    [string]$PrincipalType = "ServicePrincipal"
  )

  if ([string]::IsNullOrWhiteSpace($PrincipalId) -or [string]::IsNullOrWhiteSpace($Scope)) {
    return $false
  }

  $existing = "0"
  try {
    $existing = az role assignment list `
      --assignee-object-id $PrincipalId `
      --scope $Scope `
      --query "[?roleDefinitionName=='$RoleName'] | length(@)" -o tsv --only-show-errors 2>$null
    if (-not $existing) { $existing = "0" }
  }
  catch {
    $existing = "0"
  }

  if ([int]$existing -gt 0) {
    return $false
  }

  az role assignment create `
    --assignee-object-id $PrincipalId `
    --assignee-principal-type $PrincipalType `
    --role $RoleName `
    --scope $Scope `
    --only-show-errors 1>$null
  return $true
}

function Ensure-AcaOperatorRoleDefinition {
  param(
    [Parameter(Mandatory = $true)][string]$RoleName,
    [Parameter(Mandatory = $true)][string]$Scope
  )

  $definition = [ordered]@{
    Name             = $RoleName
    IsCustom         = $true
    Description      = "Start, stop, and read Azure Container Apps resources used by the asset-allocation control plane."
    Actions          = @(
      "Microsoft.App/containerApps/read",
      "Microsoft.App/containerApps/start/action",
      "Microsoft.App/containerApps/stop/action",
      "Microsoft.App/jobs/read",
      "Microsoft.App/jobs/start/action",
      "Microsoft.App/jobs/stop/action",
      "Microsoft.App/jobs/suspend/action",
      "Microsoft.App/jobs/resume/action",
      "Microsoft.App/jobs/executions/read",
      "Microsoft.OperationalInsights/workspaces/query/read"
    )
    NotActions       = @()
    DataActions      = @()
    NotDataActions   = @()
    AssignableScopes = @($Scope)
  }

  $tempPath = Join-Path $env:TEMP ("aca-operator-role-{0}.json" -f ([Guid]::NewGuid().ToString("N")))
  try {
    $definition | ConvertTo-Json -Depth 8 | Set-Content -Path $tempPath -Encoding utf8
    $existingRole = Get-AzTsvOrEmpty -Arguments @(
      "role", "definition", "list",
      "--name", $RoleName,
      "--query", "[0].name",
      "-o", "tsv",
      "--only-show-errors"
    )

    if ([string]::IsNullOrWhiteSpace($existingRole)) {
      az role definition create --role-definition $tempPath --only-show-errors 1>$null
    }
    else {
      az role definition update --role-definition $tempPath --only-show-errors 1>$null
    }
  }
  finally {
    if (Test-Path $tempPath) {
      Remove-Item -LiteralPath $tempPath -Force
    }
  }

  return $RoleName
}

function Ensure-PrivateDnsZoneLink {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$ZoneName,
    [Parameter(Mandatory = $true)][string]$VnetId,
    [Parameter(Mandatory = $true)][string]$LinkName
  )

  az network private-dns zone create `
    --resource-group $ResourceGroupName `
    --name $ZoneName `
    --only-show-errors 1>$null

  $existingLink = Get-AzTsvOrEmpty -Arguments @(
    "network", "private-dns", "link", "vnet", "show",
    "--resource-group", $ResourceGroupName,
    "--zone-name", $ZoneName,
    "--name", $LinkName,
    "--query", "name",
    "-o", "tsv",
    "--only-show-errors"
  )

  if ([string]::IsNullOrWhiteSpace($existingLink)) {
    az network private-dns link vnet create `
      --resource-group $ResourceGroupName `
      --zone-name $ZoneName `
      --name $LinkName `
      --virtual-network $VnetId `
      --registration-enabled false `
      --only-show-errors 1>$null
  }
}

function Ensure-PrivateEndpointConnection {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$LocationName,
    [Parameter(Mandatory = $true)][string]$EndpointName,
    [Parameter(Mandatory = $true)][string]$ConnectionName,
    [Parameter(Mandatory = $true)][string]$PrivateConnectionResourceId,
    [Parameter(Mandatory = $true)][string]$GroupId,
    [Parameter(Mandatory = $true)][string]$VnetNameValue,
    [Parameter(Mandatory = $true)][string]$SubnetNameValue,
    [Parameter(Mandatory = $true)][string]$PrivateDnsZoneName,
    [string]$DnsZoneGroupName = "default"
  )

  $existingEndpoint = Get-AzTsvOrEmpty -Arguments @(
    "network", "private-endpoint", "show",
    "--resource-group", $ResourceGroupName,
    "--name", $EndpointName,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )

  if ([string]::IsNullOrWhiteSpace($existingEndpoint)) {
    az network private-endpoint create `
      --resource-group $ResourceGroupName `
      --location $LocationName `
      --name $EndpointName `
      --connection-name $ConnectionName `
      --private-connection-resource-id $PrivateConnectionResourceId `
      --group-id $GroupId `
      --vnet-name $VnetNameValue `
      --subnet $SubnetNameValue `
      --only-show-errors 1>$null
  }

  $existingZoneGroup = Get-AzTsvOrEmpty -Arguments @(
    "network", "private-endpoint", "dns-zone-group", "show",
    "--resource-group", $ResourceGroupName,
    "--endpoint-name", $EndpointName,
    "--name", $DnsZoneGroupName,
    "--query", "name",
    "-o", "tsv",
    "--only-show-errors"
  )

  if ([string]::IsNullOrWhiteSpace($existingZoneGroup)) {
    az network private-endpoint dns-zone-group create `
      --resource-group $ResourceGroupName `
      --endpoint-name $EndpointName `
      --name $DnsZoneGroupName `
      --private-dns-zone $PrivateDnsZoneName `
      --zone-name $PrivateDnsZoneName `
      --only-show-errors 1>$null
  }
}

function Ensure-NetworkSmokeJob {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$EnvironmentName,
    [Parameter(Mandatory = $true)][string]$JobName,
    [Parameter(Mandatory = $true)][string]$ApiInternalAppName,
    [Parameter(Mandatory = $true)][string]$StorageAccount,
    [string]$AcrNameValue = "",
    [string]$PostgresServer = ""
  )

  $jobScript = @'
set -eu
wget -qO- "${API_BASE_URL}/healthz" > /dev/null
nslookup "${STORAGE_ACCOUNT_NAME}.blob.core.windows.net" > /dev/null
nslookup "${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net" > /dev/null
nc -z "${STORAGE_ACCOUNT_NAME}.blob.core.windows.net" 443
nc -z "${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net" 443
if [ -n "${ACR_NAME}" ]; then
  nslookup "${ACR_NAME}.azurecr.io" > /dev/null
  nc -z "${ACR_NAME}.azurecr.io" 443
fi
if [ -n "${POSTGRES_SERVER_NAME}" ]; then
  nslookup "${POSTGRES_SERVER_NAME}.postgres.database.azure.com" > /dev/null
  nc -z "${POSTGRES_SERVER_NAME}.postgres.database.azure.com" 5432
fi
'@

  $jobEnvVars = @(
    "API_BASE_URL=http://$ApiInternalAppName",
    "STORAGE_ACCOUNT_NAME=$StorageAccount",
    "ACR_NAME=$AcrNameValue",
    "POSTGRES_SERVER_NAME=$PostgresServer"
  )

  $existingJob = Get-AzTsvOrEmpty -Arguments @(
    "containerapp", "job", "show",
    "--resource-group", $ResourceGroupName,
    "--name", $JobName,
    "--query", "name",
    "-o", "tsv",
    "--only-show-errors"
  )

  if ([string]::IsNullOrWhiteSpace($existingJob)) {
    $createArgs = @(
      "containerapp", "job", "create",
      "--resource-group", $ResourceGroupName,
      "--name", $JobName,
      "--environment", $EnvironmentName,
      "--trigger-type", "Manual",
      "--replica-timeout", "600",
      "--replica-retry-limit", "0",
      "--replica-completion-count", "1",
      "--parallelism", "1",
      "--image", "busybox:1.36",
      "--container-name", "network-smoke",
      "--command", "/bin/sh",
      "--args", "-c", $jobScript,
      "--cpu", "0.25",
      "--memory", "0.5Gi",
      "--workload-profile-name", "Consumption",
      "--env-vars"
    ) + $jobEnvVars + @("--only-show-errors")
    & az @createArgs 1>$null
  }
  else {
    $updateArgs = @(
      "containerapp", "job", "update",
      "--resource-group", $ResourceGroupName,
      "--name", $JobName,
      "--image", "busybox:1.36",
      "--command", "/bin/sh",
      "--args", "-c", $jobScript,
      "--cpu", "0.25",
      "--memory", "0.5Gi",
      "--replace-env-vars"
    ) + $jobEnvVars + @("--only-show-errors")
    & az @updateArgs 1>$null
  }
}

function Ensure-ParallelPrivateRuntime {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$LocationName,
    [Parameter(Mandatory = $true)][string]$VnetNameValue,
    [Parameter(Mandatory = $true)][string]$VnetAddressSpaceValue,
    [Parameter(Mandatory = $true)][string]$AcaSubnetName,
    [Parameter(Mandatory = $true)][string]$AcaSubnetPrefix,
    [Parameter(Mandatory = $true)][string]$PeSubnetName,
    [Parameter(Mandatory = $true)][string]$PeSubnetPrefix,
    [string]$ReservedSubnetPrefixValue = "",
    [Parameter(Mandatory = $true)][string]$NatGatewayNameValue,
    [Parameter(Mandatory = $true)][string]$NatPublicIpNameValue,
    [Parameter(Mandatory = $true)][string]$EnvironmentName,
    [Parameter(Mandatory = $true)][string]$NetworkSmokeJobNameValue,
    [Parameter(Mandatory = $true)][string]$ApiInternalAppName,
    [Parameter(Mandatory = $true)][string]$WorkspaceCustomerId,
    [Parameter(Mandatory = $true)][string]$WorkspaceSharedKey,
    [Parameter(Mandatory = $true)][string]$StorageAccount,
    [string]$AcrNameValue = "",
    [string]$PostgresServer = "",
    [switch]$EnableAcrPrivateLink,
    [switch]$DisableAcrPublicNetworkAccess,
    [switch]$DisablePublicDataPlane
  )

  $existingVnet = Get-AzTsvOrEmpty -Arguments @(
    "network", "vnet", "show",
    "--resource-group", $ResourceGroupName,
    "--name", $VnetNameValue,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ([string]::IsNullOrWhiteSpace($existingVnet)) {
    az network vnet create `
      --resource-group $ResourceGroupName `
      --location $LocationName `
      --name $VnetNameValue `
      --address-prefixes $VnetAddressSpaceValue `
      --only-show-errors 1>$null
  }

  az network public-ip create `
    --resource-group $ResourceGroupName `
    --location $LocationName `
    --name $NatPublicIpNameValue `
    --sku Standard `
    --allocation-method Static `
    --only-show-errors 1>$null

  az network nat gateway create `
    --resource-group $ResourceGroupName `
    --location $LocationName `
    --name $NatGatewayNameValue `
    --public-ip-addresses $NatPublicIpNameValue `
    --only-show-errors 1>$null

  $acaSubnetId = Get-AzTsvOrEmpty -Arguments @(
    "network", "vnet", "subnet", "show",
    "--resource-group", $ResourceGroupName,
    "--vnet-name", $VnetNameValue,
    "--name", $AcaSubnetName,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ([string]::IsNullOrWhiteSpace($acaSubnetId)) {
    az network vnet subnet create `
      --resource-group $ResourceGroupName `
      --vnet-name $VnetNameValue `
      --name $AcaSubnetName `
      --address-prefixes $AcaSubnetPrefix `
      --delegations Microsoft.App/environments `
      --only-show-errors 1>$null
  }
  az network vnet subnet update `
    --resource-group $ResourceGroupName `
    --vnet-name $VnetNameValue `
    --name $AcaSubnetName `
    --address-prefixes $AcaSubnetPrefix `
    --delegations Microsoft.App/environments `
    --nat-gateway $NatGatewayNameValue `
    --only-show-errors 1>$null

  $peSubnetId = Get-AzTsvOrEmpty -Arguments @(
    "network", "vnet", "subnet", "show",
    "--resource-group", $ResourceGroupName,
    "--vnet-name", $VnetNameValue,
    "--name", $PeSubnetName,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ([string]::IsNullOrWhiteSpace($peSubnetId)) {
    az network vnet subnet create `
      --resource-group $ResourceGroupName `
      --vnet-name $VnetNameValue `
      --name $PeSubnetName `
      --address-prefixes $PeSubnetPrefix `
      --only-show-errors 1>$null
  }
  az network vnet subnet update `
    --resource-group $ResourceGroupName `
    --vnet-name $VnetNameValue `
    --name $PeSubnetName `
    --address-prefixes $PeSubnetPrefix `
    --private-endpoint-network-policies Disabled `
    --only-show-errors 1>$null

  $vnetId = Get-AzTsvOrEmpty -Arguments @(
    "network", "vnet", "show",
    "--resource-group", $ResourceGroupName,
    "--name", $VnetNameValue,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )
  $acaSubnetId = Get-AzTsvOrEmpty -Arguments @(
    "network", "vnet", "subnet", "show",
    "--resource-group", $ResourceGroupName,
    "--vnet-name", $VnetNameValue,
    "--name", $AcaSubnetName,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )

  $existingEnvironment = Get-AzTsvOrEmpty -Arguments @(
    "containerapp", "env", "show",
    "--resource-group", $ResourceGroupName,
    "--name", $EnvironmentName,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ([string]::IsNullOrWhiteSpace($existingEnvironment)) {
    az containerapp env create `
      --name $EnvironmentName `
      --resource-group $ResourceGroupName `
      --location $LocationName `
      --logs-workspace-id $WorkspaceCustomerId `
      --logs-workspace-key $WorkspaceSharedKey `
      --infrastructure-subnet-resource-id $acaSubnetId `
      --only-show-errors 1>$null
  }

  Ensure-PrivateDnsZoneLink -ResourceGroupName $ResourceGroupName -ZoneName "privatelink.blob.core.windows.net" -VnetId $vnetId -LinkName "$VnetNameValue-blob"
  Ensure-PrivateDnsZoneLink -ResourceGroupName $ResourceGroupName -ZoneName "privatelink.dfs.core.windows.net" -VnetId $vnetId -LinkName "$VnetNameValue-dfs"
  Ensure-PrivateDnsZoneLink -ResourceGroupName $ResourceGroupName -ZoneName "privatelink.postgres.database.azure.com" -VnetId $vnetId -LinkName "$VnetNameValue-postgres"
  if ($EnableAcrPrivateLink -and -not [string]::IsNullOrWhiteSpace($AcrNameValue)) {
    Ensure-PrivateDnsZoneLink -ResourceGroupName $ResourceGroupName -ZoneName "privatelink.azurecr.io" -VnetId $vnetId -LinkName "$VnetNameValue-acr"
  }

  $storageAccountId = Get-AzTsvOrEmpty -Arguments @(
    "storage", "account", "show",
    "--resource-group", $ResourceGroupName,
    "--name", $StorageAccount,
    "--query", "id",
    "-o", "tsv",
    "--only-show-errors"
  )
  if (-not [string]::IsNullOrWhiteSpace($storageAccountId)) {
    Ensure-PrivateEndpointConnection `
      -ResourceGroupName $ResourceGroupName `
      -LocationName $LocationName `
      -EndpointName "$StorageAccount-blob-pe" `
      -ConnectionName "$StorageAccount-blob-link" `
      -PrivateConnectionResourceId $storageAccountId `
      -GroupId "blob" `
      -VnetNameValue $VnetNameValue `
      -SubnetNameValue $PeSubnetName `
      -PrivateDnsZoneName "privatelink.blob.core.windows.net"

    Ensure-PrivateEndpointConnection `
      -ResourceGroupName $ResourceGroupName `
      -LocationName $LocationName `
      -EndpointName "$StorageAccount-dfs-pe" `
      -ConnectionName "$StorageAccount-dfs-link" `
      -PrivateConnectionResourceId $storageAccountId `
      -GroupId "dfs" `
      -VnetNameValue $VnetNameValue `
      -SubnetNameValue $PeSubnetName `
      -PrivateDnsZoneName "privatelink.dfs.core.windows.net"

    if ($DisablePublicDataPlane) {
      az storage account update `
        --resource-group $ResourceGroupName `
        --name $StorageAccount `
        --default-action Deny `
        --public-network-access Disabled `
        --only-show-errors 1>$null
    }
  }

  if (-not [string]::IsNullOrWhiteSpace($PostgresServer)) {
    $postgresServerId = Get-AzTsvOrEmpty -Arguments @(
      "postgres", "flexible-server", "show",
      "--resource-group", $ResourceGroupName,
      "--name", $PostgresServer,
      "--query", "id",
      "-o", "tsv",
      "--only-show-errors"
    )
    if (-not [string]::IsNullOrWhiteSpace($postgresServerId)) {
      $postgresPrivateLinkGroupsRaw = Get-AzTsvOrEmpty -Arguments @(
        "network", "private-link-resource", "list",
        "--id", $postgresServerId,
        "--query", "[].properties.groupId",
        "-o", "tsv",
        "--only-show-errors"
      )
      $postgresPrivateLinkGroups = @()
      if (-not [string]::IsNullOrWhiteSpace($postgresPrivateLinkGroupsRaw)) {
        $postgresPrivateLinkGroups = @(
          $postgresPrivateLinkGroupsRaw -split "`r?`n" |
          Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
      }
      if ($postgresPrivateLinkGroups -notcontains "postgresqlServer") {
        throw "Postgres flexible server '$PostgresServer' does not expose the required private link group 'postgresqlServer'. Replace or reconfigure the server in this phase instead of weakening the target topology."
      }

      Ensure-PrivateEndpointConnection `
        -ResourceGroupName $ResourceGroupName `
        -LocationName $LocationName `
        -EndpointName "$PostgresServer-pe" `
        -ConnectionName "$PostgresServer-link" `
        -PrivateConnectionResourceId $postgresServerId `
        -GroupId "postgresqlServer" `
        -VnetNameValue $VnetNameValue `
        -SubnetNameValue $PeSubnetName `
        -PrivateDnsZoneName "privatelink.postgres.database.azure.com"

      if ($DisablePublicDataPlane) {
        az postgres flexible-server update `
          --resource-group $ResourceGroupName `
          --name $PostgresServer `
          --public-access Disabled `
          --only-show-errors `
          --yes 1>$null
      }
    }
  }

  $acrPrivateEndpointName = ""
  if ($EnableAcrPrivateLink -and -not [string]::IsNullOrWhiteSpace($AcrNameValue)) {
    $acrResourceId = Get-AzTsvOrEmpty -Arguments @(
      "acr", "show",
      "--resource-group", $ResourceGroupName,
      "--name", $AcrNameValue,
      "--query", "id",
      "-o", "tsv",
      "--only-show-errors"
    )
    if ([string]::IsNullOrWhiteSpace($acrResourceId)) {
      throw "ACR '$AcrNameValue' was not found in resource group '$ResourceGroupName'. Create it before enabling ACR private link."
    }

    $acrSku = Get-AzTsvOrEmpty -Arguments @(
      "acr", "show",
      "--resource-group", $ResourceGroupName,
      "--name", $AcrNameValue,
      "--query", "sku.name",
      "-o", "tsv",
      "--only-show-errors"
    )
    if ($acrSku -ne "Premium") {
      Write-Host "Upgrading ACR '$AcrNameValue' to Premium for private endpoint support." -ForegroundColor Cyan
      az acr update `
        --resource-group $ResourceGroupName `
        --name $AcrNameValue `
        --sku Premium `
        --only-show-errors 1>$null
    }

    $acrPrivateEndpointName = "$AcrNameValue-registry-pe"
    Ensure-PrivateEndpointConnection `
      -ResourceGroupName $ResourceGroupName `
      -LocationName $LocationName `
      -EndpointName $acrPrivateEndpointName `
      -ConnectionName "$AcrNameValue-registry-link" `
      -PrivateConnectionResourceId $acrResourceId `
      -GroupId "registry" `
      -VnetNameValue $VnetNameValue `
      -SubnetNameValue $PeSubnetName `
      -PrivateDnsZoneName "privatelink.azurecr.io"

    if ($DisableAcrPublicNetworkAccess) {
      az acr update `
        --resource-group $ResourceGroupName `
        --name $AcrNameValue `
        --public-network-enabled false `
        --only-show-errors 1>$null
    }
  }

  Ensure-NetworkSmokeJob `
    -ResourceGroupName $ResourceGroupName `
    -EnvironmentName $EnvironmentName `
    -JobName $NetworkSmokeJobNameValue `
    -ApiInternalAppName $ApiInternalAppName `
    -StorageAccount $StorageAccount `
    -AcrNameValue $AcrNameValue `
    -PostgresServer $PostgresServer

  return [pscustomobject]@{
    VnetName                       = $VnetNameValue
    VnetId                         = $vnetId
    VnetAddressSpace               = $VnetAddressSpaceValue
    EnvironmentName                = $EnvironmentName
    InfrastructureSubnetName       = $AcaSubnetName
    InfrastructureSubnetId         = $acaSubnetId
    InfrastructureSubnetPrefix     = $AcaSubnetPrefix
    PrivateEndpointSubnetName      = $PeSubnetName
    PrivateEndpointSubnetPrefix    = $PeSubnetPrefix
    ReservedSubnetPrefix           = $ReservedSubnetPrefixValue
    NatGatewayName                 = $NatGatewayNameValue
    NatPublicIpName                = $NatPublicIpNameValue
    NatPublicIpAddress             = (Get-AzTsvOrEmpty -Arguments @("network", "public-ip", "show", "--resource-group", $ResourceGroupName, "--name", $NatPublicIpNameValue, "--query", "ipAddress", "-o", "tsv", "--only-show-errors"))
    NetworkSmokeJobName            = $NetworkSmokeJobNameValue
    ApiInternalAppName             = $ApiInternalAppName
    StoragePrivateDnsZone          = "privatelink.blob.core.windows.net"
    StorageDfsPrivateDnsZone       = "privatelink.dfs.core.windows.net"
    PostgresPrivateDnsZone         = "privatelink.postgres.database.azure.com"
    AcrPrivateDnsZone              = if ($EnableAcrPrivateLink) { "privatelink.azurecr.io" } else { "" }
    AcrPrivateEndpointName         = $acrPrivateEndpointName
    EnableAcrPrivateLink           = [bool]$EnableAcrPrivateLink
    DisableAcrPublicNetworkAccess  = [bool]$DisableAcrPublicNetworkAccess
    DisablePublicDataPlaneAccess   = [bool]$DisablePublicDataPlane
  }
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

if ((-not $PSBoundParameters.ContainsKey("ApiRuntimeIdentityName")) -or [string]::IsNullOrWhiteSpace($ApiRuntimeIdentityName)) {
  $apiRuntimeIdentityNameFromEnv = Get-EnvValueFirst -Keys @("API_RUNTIME_IDENTITY_NAME")
  if ($apiRuntimeIdentityNameFromEnv) {
    Write-Host "Using API_RUNTIME_IDENTITY_NAME from ${envLabel}: $apiRuntimeIdentityNameFromEnv"
    $ApiRuntimeIdentityName = $apiRuntimeIdentityNameFromEnv
  }
}

if ((-not $PSBoundParameters.ContainsKey("JobControlIdentityName")) -or [string]::IsNullOrWhiteSpace($JobControlIdentityName)) {
  $jobControlIdentityNameFromEnv = Get-EnvValueFirst -Keys @("JOB_CONTROL_IDENTITY_NAME")
  if ($jobControlIdentityNameFromEnv) {
    Write-Host "Using JOB_CONTROL_IDENTITY_NAME from ${envLabel}: $jobControlIdentityNameFromEnv"
    $JobControlIdentityName = $jobControlIdentityNameFromEnv
  }
}

if (-not $PSBoundParameters.ContainsKey("EnableAcrPrivateLink")) {
  $enableAcrPrivateLinkFromEnv = Get-EnvBool -Key "ENABLE_ACR_PRIVATE_LINK"
  if ($enableAcrPrivateLinkFromEnv -ne $null) {
    Write-Host "Using ENABLE_ACR_PRIVATE_LINK from ${envLabel}: $enableAcrPrivateLinkFromEnv"
    $EnableAcrPrivateLink = $enableAcrPrivateLinkFromEnv
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

if ((-not $PSBoundParameters.ContainsKey("VnetContainerAppsEnvironmentName")) -or [string]::IsNullOrWhiteSpace($VnetContainerAppsEnvironmentName)) {
  $vnetEnvFromEnv = Get-EnvValueFirst -Keys @("CONTAINER_APPS_ENVIRONMENT_VNET_NAME")
  if ($vnetEnvFromEnv) {
    Write-Host "Using CONTAINER_APPS_ENVIRONMENT_VNET_NAME from ${envLabel}: $vnetEnvFromEnv"
    $VnetContainerAppsEnvironmentName = $vnetEnvFromEnv
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

if (-not $PSBoundParameters.ContainsKey("VnetName")) {
  $vnetNameFromEnv = Get-EnvValueFirst -Keys @("ACA_VNET_NAME")
  if (-not [string]::IsNullOrWhiteSpace($vnetNameFromEnv)) {
    $VnetName = $vnetNameFromEnv.Trim()
    Write-Host "Using ACA_VNET_NAME from ${envLabel}: $VnetName"
  }
}

if (-not $PSBoundParameters.ContainsKey("VnetAddressSpace")) {
  $vnetAddressSpaceFromEnv = Get-EnvValueFirst -Keys @("ACA_VNET_ADDRESS_SPACE")
  if (-not [string]::IsNullOrWhiteSpace($vnetAddressSpaceFromEnv)) {
    $VnetAddressSpace = $vnetAddressSpaceFromEnv.Trim()
    Write-Host "Using ACA_VNET_ADDRESS_SPACE from ${envLabel}: $VnetAddressSpace"
  }
}

if (-not $PSBoundParameters.ContainsKey("AcaInfrastructureSubnetName")) {
  $acaInfraSubnetNameFromEnv = Get-EnvValueFirst -Keys @("ACA_INFRA_SUBNET_NAME")
  if (-not [string]::IsNullOrWhiteSpace($acaInfraSubnetNameFromEnv)) {
    $AcaInfrastructureSubnetName = $acaInfraSubnetNameFromEnv.Trim()
    Write-Host "Using ACA_INFRA_SUBNET_NAME from ${envLabel}: $AcaInfrastructureSubnetName"
  }
}

if (-not $PSBoundParameters.ContainsKey("AcaInfrastructureSubnetPrefix")) {
  $acaInfraSubnetPrefixFromEnv = Get-EnvValueFirst -Keys @("ACA_INFRA_SUBNET_PREFIX")
  if (-not [string]::IsNullOrWhiteSpace($acaInfraSubnetPrefixFromEnv)) {
    $AcaInfrastructureSubnetPrefix = $acaInfraSubnetPrefixFromEnv.Trim()
    Write-Host "Using ACA_INFRA_SUBNET_PREFIX from ${envLabel}: $AcaInfrastructureSubnetPrefix"
  }
}

if (-not $PSBoundParameters.ContainsKey("PrivateEndpointSubnetName")) {
  $privateEndpointSubnetNameFromEnv = Get-EnvValueFirst -Keys @("PRIVATE_ENDPOINT_SUBNET_NAME")
  if (-not [string]::IsNullOrWhiteSpace($privateEndpointSubnetNameFromEnv)) {
    $PrivateEndpointSubnetName = $privateEndpointSubnetNameFromEnv.Trim()
    Write-Host "Using PRIVATE_ENDPOINT_SUBNET_NAME from ${envLabel}: $PrivateEndpointSubnetName"
  }
}

if (-not $PSBoundParameters.ContainsKey("PrivateEndpointSubnetPrefix")) {
  $privateEndpointSubnetPrefixFromEnv = Get-EnvValueFirst -Keys @("PRIVATE_ENDPOINT_SUBNET_PREFIX")
  if (-not [string]::IsNullOrWhiteSpace($privateEndpointSubnetPrefixFromEnv)) {
    $PrivateEndpointSubnetPrefix = $privateEndpointSubnetPrefixFromEnv.Trim()
    Write-Host "Using PRIVATE_ENDPOINT_SUBNET_PREFIX from ${envLabel}: $PrivateEndpointSubnetPrefix"
  }
}

if (-not $PSBoundParameters.ContainsKey("ReservedSubnetPrefix")) {
  $reservedSubnetPrefixFromEnv = Get-EnvValueFirst -Keys @("ACA_RESERVED_SUBNET_PREFIX")
  if (-not [string]::IsNullOrWhiteSpace($reservedSubnetPrefixFromEnv)) {
    $ReservedSubnetPrefix = $reservedSubnetPrefixFromEnv.Trim()
    Write-Host "Using ACA_RESERVED_SUBNET_PREFIX from ${envLabel}: $ReservedSubnetPrefix"
  }
}

if (-not $PSBoundParameters.ContainsKey("NatGatewayName")) {
  $natGatewayNameFromEnv = Get-EnvValueFirst -Keys @("NAT_GATEWAY_NAME")
  if (-not [string]::IsNullOrWhiteSpace($natGatewayNameFromEnv)) {
    $NatGatewayName = $natGatewayNameFromEnv.Trim()
    Write-Host "Using NAT_GATEWAY_NAME from ${envLabel}: $NatGatewayName"
  }
}

if (-not $PSBoundParameters.ContainsKey("NatPublicIpName")) {
  $natPublicIpNameFromEnv = Get-EnvValueFirst -Keys @("NAT_PUBLIC_IP_NAME")
  if (-not [string]::IsNullOrWhiteSpace($natPublicIpNameFromEnv)) {
    $NatPublicIpName = $natPublicIpNameFromEnv.Trim()
    Write-Host "Using NAT_PUBLIC_IP_NAME from ${envLabel}: $NatPublicIpName"
  }
}

if (-not $PSBoundParameters.ContainsKey("NetworkSmokeJobName")) {
  $networkSmokeJobNameFromEnv = Get-EnvValueFirst -Keys @("ACA_NETWORK_SMOKE_JOB_NAME")
  if (-not [string]::IsNullOrWhiteSpace($networkSmokeJobNameFromEnv)) {
    $NetworkSmokeJobName = $networkSmokeJobNameFromEnv.Trim()
    Write-Host "Using ACA_NETWORK_SMOKE_JOB_NAME from ${envLabel}: $NetworkSmokeJobName"
  }
}

if (-not $PSBoundParameters.ContainsKey("VnetApiAppName")) {
  $vnetApiAppNameFromEnv = Get-EnvValueFirst -Keys @("API_APP_VNET_NAME")
  if (-not [string]::IsNullOrWhiteSpace($vnetApiAppNameFromEnv)) {
    $VnetApiAppName = $vnetApiAppNameFromEnv.Trim()
    Write-Host "Using API_APP_VNET_NAME from ${envLabel}: $VnetApiAppName"
  }
}

if (-not $PSBoundParameters.ContainsKey("VnetUiAppName")) {
  $vnetUiAppNameFromEnv = Get-EnvValueFirst -Keys @("UI_APP_VNET_NAME")
  if (-not [string]::IsNullOrWhiteSpace($vnetUiAppNameFromEnv)) {
    $VnetUiAppName = $vnetUiAppNameFromEnv.Trim()
    Write-Host "Using UI_APP_VNET_NAME from ${envLabel}: $VnetUiAppName"
  }
}

if (-not $PSBoundParameters.ContainsKey("UiPublicHostname")) {
  $uiPublicHostnameFromEnv = Get-EnvValueFirst -Keys @("UI_PUBLIC_HOSTNAME")
  if (-not [string]::IsNullOrWhiteSpace($uiPublicHostnameFromEnv)) {
    $UiPublicHostname = $uiPublicHostnameFromEnv.Trim()
    Write-Host "Using UI_PUBLIC_HOSTNAME from ${envLabel}: $UiPublicHostname"
  }
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

function Sync-EnvWebToGitHub {
  param([Parameter(Mandatory = $true)][string]$EnvPath)

  if ((Split-Path -Leaf $EnvPath) -ine ".env.web") {
    Write-Host "Skipping GitHub env sync because the active env file is not .env.web." -ForegroundColor DarkGray
    return
  }

  $syncScript = Join-Path $repoRoot "scripts\sync-all-to-github.ps1"
  if (-not (Test-Path $syncScript)) {
    throw "GitHub sync script not found at $syncScript"
  }

  Assert-CommandExists -Name "gh"

  Write-Host "Syncing .env.web values to GitHub vars/secrets..." -ForegroundColor Cyan
  & $syncScript
  if (-not $?) {
    throw "GitHub vars/secrets sync failed."
  }
}

function Configure-AiRelayBootstrap {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$ContainerAppName
  )

  $enabled = Get-EnvBool -Key "AI_RELAY_ENABLED"
  if ($enabled -ne $true) {
    Write-Host "Skipping AI relay bootstrap because AI_RELAY_ENABLED is not true in ${envLabel}." -ForegroundColor DarkGray
    return $false
  }

  $apiKey = Get-EnvValue -Key "AI_RELAY_API_KEY"
  if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AI_RELAY_ENABLED=true, but AI_RELAY_API_KEY is blank in ${envLabel}."
  }

  $requiredRoles = Get-EnvValue -Key "AI_RELAY_REQUIRED_ROLES"
  if ([string]::IsNullOrWhiteSpace($requiredRoles)) {
    throw "AI_RELAY_ENABLED=true, but AI_RELAY_REQUIRED_ROLES is blank in ${envLabel}."
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
    Write-Warning "Skipping AI relay bootstrap because container app '$ContainerAppName' was not found in '$ResourceGroupName'."
    return $false
  }

  $secretName = "ai-relay-api-key"
  Write-Host "Setting AI relay secret on container app '$ContainerAppName'..." -ForegroundColor Cyan
  $secretArgs = @(
    "containerapp", "secret", "set",
    "--name", $ContainerAppName,
    "--resource-group", $ResourceGroupName,
    "--secrets", "$secretName=$apiKey",
    "--only-show-errors"
  )
  & az @secretArgs 1>$null
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to set AI relay secret on container app '$ContainerAppName'."
  }

  $setEnvVars = @(
    "AI_RELAY_ENABLED=true",
    "AI_RELAY_MODEL=$(Get-EnvValue -Key 'AI_RELAY_MODEL')",
    "AI_RELAY_REASONING_EFFORT=$(Get-EnvValue -Key 'AI_RELAY_REASONING_EFFORT')",
    "AI_RELAY_TIMEOUT_SECONDS=$(Get-EnvValue -Key 'AI_RELAY_TIMEOUT_SECONDS')",
    "AI_RELAY_MAX_PROMPT_CHARS=$(Get-EnvValue -Key 'AI_RELAY_MAX_PROMPT_CHARS')",
    "AI_RELAY_MAX_FILES=$(Get-EnvValue -Key 'AI_RELAY_MAX_FILES')",
    "AI_RELAY_MAX_FILE_BYTES=$(Get-EnvValue -Key 'AI_RELAY_MAX_FILE_BYTES')",
    "AI_RELAY_MAX_TOTAL_FILE_BYTES=$(Get-EnvValue -Key 'AI_RELAY_MAX_TOTAL_FILE_BYTES')",
    "AI_RELAY_MAX_OUTPUT_TOKENS=$(Get-EnvValue -Key 'AI_RELAY_MAX_OUTPUT_TOKENS')",
    "AI_RELAY_REQUIRED_ROLES=$requiredRoles",
    "AI_RELAY_API_KEY=secretref:$secretName"
  )

  Write-Host "Applying AI relay environment settings to container app '$ContainerAppName'..." -ForegroundColor Cyan
  $updateArgs = @(
    "containerapp", "update",
    "--name", $ContainerAppName,
    "--resource-group", $ResourceGroupName,
    "--set-env-vars"
  )
  $updateArgs += $setEnvVars
  $updateArgs += "--only-show-errors"
  & az @updateArgs 1>$null
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to update AI relay settings on container app '$ContainerAppName'."
  }

  return $true
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
$apiRuntimeIdentityId = ""
$apiRuntimeIdentityClientId = ""
$apiRuntimeIdentityPrincipalId = ""
$jobControlIdentityId = ""
$jobControlIdentityClientId = ""
$jobControlIdentityPrincipalId = ""

if (-not $NonInteractive -and $PromptForResources) {
  Write-Host ""
  Write-Host "Resource provisioning prompts (set -NonInteractive to skip prompts)" -ForegroundColor Cyan
}

if ($AzureClientId) {
  $doFederatedCredential = Get-YesNo "Ensure GitHub Actions federated credential on Azure app ($AzureClientId)?" $true
}

if ($AzureClientId -and $doFederatedCredential) {
  $repoSlug = Get-GitHubRepositorySlug
  if ([string]::IsNullOrWhiteSpace($repoSlug)) {
    throw "Unable to resolve GitHub repository slug from origin remote. Set AZURE_CLIENT_ID manually and provision federated credentials separately."
  }
  $repoName = ($repoSlug -split "/", 2)[1]
  if ([string]::IsNullOrWhiteSpace($repoName)) {
    throw "Unable to derive GitHub repository name from '$repoSlug'."
  }
  $credentialPrefix = ("github-actions-" + ($repoName -replace "[^A-Za-z0-9-]", "-")).TrimEnd("-")

  $federatedCredentials = @(
    @{
      Name        = "$credentialPrefix-main"
      Subject     = "repo:$repoSlug:ref:refs/heads/main"
      Description = "GitHub Actions OIDC for main branch"
    },
    @{
      Name        = "$credentialPrefix-prod"
      Subject     = "repo:$repoSlug:environment:prod"
      Description = "GitHub Actions OIDC for prod environment"
    }
  )

  foreach ($credential in $federatedCredentials) {
    Write-Host "Checking for existing Federated Credential '$($credential.Name)'..."
    $creds = az ad app federated-credential list --id $AzureClientId --query "[?name=='$($credential.Name)']" -o json | ConvertFrom-Json

    if (-not $creds) {
      Write-Host "Creating Federated Credential '$($credential.Name)'..."
      $paramsFile = Join-Path $env:TEMP "$($credential.Name).json"
      $json = @{
        name        = $credential.Name
        issuer      = "https://token.actions.githubusercontent.com"
        subject     = $credential.Subject
        description = $credential.Description
        audiences   = @("api://AzureADTokenExchange")
      } | ConvertTo-Json -Compress

      Set-Content -Path $paramsFile -Value $json

      try {
        az ad app federated-credential create --id $AzureClientId --parameters $paramsFile 2>&1
        Write-Host "Successfully created federated credential '$($credential.Name)'."
      }
      catch {
        Write-Error "Failed to create federated credential '$($credential.Name)': $_"
        if (Test-Path $paramsFile) { Remove-Item $paramsFile }
        throw
      }

      if (Test-Path $paramsFile) { Remove-Item $paramsFile }
    }
    else {
      Write-Host "Federated Credential '$($credential.Name)' already exists."
    }
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
    EnvFile              = $envPath
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

  if (Test-Path $envPath) {
    $envLines = Get-Content $envPath
  }

  Sync-EnvWebToGitHub -EnvPath $envPath
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
  $acrSkuName = if ($EnableAcrPrivateLink) { "Premium" } else { "Basic" }
  az acr create `
    --name $AcrName `
    --resource-group $ResourceGroup `
    --location $Location `
    --sku $acrSkuName `
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

$parallelPrivateRuntime = $null
$doParallelPrivateRuntime = $false
if ($SkipParallelPrivateRuntime) {
  Write-Host "Skipping parallel VNet Container Apps environment because -SkipParallelPrivateRuntime was supplied."
}
else {
  $doParallelPrivateRuntime = Get-YesNo "Ensure parallel VNet Container Apps environment exists: ${VnetContainerAppsEnvironmentName}?" $true
}
if ($doParallelPrivateRuntime) {
  if (-not $lawCustomerId -or -not $lawSharedKey) {
    throw "Log Analytics workspace details missing; cannot create the parallel VNet Container Apps environment. Enable Log Analytics or provide workspace info."
  }

  Write-Host "Ensuring parallel VNet Container Apps environment exists: $VnetContainerAppsEnvironmentName"
  $parallelPrivateRuntime = Ensure-ParallelPrivateRuntime `
    -ResourceGroupName $ResourceGroup `
    -LocationName $Location `
    -VnetNameValue $VnetName `
    -VnetAddressSpaceValue $VnetAddressSpace `
    -AcaSubnetName $AcaInfrastructureSubnetName `
    -AcaSubnetPrefix $AcaInfrastructureSubnetPrefix `
    -PeSubnetName $PrivateEndpointSubnetName `
    -PeSubnetPrefix $PrivateEndpointSubnetPrefix `
    -ReservedSubnetPrefixValue $ReservedSubnetPrefix `
    -NatGatewayNameValue $NatGatewayName `
    -NatPublicIpNameValue $NatPublicIpName `
    -EnvironmentName $VnetContainerAppsEnvironmentName `
    -NetworkSmokeJobNameValue $NetworkSmokeJobName `
    -ApiInternalAppName $VnetApiAppName `
    -WorkspaceCustomerId $lawCustomerId `
    -WorkspaceSharedKey $lawSharedKey `
    -StorageAccount $StorageAccountName `
    -AcrNameValue $AcrName `
    -PostgresServer $PostgresServerName `
    -EnableAcrPrivateLink:$EnableAcrPrivateLink `
    -DisableAcrPublicNetworkAccess:$DisableAcrPublicNetworkAccess `
    -DisablePublicDataPlane:$DisablePublicDataPlaneAccess

  Write-Host ("Parallel private runtime ready: environment={0} vnet={1} natPublicIp={2} smokeJob={3}" -f `
      $parallelPrivateRuntime.EnvironmentName, `
      $parallelPrivateRuntime.VnetName, `
      $(if (-not [string]::IsNullOrWhiteSpace($parallelPrivateRuntime.NatPublicIpAddress)) { $parallelPrivateRuntime.NatPublicIpAddress } else { "<pending>" }), `
      $parallelPrivateRuntime.NetworkSmokeJobName) -ForegroundColor Cyan
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

  Write-Host "Ensuring user-assigned managed identity exists (for API runtime): $ApiRuntimeIdentityName"
  $apiRuntimeIdentity = Ensure-UserAssignedIdentity `
    -IdentityName $ApiRuntimeIdentityName `
    -ResourceGroupName $ResourceGroup `
    -LocationName $Location
  $apiRuntimeIdentityId = $apiRuntimeIdentity.id
  $apiRuntimeIdentityClientId = $apiRuntimeIdentity.clientId
  $apiRuntimeIdentityPrincipalId = $apiRuntimeIdentity.principalId

  Write-Host "Ensuring user-assigned managed identity exists (for job/control operations): $JobControlIdentityName"
  $jobControlIdentity = Ensure-UserAssignedIdentity `
    -IdentityName $JobControlIdentityName `
    -ResourceGroupName $ResourceGroup `
    -LocationName $Location
  $jobControlIdentityId = $jobControlIdentity.id
  $jobControlIdentityClientId = $jobControlIdentity.clientId
  $jobControlIdentityPrincipalId = $jobControlIdentity.principalId

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
  if ($storageAccountId -and $apiRuntimeIdentityPrincipalId) {
    Write-Host ""
    Write-Host "Ensuring API runtime identity can access storage data (Storage Blob Data Contributor)..."
    $storageDataRuntimeExisting = "0"
    try {
      $storageDataRuntimeExisting = az role assignment list `
        --assignee-object-id $apiRuntimeIdentityPrincipalId `
        --scope $storageAccountId `
        --query "[?roleDefinitionName=='Storage Blob Data Contributor'] | length(@)" -o tsv --only-show-errors 2>$null
      if (-not $storageDataRuntimeExisting) { $storageDataRuntimeExisting = "0" }
    }
    catch {
      $storageDataRuntimeExisting = "0"
    }

    if ([int]$storageDataRuntimeExisting -eq 0) {
      az role assignment create `
        --assignee-object-id $apiRuntimeIdentityPrincipalId `
        --assignee-principal-type ServicePrincipal `
        --role "Storage Blob Data Contributor" `
        --scope $storageAccountId `
        --only-show-errors 1>$null
      Write-Host "  Storage Blob Data Contributor granted to $ApiRuntimeIdentityName on $StorageAccountName."
    }
    else {
      Write-Host "  Storage Blob Data Contributor already assigned to $ApiRuntimeIdentityName on $StorageAccountName."
    }
  }
}

if ($AzureClientId -and $doManagedIdentity) {
  Write-Host ""
  Write-Host "Ensuring GitHub Actions principal can assign deployment managed identities..."
  if (-not $githubSpObjectId) {
    try {
      $githubSpObjectId = az ad sp show --id $AzureClientId --query id -o tsv --only-show-errors 2>$null
    }
    catch {
      $githubSpObjectId = $null
    }
  }

  if ($githubSpObjectId) {
    $identityScopes = @(
      @{ Name = $AcrPullIdentityName; Id = $acrPullIdentityId },
      @{ Name = $ApiRuntimeIdentityName; Id = $apiRuntimeIdentityId },
      @{ Name = $JobControlIdentityName; Id = $jobControlIdentityId }
    )
    foreach ($identityScope in $identityScopes) {
      if ([string]::IsNullOrWhiteSpace([string]$identityScope.Id)) {
        continue
      }
      $created = Ensure-RoleAssignment `
        -PrincipalId $githubSpObjectId `
        -RoleName "Managed Identity Operator" `
        -Scope ([string]$identityScope.Id)
      if ($created) {
        Write-Host "  Managed Identity Operator granted to $AzureClientId on $($identityScope.Name)."
      }
      else {
        Write-Host "  Managed Identity Operator already assigned to $AzureClientId on $($identityScope.Name)."
      }
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
  Write-Host "API runtime identity resource ID:"
  Write-Host "  $apiRuntimeIdentityId"
  Write-Host "Set API_RUNTIME_IDENTITY_NAME to '$ApiRuntimeIdentityName' and render API_RUNTIME_IDENTITY_CLIENT_ID as the runtime AZURE_CLIENT_ID."
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
    $rgScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup"
    $acaOperatorRoleName = Ensure-AcaOperatorRoleDefinition `
      -RoleName "Asset Allocation ACA Operator" `
      -Scope $rgScope

    Write-Host "Granting narrow Container Apps/job operator permissions (best-effort)..."
    Write-Host "  Scope: Resource group $ResourceGroup"
    Write-Host "  Role: $acaOperatorRoleName"

    $operatorIdentities = @(
      @{ Name = $ApiRuntimeIdentityName; PrincipalId = $apiRuntimeIdentityPrincipalId },
      @{ Name = $JobControlIdentityName; PrincipalId = $jobControlIdentityPrincipalId }
    )
    foreach ($identity in $operatorIdentities) {
      if ([string]::IsNullOrWhiteSpace([string]$identity.PrincipalId)) {
        continue
      }
      try {
        $created = Ensure-RoleAssignment `
          -PrincipalId ([string]$identity.PrincipalId) `
          -RoleName $acaOperatorRoleName `
          -Scope $rgScope
        if ($created) {
          $jobStartAssignmentsCreated += 1
          Write-Host "  $acaOperatorRoleName granted to $($identity.Name)." -ForegroundColor Cyan
        }
        else {
          $jobStartAssignmentsSkipped += 1
          Write-Host "  $acaOperatorRoleName already assigned to $($identity.Name)."
        }
      }
      catch {
        Write-Warning "Failed to grant $acaOperatorRoleName to $($identity.Name): $($_.Exception.Message)"
      }
    }

    Write-Host "Job start role assignment summary: created=$jobStartAssignmentsCreated skipped=$jobStartAssignmentsSkipped"
  }
}
else {
  Write-Host ""
  Write-Host "NOTE: Jobs may trigger downstream jobs and wake API container apps via ARM."
  Write-Host "To grant the required narrow permissions, re-run this script with -GrantJobStartToAcaResources."
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

$aiRelayBootstrapApplied = Configure-AiRelayBootstrap `
  -ResourceGroupName $ResourceGroup `
  -ContainerAppName $ApiContainerAppName
if ($aiRelayBootstrapApplied) {
  Sync-EnvWebToGitHub -EnvPath $envPath
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
  apiRuntimeIdentityName                  = $ApiRuntimeIdentityName
  apiRuntimeIdentityId                    = $apiRuntimeIdentityId
  apiRuntimeIdentityResourceId            = $apiRuntimeIdentityId
  apiRuntimeIdentityClientId              = $apiRuntimeIdentityClientId
  apiRuntimeIdentityPrincipalId           = $apiRuntimeIdentityPrincipalId
  jobControlIdentityName                  = $JobControlIdentityName
  jobControlIdentityId                    = $jobControlIdentityId
  jobControlIdentityResourceId            = $jobControlIdentityId
  jobControlIdentityClientId              = $jobControlIdentityClientId
  jobControlIdentityPrincipalId           = $jobControlIdentityPrincipalId
  acrPullIdentityOperatorAssigneeObjectId = $githubSpObjectId
  acrPullAssignmentsCreated               = $acrPullAssignmentsCreated
  acrPullAssignmentsSkipped               = $acrPullAssignmentsSkipped
  jobStartAssignmentsCreated              = $jobStartAssignmentsCreated
  jobStartAssignmentsSkipped              = $jobStartAssignmentsSkipped
  apiStorageAuthCorrectionRequested       = [bool]$CorrectApiStorageAuthMode
  apiStorageAuthMode                      = $ApiStorageAuthMode
  apiContainerAppName                     = $ApiContainerAppName
  aiRelayBootstrapApplied                 = [bool]$aiRelayBootstrapApplied
  logAnalyticsWorkspaceName               = $LogAnalyticsWorkspaceName
  logAnalyticsCustomerId                  = $lawCustomerId
  containerAppsEnvironmentName            = $ContainerAppsEnvironmentName
  containerAppsEnvironmentVnetName        = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.EnvironmentName } else { $VnetContainerAppsEnvironmentName }
  vnetName                                = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.VnetName } else { $VnetName }
  vnetAddressSpace                        = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.VnetAddressSpace } else { $VnetAddressSpace }
  acaInfrastructureSubnetName             = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.InfrastructureSubnetName } else { $AcaInfrastructureSubnetName }
  acaInfrastructureSubnetPrefix           = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.InfrastructureSubnetPrefix } else { $AcaInfrastructureSubnetPrefix }
  privateEndpointSubnetName               = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.PrivateEndpointSubnetName } else { $PrivateEndpointSubnetName }
  privateEndpointSubnetPrefix             = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.PrivateEndpointSubnetPrefix } else { $PrivateEndpointSubnetPrefix }
  reservedSubnetPrefix                    = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.ReservedSubnetPrefix } else { $ReservedSubnetPrefix }
  natGatewayName                          = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.NatGatewayName } else { $NatGatewayName }
  natPublicIpName                         = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.NatPublicIpName } else { $NatPublicIpName }
  natPublicIpAddress                      = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.NatPublicIpAddress } else { "" }
  networkSmokeJobName                     = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.NetworkSmokeJobName } else { $NetworkSmokeJobName }
  acrPrivateDnsZone                       = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.AcrPrivateDnsZone } else { "" }
  acrPrivateEndpointName                  = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.AcrPrivateEndpointName } else { "" }
  enableAcrPrivateLink                    = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.EnableAcrPrivateLink } else { [bool]$EnableAcrPrivateLink }
  disableAcrPublicNetworkAccess           = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.DisableAcrPublicNetworkAccess } else { [bool]$DisableAcrPublicNetworkAccess }
  apiVnetContainerAppName                 = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.ApiInternalAppName } else { $VnetApiAppName }
  uiVnetContainerAppName                  = $VnetUiAppName
  uiPublicHostname                        = $UiPublicHostname
  disablePublicDataPlaneAccess            = if ($null -ne $parallelPrivateRuntime) { $parallelPrivateRuntime.DisablePublicDataPlaneAccess } else { [bool]$DisablePublicDataPlaneAccess }
  kubernetesServiceAccountName            = $ServiceAccountName
  kubernetesNamespace                     = $KubernetesNamespace
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 4
