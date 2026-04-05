param(
  [string]$Location = "eastus",
  [string]$DeploymentName = "asset-allocation-cost-guardrails",
  [string]$ResourceGroupName = "AssetAllocationRG",
  [string]$NotificationEmail = "rdprokes@gmail.com",
  [string[]]$ContactEmails = @("rdprokes@gmail.com"),
  [ValidateRange(0, 23)]
  [int]$AnomalyAlertHourOfDay = 13,
  [string]$StartDate = "",
  [string]$EndDate = "",
  [switch]$DisableAnomalyAlert,
  [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$templateFile = Join-Path $repoRoot "deploy/cost_guardrails.bicep"

if (-not (Test-Path $templateFile)) {
  throw "Template file not found: $templateFile"
}

function ConvertTo-UtcDateString {
  param([datetime]$Date)
  return $Date.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

if ([string]::IsNullOrWhiteSpace($StartDate)) {
  $utcNow = [datetime]::UtcNow
  $StartDate = ConvertTo-UtcDateString ([datetime]::new($utcNow.Year, $utcNow.Month, 1, 0, 0, 0, [datetimekind]::Utc))
}

if ([string]::IsNullOrWhiteSpace($EndDate)) {
  $startDateTime = [datetime]::Parse($StartDate).ToUniversalTime()
  $EndDate = ConvertTo-UtcDateString ($startDateTime.AddYears(10))
}

$budgetDefinitions = @(
  @{
    name = "asset-allocation-container-apps"
    amount = 150
    meterCategories = @("Container Apps")
    actualThreshold = 80
    forecastThreshold = 100
  },
  @{
    name = "asset-allocation-monitoring"
    amount = 30
    meterCategories = @("Azure Monitor")
    actualThreshold = 80
    forecastThreshold = 100
  },
  @{
    name = "asset-allocation-storage"
    amount = 40
    meterCategories = @("Storage")
    actualThreshold = 80
    forecastThreshold = 100
  },
  @{
    name = "asset-allocation-container-registry"
    amount = 20
    meterCategories = @("Container Registry")
    actualThreshold = 80
    forecastThreshold = 100
  },
  @{
    name = "asset-allocation-postgres"
    amount = 50
    meterCategories = @("Azure Database for PostgreSQL")
    actualThreshold = 80
    forecastThreshold = 100
  }
)

$parameterPayload = @{
  '$schema' = "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#"
  contentVersion = "1.0.0.0"
  parameters = @{
    contactEmails = @{ value = $ContactEmails }
    notificationEmail = @{ value = $NotificationEmail }
    startDate = @{ value = $StartDate }
    endDate = @{ value = $EndDate }
    resourceGroupFilterValues = @{ value = @($ResourceGroupName) }
    budgetDefinitions = @{ value = $budgetDefinitions }
    anomalyAlertEnabled = @{ value = (-not $DisableAnomalyAlert.IsPresent) }
    anomalyAlertHourOfDay = @{ value = $AnomalyAlertHourOfDay }
  }
}

$tempParameterFile = Join-Path ([System.IO.Path]::GetTempPath()) "asset-allocation-cost-guardrails.parameters.json"
$parameterPayload | ConvertTo-Json -Depth 20 | Set-Content -Path $tempParameterFile -Encoding UTF8

Write-Host "Using budget window start=$StartDate end=$EndDate" -ForegroundColor Cyan
Write-Host "Using resource-group filter: $ResourceGroupName" -ForegroundColor Cyan
Write-Host "Using notification emails: $($ContactEmails -join ', ')" -ForegroundColor Cyan
Write-Host "Generated parameter file: $tempParameterFile" -ForegroundColor Gray
Write-Host "Default budget definitions:" -ForegroundColor Gray
$budgetDefinitions | ForEach-Object {
  Write-Host "  - $($_.name): amount=$($_.amount) meterCategories=$([string]::Join(',', $_.meterCategories))" -ForegroundColor Gray
}

if ($WhatIf) {
  Write-Host "Running subscription-scope what-if for cost guardrails..." -ForegroundColor Yellow
  & az deployment sub what-if `
    --name $DeploymentName `
    --location $Location `
    --template-file $templateFile `
    --parameters "@$tempParameterFile"
}
else {
  Write-Host "Deploying subscription-scope cost guardrails..." -ForegroundColor Yellow
  & az deployment sub create `
    --name $DeploymentName `
    --location $Location `
    --template-file $templateFile `
    --parameters "@$tempParameterFile"
}

if ($LASTEXITCODE -ne 0) {
  throw "Azure deployment command failed with exit code $LASTEXITCODE."
}
