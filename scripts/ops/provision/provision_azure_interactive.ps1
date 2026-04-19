param(
  [string]$EnvFile = "",
  [switch]$EmitSecrets
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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

function Resolve-EnvFilePath {
  param([string]$RequestedPath)

  if (-not [string]::IsNullOrWhiteSpace($RequestedPath)) {
    return (Resolve-Path $RequestedPath -ErrorAction Stop).Path
  }

  $candidateWeb = Join-Path $repoRoot ".env.web"
  $candidateEnv = Join-Path $repoRoot ".env"

  if (Test-Path $candidateWeb) {
    return (Resolve-Path $candidateWeb -ErrorAction Stop).Path
  }

  if (Test-Path $candidateEnv) {
    return (Resolve-Path $candidateEnv -ErrorAction Stop).Path
  }

  return $null
}

function Get-EnvLines {
  param([string]$Path)

  if ([string]::IsNullOrWhiteSpace($Path) -or (-not (Test-Path $Path))) {
    return @()
  }

  return ,@(Get-Content -Path $Path)
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string[]]$Lines = @()
  )

  foreach ($line in $Lines) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
      continue
    }

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
    [string[]]$Lines = @()
  )

  foreach ($key in $Keys) {
    $value = Get-EnvValue -Key $key -Lines $Lines
    if (-not [string]::IsNullOrWhiteSpace($value)) {
      return $value
    }
  }

  return $null
}

function Parse-EnvBool {
  param([string]$Raw)

  if ([string]::IsNullOrWhiteSpace($Raw)) {
    return $null
  }

  $normalized = $Raw.Trim().ToLowerInvariant()
  if ($normalized -in @("1", "true", "yes", "y", "on")) { return $true }
  if ($normalized -in @("0", "false", "no", "n", "off")) { return $false }

  throw "Invalid boolean value '$Raw'. Expected true/false."
}

function Get-YesNo {
  param(
    [Parameter(Mandatory = $true)][string]$Prompt,
    [bool]$DefaultYes = $true
  )

  $suffix = if ($DefaultYes) { "[Y/n]" } else { "[y/N]" }
  while ($true) {
    $input = Read-Host "$Prompt $suffix"
    if ([string]::IsNullOrWhiteSpace($input)) {
      return $DefaultYes
    }

    $value = $input.Trim().ToLowerInvariant()
    if ($value -in @("y", "yes")) { return $true }
    if ($value -in @("n", "no")) { return $false }

    Write-Host "Please enter y or n." -ForegroundColor Yellow
  }
}

function Read-SecretText {
  param([Parameter(Mandatory = $true)][string]$Prompt)

  $secureValue = Read-Host $Prompt -AsSecureString
  $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
  }
  finally {
    if ($bstr -ne [IntPtr]::Zero) {
      [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
  }
}

function Resolve-PowerShellExe {
  $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
  if ($pwsh -and $pwsh.Source) {
    return $pwsh.Source
  }

  $powershell = Get-Command powershell -ErrorAction SilentlyContinue
  if ($powershell -and $powershell.Source) {
    return $powershell.Source
  }

  return $null
}

function New-ProvisionLogDirectory {
  $root = if (-not [string]::IsNullOrWhiteSpace($env:ASSET_ALLOCATION_PROVISION_LOG_ROOT)) {
    $env:ASSET_ALLOCATION_PROVISION_LOG_ROOT
  }
  else {
    Join-Path ([System.IO.Path]::GetTempPath()) "asset-allocation-provisioning"
  }

  if (-not (Test-Path $root)) {
    New-Item -ItemType Directory -Path $root -Force | Out-Null
  }

  $sessionDir = Join-Path $root (Get-Date -Format "yyyyMMdd-HHmmss")
  New-Item -ItemType Directory -Path $sessionDir -Force | Out-Null
  return $sessionDir
}

function ConvertTo-SafeFileName {
  param([Parameter(Mandatory = $true)][string]$Value)

  $safe = $Value
  foreach ($invalid in [System.IO.Path]::GetInvalidFileNameChars()) {
    $safe = $safe.Replace([string]$invalid, "-")
  }
  $safe = ($safe -replace "\s+", "-").Trim("-")
  if ([string]::IsNullOrWhiteSpace($safe)) {
    return "step"
  }
  return $safe
}

function Add-StringArgument {
  param(
    [Parameter(Mandatory = $true)][AllowEmptyCollection()][System.Collections.Generic.List[string]]$Arguments,
    [Parameter(Mandatory = $true)][string]$Name,
    [string]$Value
  )

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return
  }

  $Arguments.Add("-$Name")
  $Arguments.Add($Value)
}

function Add-SwitchArgument {
  param(
    [Parameter(Mandatory = $true)][AllowEmptyCollection()][System.Collections.Generic.List[string]]$Arguments,
    [Parameter(Mandatory = $true)][string]$Name,
    [bool]$Enabled
  )

  if ($Enabled) {
    $Arguments.Add("-$Name")
  }
}

function Add-StepResult {
  param(
    [Parameter(Mandatory = $true)][string]$Step,
    [Parameter(Mandatory = $true)][string]$Status,
    [string]$Notes = ""
  )

  $script:StepResults.Add(
    [pscustomobject]@{
      Step   = $Step
      Status = $Status
      Notes  = $Notes
    }
  ) | Out-Null
}

function Invoke-ChildScript {
  param(
    [Parameter(Mandatory = $true)][string]$StepName,
    [Parameter(Mandatory = $true)][string]$ScriptPath,
    [string[]]$Arguments = @()
  )

  $logPath = Join-Path $script:SessionLogDir ("{0}.log" -f (ConvertTo-SafeFileName -Value $StepName))

  Write-Host ""
  Write-Host "==> $StepName" -ForegroundColor Cyan
  Write-Host "    $(Split-Path -Leaf $ScriptPath)" -ForegroundColor DarkGray
  Write-Host "    Log: $logPath" -ForegroundColor DarkGray

  # Run child scripts in a separate PowerShell process so validation scripts that call
  # `exit` do not terminate the orchestrator.
  Set-Content -Path $logPath -Value @() -Encoding utf8
  $exitCode = 0
  try {
    & $script:PowerShellExe -NoProfile -ExecutionPolicy Bypass -File $ScriptPath @Arguments 2>&1 | Tee-Object -FilePath $logPath -Append
    $exitCode = $LASTEXITCODE
  }
  catch {
    ($_ | Out-String) | Tee-Object -FilePath $logPath -Append | Out-Host
    if ($LASTEXITCODE -ne 0) {
      $exitCode = $LASTEXITCODE
    }
    else {
      $exitCode = 1
    }
  }

  if ($exitCode -eq 0) {
    Add-StepResult -Step $StepName -Status "Succeeded" -Notes "Log: $logPath"
    return $true
  }

  Add-StepResult -Step $StepName -Status "Failed" -Notes "Exit code $exitCode; Log: $logPath"
  Write-Warning "$StepName failed with exit code $exitCode. See log: $logPath"

  if (Get-YesNo "Continue to the next step?" $false) {
    return $false
  }

  throw "$StepName failed with exit code $exitCode."
}

$envPath = Resolve-EnvFilePath -RequestedPath $EnvFile
$envLines = Get-EnvLines -Path $envPath
$envLabel = if ([string]::IsNullOrWhiteSpace($envPath)) { "<none>" } else { Split-Path -Leaf $envPath }

if (-not $PSBoundParameters.ContainsKey("EmitSecrets")) {
  $emitSecretsFromEnv = Parse-EnvBool (Get-EnvValue -Key "EMIT_SECRETS" -Lines $envLines)
  if ($null -ne $emitSecretsFromEnv) {
    $EmitSecrets = [bool]$emitSecretsFromEnv
  }
}

$subscriptionId = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID") -Lines $envLines
$resourceGroup = Get-EnvValueFirst -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP") -Lines $envLines
$location = Get-EnvValueFirst -Keys @("AZURE_LOCATION", "AZURE_REGION", "LOCATION") -Lines $envLines
$hasPostgresCredential = -not [string]::IsNullOrWhiteSpace((Get-EnvValueFirst -Keys @("POSTGRES_DSN", "POSTGRES_ADMIN_PASSWORD") -Lines $envLines))

$script:PowerShellExe = Resolve-PowerShellExe
if (-not $script:PowerShellExe) {
  throw "Could not find a PowerShell executable. Install pwsh or powershell and retry."
}

$script:SessionLogDir = New-ProvisionLogDirectory
$script:StepResults = [System.Collections.Generic.List[object]]::new()

Write-Host "Azure Provisioning Orchestrator" -ForegroundColor Green
Write-Host "Environment file: $envLabel"
Write-Host "Subscription: $(if ($subscriptionId) { $subscriptionId } else { '<from child script defaults>' })"
Write-Host "Resource group: $(if ($resourceGroup) { $resourceGroup } else { '<from child script defaults>' })"
Write-Host "Location: $(if ($location) { $location } else { '<from child script defaults>' })"
Write-Host "Emit secrets: $([bool]$EmitSecrets)"
Write-Host "Session logs: $script:SessionLogDir"
Write-Host ""
Write-Host "This orchestrator wraps the existing Azure scripts and asks before each major step." -ForegroundColor DarkGray
Write-Host "The shared resource step still prompts for individual resources inside scripts/ops/provision/provision_azure.ps1." -ForegroundColor DarkGray

if (-not (Get-YesNo "Continue with this configuration?" $true)) {
  Add-StepResult -Step "Session" -Status "Cancelled" -Notes "User cancelled before execution."
  Write-Host ""
  Write-Host "No changes made." -ForegroundColor Yellow
  exit 0
}

$commonArgs = [System.Collections.Generic.List[string]]::new()
Add-StringArgument -Arguments $commonArgs -Name "EnvFile" -Value $envPath
Add-StringArgument -Arguments $commonArgs -Name "SubscriptionId" -Value $subscriptionId
Add-StringArgument -Arguments $commonArgs -Name "ResourceGroup" -Value $resourceGroup
Add-StringArgument -Arguments $commonArgs -Name "Location" -Value $location

if (Get-YesNo "Run preflight Azure permission validation?" $true) {
  $permissionArgs = [System.Collections.Generic.List[string]]::new()
  Add-StringArgument -Arguments $permissionArgs -Name "EnvFile" -Value $envPath
  Add-StringArgument -Arguments $permissionArgs -Name "SubscriptionId" -Value $subscriptionId
  Add-StringArgument -Arguments $permissionArgs -Name "ResourceGroup" -Value $resourceGroup
  Invoke-ChildScript `
    -StepName "Preflight Azure permission validation" `
    -ScriptPath (Join-Path $repoRoot "scripts\ops\validate\validate_azure_permissions.ps1") `
    -Arguments $permissionArgs.ToArray() | Out-Null
}
else {
  Add-StepResult -Step "Preflight Azure permission validation" -Status "Skipped"
}

if (Get-YesNo "Run shared Azure resource provisioning (resource group, storage, ACR, managed identity, Container Apps env)?" $true) {
  $sharedArgs = [System.Collections.Generic.List[string]]::new()
  foreach ($arg in $commonArgs) {
    $sharedArgs.Add($arg)
  }
  Add-SwitchArgument -Arguments $sharedArgs -Name "EmitSecrets" -Enabled ([bool]$EmitSecrets)
  Add-SwitchArgument -Arguments $sharedArgs -Name "SkipPostgresPrompt" -Enabled $true

  Invoke-ChildScript `
    -StepName "Shared Azure resource provisioning" `
    -ScriptPath (Join-Path $repoRoot "scripts\ops\provision\provision_azure.ps1") `
    -Arguments $sharedArgs.ToArray() | Out-Null
}
else {
  Add-StepResult -Step "Shared Azure resource provisioning" -Status "Skipped"
}

if (Get-YesNo "Provision or reconcile Entra OIDC applications and update the env file?" $true) {
  $entraArgs = [System.Collections.Generic.List[string]]::new()
  foreach ($arg in $commonArgs) {
    $entraArgs.Add($arg)
  }

  Invoke-ChildScript `
    -StepName "Entra OIDC provisioning" `
    -ScriptPath (Join-Path $repoRoot "scripts\ops\provision\provision_entra_oidc.ps1") `
    -Arguments $entraArgs.ToArray() | Out-Null
}
else {
  Add-StepResult -Step "Entra OIDC provisioning" -Status "Skipped"
}

if (Get-YesNo "Run dedicated Postgres provisioning?" $false) {
  $applyMigrations = Get-YesNo "Apply repo-owned Postgres migrations?" $true
  $resetBeforeMigrations = $true
  if ($applyMigrations) {
    $resetBeforeMigrations = Get-YesNo "Reset repo-owned Postgres objects before applying migrations?" $true
  }
  $createAppUsers = Get-YesNo "Create Postgres application users?" $false
  $useDockerPsql = $false
  if ($applyMigrations -or $createAppUsers) {
    $useDockerPsql = Get-YesNo "Use Dockerized psql instead of a local psql install?" $false
  }

  $postgresAdminPassword = ""
  if (($applyMigrations -or $createAppUsers) -and (-not $hasPostgresCredential)) {
    Write-Host ""
    Write-Warning "No POSTGRES_DSN or POSTGRES_ADMIN_PASSWORD was found in $envLabel."
    if (Get-YesNo "Enter a Postgres admin password now?" $true) {
      $postgresAdminPassword = Read-SecretText -Prompt "Postgres admin password"
    }
  }

  $postgresArgs = [System.Collections.Generic.List[string]]::new()
  foreach ($arg in $commonArgs) {
    $postgresArgs.Add($arg)
  }
  Add-SwitchArgument -Arguments $postgresArgs -Name "EmitSecrets" -Enabled ([bool]$EmitSecrets)
  Add-SwitchArgument -Arguments $postgresArgs -Name "ApplyMigrations" -Enabled $applyMigrations
  if (-not $resetBeforeMigrations) {
    $postgresArgs.Add('-ResetBeforeMigrations:$false')
  }
  Add-SwitchArgument -Arguments $postgresArgs -Name "CreateAppUsers" -Enabled $createAppUsers
  Add-SwitchArgument -Arguments $postgresArgs -Name "UseDockerPsql" -Enabled $useDockerPsql
  Add-StringArgument -Arguments $postgresArgs -Name "AdminPassword" -Value $postgresAdminPassword

  Invoke-ChildScript `
    -StepName "Dedicated Postgres provisioning" `
    -ScriptPath (Join-Path $repoRoot "scripts\ops\provision\provision_azure_postgres.ps1") `
    -Arguments $postgresArgs.ToArray() | Out-Null
}
else {
  Add-StepResult -Step "Dedicated Postgres provisioning" -Status "Skipped"
}

if (Get-YesNo "Preview and optionally deploy Azure cost guardrails?" $false) {
  $guardrailArgs = [System.Collections.Generic.List[string]]::new()
  if (-not [string]::IsNullOrWhiteSpace($location)) {
    Add-StringArgument -Arguments $guardrailArgs -Name "Location" -Value $location
  }
  if (-not [string]::IsNullOrWhiteSpace($resourceGroup)) {
    Add-StringArgument -Arguments $guardrailArgs -Name "ResourceGroupName" -Value $resourceGroup
  }

  if (Get-YesNo "Run cost guardrails WhatIf preview first?" $true) {
    $previewArgs = [System.Collections.Generic.List[string]]::new()
    foreach ($arg in $guardrailArgs) {
      $previewArgs.Add($arg)
    }
    $previewArgs.Add("-WhatIf")
    Invoke-ChildScript `
      -StepName "Cost guardrails preview" `
      -ScriptPath (Join-Path $repoRoot "scripts\ops\provision\configure_cost_guardrails.ps1") `
      -Arguments $previewArgs.ToArray() | Out-Null
  }
  else {
    Add-StepResult -Step "Cost guardrails preview" -Status "Skipped"
  }

  if (Get-YesNo "Apply Azure cost guardrails now?" $true) {
    Invoke-ChildScript `
      -StepName "Cost guardrails deployment" `
      -ScriptPath (Join-Path $repoRoot "scripts\ops\provision\configure_cost_guardrails.ps1") `
      -Arguments $guardrailArgs.ToArray() | Out-Null
  }
  else {
    Add-StepResult -Step "Cost guardrails deployment" -Status "Skipped"
  }
}
else {
  Add-StepResult -Step "Cost guardrails preview" -Status "Skipped"
  Add-StepResult -Step "Cost guardrails deployment" -Status "Skipped"
}

if ($envLabel -ieq ".env.web") {
  if (Get-YesNo "Sync .env.web values to GitHub vars/secrets now?" $false) {
    Invoke-ChildScript `
      -StepName "GitHub vars/secrets sync" `
      -ScriptPath (Join-Path $repoRoot "scripts\sync-all-to-github.ps1") | Out-Null
  }
  else {
    Add-StepResult -Step "GitHub vars/secrets sync" -Status "Skipped"
  }
}
else {
  Add-StepResult -Step "GitHub vars/secrets sync" -Status "Skipped" -Notes "Interactive sync only runs when the active env file is .env.web."
}

if (Get-YesNo "Run ACR pull validation after provisioning?" $true) {
  $acrValidationArgs = [System.Collections.Generic.List[string]]::new()
  Add-StringArgument -Arguments $acrValidationArgs -Name "EnvFile" -Value $envPath
  Add-StringArgument -Arguments $acrValidationArgs -Name "SubscriptionId" -Value $subscriptionId
  Add-StringArgument -Arguments $acrValidationArgs -Name "ResourceGroup" -Value $resourceGroup
  Invoke-ChildScript `
    -StepName "ACR pull validation" `
    -ScriptPath (Join-Path $repoRoot "scripts\ops\validate\validate_acr_pull.ps1") `
    -Arguments $acrValidationArgs.ToArray() | Out-Null
}
else {
  Add-StepResult -Step "ACR pull validation" -Status "Skipped"
}

if (Get-YesNo "Run final Azure permission validation?" $true) {
  $permissionArgs = [System.Collections.Generic.List[string]]::new()
  Add-StringArgument -Arguments $permissionArgs -Name "EnvFile" -Value $envPath
  Add-StringArgument -Arguments $permissionArgs -Name "SubscriptionId" -Value $subscriptionId
  Add-StringArgument -Arguments $permissionArgs -Name "ResourceGroup" -Value $resourceGroup
  Invoke-ChildScript `
    -StepName "Final Azure permission validation" `
    -ScriptPath (Join-Path $repoRoot "scripts\ops\validate\validate_azure_permissions.ps1") `
    -Arguments $permissionArgs.ToArray() | Out-Null
}
else {
  Add-StepResult -Step "Final Azure permission validation" -Status "Skipped"
}

Write-Host ""
Write-Host "Provisioning session summary:" -ForegroundColor Green
$script:StepResults | Format-Table -AutoSize
