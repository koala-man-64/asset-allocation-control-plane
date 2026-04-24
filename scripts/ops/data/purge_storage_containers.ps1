param(
  [string]$EnvFile = "",
  [string]$StorageAccountName = "",
  [string]$ConnectionString = "",
  [string[]]$Containers = @(),
  [string[]]$ExcludeContainers = @(),
  [switch]$ExcludeCommon,
  [switch]$IncludeCommon,
  [switch]$AllContainers,
  [switch]$DryRun,
  [switch]$Force
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Strip-Quotes {
  param([string]$Value)
  $v = if ($null -eq $Value) { "" } else { $Value.Trim() }
  if (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'"))) {
    return $v.Substring(1, $v.Length - 2)
  }
  return $v
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

Assert-CommandExists -Name "az"

$repoRoot = Resolve-RepoRoot

if ([string]::IsNullOrWhiteSpace($EnvFile)) {
  $candidateWeb = Join-Path $repoRoot ".env.web"
  $candidateEnv = Join-Path $repoRoot ".env"

  if (Test-Path $candidateWeb) {
    $EnvFile = $candidateWeb
  }
  elseif (Test-Path $candidateEnv) {
    $EnvFile = $candidateEnv
  }
  else {
    throw "Env file not found. Create '.env' (recommended) or '.env.web', or pass -EnvFile."
  }
}

if (-not (Test-Path $EnvFile)) {
  throw "Env file not found at '$EnvFile'."
}

$envLabel = Split-Path -Leaf $EnvFile
$envLines = Get-Content $EnvFile
Write-Host "Loaded configuration from $envLabel" -ForegroundColor Cyan

function Get-EnvValue {
  param([Parameter(Mandatory = $true)][string]$Key)
  foreach ($line in $envLines) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) { continue }
    if ($trimmed -match ("^" + [regex]::Escape($Key) + "=(.*)$")) {
      return (Strip-Quotes $matches[1])
    }
  }
  return $null
}

if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
  $StorageAccountName = Get-EnvValue -Key "AZURE_STORAGE_ACCOUNT_NAME"
}

if ([string]::IsNullOrWhiteSpace($ConnectionString)) {
  $ConnectionString = Get-EnvValue -Key "AZURE_STORAGE_CONNECTION_STRING"
}

if ([string]::IsNullOrWhiteSpace($StorageAccountName) -and [string]::IsNullOrWhiteSpace($ConnectionString)) {
  throw "Storage auth not configured. Set AZURE_STORAGE_ACCOUNT_NAME (and login with az) or AZURE_STORAGE_CONNECTION_STRING in $envLabel (or pass params)."
}

function Invoke-AzStorage {
  param([Parameter(Mandatory = $true)][string[]]$Args)

  # Ensure we never echo secrets to the terminal.
  if ($DryRun) {
    $safeArgs = @($Args)
    if ($ConnectionString) {
      for ($i = 0; $i -lt $safeArgs.Count; $i++) {
        if ($safeArgs[$i] -eq "--connection-string" -and $i -lt ($safeArgs.Count - 1)) {
          $safeArgs[$i + 1] = "[REDACTED]"
        }
      }
    }
    Write-Host "[DRY RUN] az $($safeArgs -join ' ')" -ForegroundColor Yellow
    return $true
  }

  & az @Args
  return ($LASTEXITCODE -eq 0)
}

function Invoke-AzStorageResult {
  param([Parameter(Mandatory = $true)][string[]]$Args)

  # Ensure we never echo secrets to the terminal.
  if ($DryRun) {
    Invoke-AzStorage -Args $Args | Out-Null
    return @{ Ok = $true; Output = "" }
  }

  $out = & az @Args 2>&1
  $ok = ($LASTEXITCODE -eq 0)

  $text = ($out | Out-String)
  if ($ConnectionString) {
    $text = $text.Replace($ConnectionString, "[REDACTED]")
  }

  return @{ Ok = $ok; Output = $text }
}

if ($AllContainers) {
  Write-Host "Listing containers in storage account..." -ForegroundColor Cyan
  $listArgs = @("storage", "container", "list", "--query", "[].name", "-o", "tsv", "--only-show-errors")
  if ($ConnectionString) {
    $listArgs += @("--connection-string", $ConnectionString)
  }
  else {
    $listArgs += @("--account-name", $StorageAccountName, "--auth-mode", "login")
  }

  $names = & az @listArgs
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to list containers. Ensure you are logged in (az login) and have Storage Blob Data permissions, or set AZURE_STORAGE_CONNECTION_STRING."
  }
  $Containers = ($names -split "`r?`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ }) | Sort-Object -Unique
}
elseif ($Containers.Count -eq 0) {
  # Default to canonical container env vars if not explicitly provided.
  $found = @()
  foreach ($line in $envLines) {
    if ($line -match "^AZURE_CONTAINER_[^=]+=(.*)$") {
      $val = Strip-Quotes $matches[1]
      if ($val) { $found += $val }
    }
  }
  if ($found.Count -gt 0) {
    $Containers = $found | Sort-Object -Unique
  }
  else {
    $Containers = @("common", "bronze", "silver", "gold", "platinum")
  }
}

$Containers = $Containers | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Sort-Object -Unique

# Apply exclusions (e.g., keep the 'common' container intact).
$excluded = New-Object System.Collections.Generic.HashSet[string] ([System.StringComparer]::OrdinalIgnoreCase)
foreach ($c in $ExcludeContainers) {
  $name = if ($null -eq $c) { "" } else { $c.Trim() }
  if ($name) { [void]$excluded.Add($name) }
}

if ($ExcludeCommon -or (-not $IncludeCommon)) {
  $commonName = Get-EnvValue -Key "AZURE_CONTAINER_COMMON"
  if ([string]::IsNullOrWhiteSpace($commonName)) { $commonName = "common" }
  [void]$excluded.Add($commonName)
}

if ($excluded.Count -gt 0) {
  $Containers = $Containers | Where-Object { -not $excluded.Contains($_) } | Sort-Object -Unique
}

if ($Containers.Count -eq 0) {
  Write-Warning "No containers resolved; nothing to purge."
  exit 0
}

$acctLabel = if ($StorageAccountName) { $StorageAccountName } else { "<from connection string>" }
Write-Host ""
Write-Host "Storage account: $acctLabel" -ForegroundColor Cyan
Write-Host "Containers to purge (containers will be deleted + recreated):" -ForegroundColor Cyan
$Containers | ForEach-Object { Write-Host " - $_" }
if ($excluded.Count -gt 0) {
  Write-Host "Excluded containers:" -ForegroundColor Cyan
  ($excluded | Sort-Object) | ForEach-Object { Write-Host " - $_" }
}

if (-not $Force) {
  Write-Host ""
  Write-Host "WARNING: This will DELETE and RECREATE the listed containers." -ForegroundColor Yellow
  Write-Host "Any container-level settings (metadata, access policies) may be lost." -ForegroundColor Yellow
  $confirm = Read-Host "Type 'purge' to continue"
  if ($confirm -ne "purge") {
    Write-Host "Aborted." -ForegroundColor Yellow
    exit 0
  }
}

Write-Host ""

function Wait-UntilContainerCreateAllowed {
  param([Parameter(Mandatory = $true)][string]$Name)

  if ($DryRun) {
    $createArgs = @("storage", "container", "create", "--name", $Name, "--public-access", "off", "--fail-on-exist", "--only-show-errors")
    if ($ConnectionString) {
      $createArgs += @("--connection-string", $ConnectionString)
    }
    else {
      $createArgs += @("--account-name", $StorageAccountName, "--auth-mode", "login")
    }
    Invoke-AzStorage -Args $createArgs | Out-Null
    return
  }

  $maxAttempts = 600
  for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    $createArgs = @("storage", "container", "create", "--name", $Name, "--public-access", "off", "--fail-on-exist", "--only-show-errors")
    if ($ConnectionString) {
      $createArgs += @("--connection-string", $ConnectionString)
    }
    else {
      $createArgs += @("--account-name", $StorageAccountName, "--auth-mode", "login")
    }

    $result = Invoke-AzStorageResult -Args $createArgs
    if ($result.Ok) { return }

    $msg = $result.Output
    if ($msg -match "ContainerBeingDeleted" -or $msg -match "specified container is being deleted") {
      if ($attempt -eq 1 -or ($attempt % 10 -eq 0)) {
        Write-Host "Container '$Name' is being deleted; waiting before recreate (attempt $attempt/$maxAttempts)..." -ForegroundColor Yellow
      }
      Start-Sleep -Seconds 3
      continue
    }

    if ($msg -match "ContainerAlreadyExists" -or $msg -match "specified container already exists") {
      if ($attempt -eq 1 -or ($attempt % 10 -eq 0)) {
        Write-Host "Container '$Name' still exists; waiting before recreate (attempt $attempt/$maxAttempts)..." -ForegroundColor Yellow
      }
      Start-Sleep -Seconds 2
      continue
    }

    throw "Failed to recreate container '$Name': $msg"
  }

  throw "Timed out waiting to recreate container '$Name'."
}

foreach ($container in $Containers) {
  Write-Host "Purging container: $container" -ForegroundColor Yellow

  $deleteArgs = @("storage", "container", "delete", "--name", $container, "--only-show-errors")
  if ($ConnectionString) {
    $deleteArgs += @("--connection-string", $ConnectionString)
  }
  else {
    $deleteArgs += @("--account-name", $StorageAccountName, "--auth-mode", "login")
  }

  $shouldRecreate = $true
  $deleteResult = Invoke-AzStorageResult -Args $deleteArgs
  if ($deleteResult.Ok) {
    Write-Host "Delete requested for $container" -ForegroundColor Gray
  }
  else {
    $msg = $deleteResult.Output
    if ($msg -match "ContainerBeingDeleted" -or $msg -match "specified container is being deleted") {
      Write-Host "Container '$container' is already being deleted; waiting to recreate..." -ForegroundColor Yellow
    }
    elseif ($msg -match "ContainerNotFound" -or $msg -match "specified container does not exist") {
      Write-Host "Container '$container' does not exist; will create it." -ForegroundColor Gray
    }
    else {
      Write-Host "Failed to delete '$container' (not retrying create):" -ForegroundColor Yellow
      Write-Host $msg -ForegroundColor Yellow
      $shouldRecreate = $false
    }
  }

  if ($shouldRecreate) {
    try {
      Wait-UntilContainerCreateAllowed -Name $container
      Write-Host "Recreated $container" -ForegroundColor Green
    }
    catch {
      Write-Host $_ -ForegroundColor Yellow
    }
  }
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
