[CmdletBinding()]
param(
  [string]$ResourceGroup,
  [string]$Subscription,
  [switch]$ListOnly
)

function Write-Log {
  param(
    [string]$Message,
    [string]$Level = "INFO"
  )
  $timestamp = (Get-Date).ToString("s")
  Write-Host ("[{0}] [{1}] {2}" -f $timestamp, $Level.ToUpper(), $Message)
}

function Read-DotEnv {
  param([string]$Path)
  $map = @{}
  if (-not (Test-Path $Path)) {
    return $map
  }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#")) {
      return
    }
    if ($line.StartsWith("export ")) {
      $line = $line.Substring(7).Trim()
    }
    $idx = $line.IndexOf("=")
    if ($idx -lt 1) {
      return
    }
    $key = $line.Substring(0, $idx).Trim()
    $value = $line.Substring($idx + 1).Trim()
    if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
      $value = $value.Substring(1, $value.Length - 2)
    }
    if ($key) {
      $map[$key] = $value
    }
  }
  return $map
}

function Get-FirstValue {
  param(
    [string[]]$Keys,
    [hashtable]$DotEnv
  )
  foreach ($key in $Keys) {
    $value = (Get-Item -Path "Env:$key" -ErrorAction SilentlyContinue).Value
    if ([string]::IsNullOrWhiteSpace($value) -and $DotEnv.ContainsKey($key)) {
      $value = $DotEnv[$key]
    }
    if (-not [string]::IsNullOrWhiteSpace($value)) {
      return $value
    }
  }
  return $null
}

function Invoke-AzCli {
  param([string[]]$Args)
  Write-Log ("az {0}" -f ($Args -join " "))
  $output = & az @Args
  if ($LASTEXITCODE -ne 0) {
    throw "Azure CLI failed: az $($Args -join ' ')"
  }
  return $output
}

function Convert-AzJson {
  param([string[]]$Output)
  $text = ($Output -join "`n").Trim()
  if (-not $text) {
    return $null
  }
  $match = [regex]::Match($text, "[\[{]")
  if (-not $match.Success) {
    $preview = if ($text.Length -gt 240) { $text.Substring(0, 240) + "..." } else { $text }
    throw "Azure CLI output did not contain JSON. Output preview: $preview"
  }
  $json = $text.Substring($match.Index)
  try {
    return $json | ConvertFrom-Json
  } catch {
    $preview = if ($json.Length -gt 240) { $json.Substring(0, 240) + "..." } else { $json }
    throw "Failed to parse Azure CLI JSON output. Preview: $preview"
  }
}

function Ensure-AzLogin {
  & az account show --only-show-errors 1>$null 2>$null
  if ($LASTEXITCODE -ne 0) {
    Write-Log "Not logged in to Azure CLI. Running 'az login'..."
    & az login 1>$null
    if ($LASTEXITCODE -ne 0) {
      throw "Azure login failed."
    }
  }
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

Write-Log "Starting Container App Job log fetcher."
Ensure-AzLogin

Write-Log "Configuring Azure CLI output suppression."
try {
  & az config set core.only_show_errors=true 1>$null 2>$null
  & az config set core.no_welcome=true 1>$null 2>$null
} catch {
  Write-Log "Failed to set Azure CLI config (continuing)." "WARN"
}

$envPath = Join-Path (Resolve-RepoRoot) ".env"
Write-Log ("Loading .env from {0}" -f $envPath)
$dotEnv = Read-DotEnv -Path $envPath

$listOnlyResolved = $true
if ($PSBoundParameters.ContainsKey("ListOnly")) {
  $listOnlyResolved = [bool]$ListOnly
} elseif ($PSBoundParameters.Count -gt 0) {
  $listOnlyResolved = $false
}

if ($Subscription) {
  Write-Log ("Setting Azure subscription from parameter: {0}" -f $Subscription)
  Invoke-AzCli @("account", "set", "--subscription", $Subscription, "--only-show-errors") | Out-Null
}

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = Get-FirstValue -Keys @(
    "AZURE_RESOURCE_GROUP",
    "RESOURCE_GROUP",
    "AZURE_RG",
    "CONTAINER_APP_RESOURCE_GROUP",
    "SYSTEM_HEALTH_ARM_RESOURCE_GROUP"
  ) -DotEnv $dotEnv
  if ($ResourceGroup) {
    Write-Log ("Resource group resolved from .env/env vars: {0}" -f $ResourceGroup)
  }
}

if ([string]::IsNullOrWhiteSpace($Subscription)) {
  $Subscription = Get-FirstValue -Keys @(
    "AZURE_SUBSCRIPTION_ID",
    "AZURE_SUBSCRIPTION",
    "SUBSCRIPTION_ID"
  ) -DotEnv $dotEnv
  if ($Subscription) {
    Write-Log ("Setting Azure subscription from .env/env vars: {0}" -f $Subscription)
    Invoke-AzCli @("account", "set", "--subscription", $Subscription, "--only-show-errors") | Out-Null
  }
}

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  Write-Log "Prompting for resource group."
  $ResourceGroup = Read-Host "Resource group name"
}

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  throw "Resource group is required."
}

Write-Log ("Listing Container App Jobs in resource group '{0}'" -f $ResourceGroup)
$jobs = @()
try {
  Write-Log "Fetching job names via TSV query."
  $jobsOutput = Invoke-AzCli @(
    "containerapp", "job", "list",
    "--resource-group", $ResourceGroup,
    "--query", "[].name",
    "-o", "tsv",
    "--only-show-errors"
  )
  $rawLines = $jobsOutput -split "\r?\n"
  $jobs = $rawLines | Where-Object { $_ -and $_.Trim() -match "^[A-Za-z0-9][A-Za-z0-9._-]*$" }
} catch {
  Write-Log "TSV job listing failed; falling back to JSON." "WARN"
}

if (-not $jobs -or $jobs.Count -eq 0) {
  try {
    Write-Log "Fetching job list as JSON (fallback)."
    $jobsJsonOutput = Invoke-AzCli @(
      "containerapp", "job", "list",
      "--resource-group", $ResourceGroup,
      "-o", "json"
    )
    Write-Log "Parsing jobs list JSON output."
    $jobsRaw = Convert-AzJson -Output $jobsJsonOutput
    if ($jobsRaw) {
      $jobs = @($jobsRaw | ForEach-Object { $_.name })
    }
  } catch {
    Write-Log ("Failed to list jobs: {0}" -f $_.Exception.Message) "WARN"
  }
}

if (-not $jobs -or $jobs.Count -eq 0) {
  Write-Host "No Container App Jobs found in resource group '$ResourceGroup'."
  exit 0
}

Write-Host "Container App Jobs:"
for ($i = 0; $i -lt $jobs.Count; $i++) {
  Write-Host ("[{0}] {1}" -f $i, $jobs[$i])
}

if ($listOnlyResolved) {
  Write-Log "List-only mode enabled. Exiting."
  exit 0
}

$index = -1
do {
  Write-Log "Prompting for job selection index."
  $selection = Read-Host "Select job index"
  $parsed = [int]::TryParse($selection, [ref]$index)
  if ($parsed -and $index -ge 0 -and $index -lt $jobs.Count) {
    break
  }
  Write-Host "Invalid selection. Enter a number between 0 and $($jobs.Count - 1)."
} while ($true)

$jobName = $jobs[$index]
Write-Log ("Selected job: {0}" -f $jobName)

Write-Log "Fetching executions for selected job."
$executionJson = Invoke-AzCli @(
  "containerapp", "job", "execution", "list",
  "--name", $jobName,
  "--resource-group", $ResourceGroup,
  "-o", "json",
  "--only-show-errors"
)

$executions = Convert-AzJson -Output $executionJson
if (-not $executions) {
  Write-Host "No executions found for job '$jobName'."
  exit 0
}

Write-Log "Selecting latest execution by timestamp."
$timeKeys = @("startTime", "startTimeUtc", "createdTime", "createdTimeUtc", "createdAt", "createdOn")
$latestExecution = $executions | Sort-Object -Descending -Property @{
  Expression = {
    foreach ($key in $timeKeys) {
      if ($_.PSObject.Properties[$key]) {
        $value = $_.$key
        if ($value) {
          try {
            return [DateTime]::Parse($value)
          } catch {
            Write-Log ("Failed to parse timestamp '{0}' for key '{1}'." -f $value, $key) "WARN"
          }
        }
      }
    }
    [DateTime]::MinValue
  }
} | Select-Object -First 1

$executionName = $latestExecution.name
if ([string]::IsNullOrWhiteSpace($executionName)) {
  throw "Could not resolve the latest execution name for job '$jobName'."
}

Write-Host "Latest execution: $executionName"

try {
  Write-Log "Fetching execution logs (primary flag)."
  $logOutput = Invoke-AzCli @(
    "containerapp", "job", "execution", "logs", "show",
    "--name", $jobName,
    "--resource-group", $ResourceGroup,
    "--execution", $executionName,
    "--only-show-errors"
  )
} catch {
  Write-Log "Primary logs command failed. Retrying with alternate flag." "WARN"
  $logOutput = Invoke-AzCli @(
    "containerapp", "job", "execution", "logs", "show",
    "--name", $jobName,
    "--resource-group", $ResourceGroup,
    "--execution-name", $executionName,
    "--only-show-errors"
  )
}

($logOutput -split "\r?\n") | ForEach-Object { Write-Host $_ }
Write-Log "Done."
