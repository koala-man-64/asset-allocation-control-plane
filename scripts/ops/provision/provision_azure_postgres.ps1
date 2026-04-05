param(
  # Note: some subscriptions are restricted from provisioning Postgres Flexible Server in certain regions (e.g., eastus).
  [string]$Location = "eastus",
  # If provisioning fails due to a restricted region, retry creation in these locations (in order).
  # Example: -Location "eastus" -LocationFallback @("eastus2","centralus","westus2")
  [string[]]$LocationFallback = @("eastus2", "centralus", "westus2"),
  [string]$SubscriptionId = "",
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$ServerName = "pg-asset-allocation",

  [string]$DatabaseName = "asset_allocation",
  [string]$AdminUser = "assetallocadmin",
  [string]$AdminPassword = "mysupersecretpassword1234$",

  [switch]$ApplyMigrations,
  [bool]$ResetBeforeMigrations = $true,
  [switch]$UseDockerPsql,
  [switch]$CreateAppUsers,
  [string]$BacktestServiceUser = "backtest_service",
  [string]$BacktestServicePassword = $AdminPassword,

  # Burstable SKUs (standard_b*) require `--tier Burstable`.
  [string]$SkuName = "standard_b1ms",
  [ValidateSet("", "Burstable", "GeneralPurpose", "MemoryOptimized")]
  [string]$Tier = "",
  [ValidateRange(32, 16384)]
  [int]$StorageSizeGiB = 32,
  [ValidateSet("14", "15", "16")]
  [string]$PostgresVersion = "16",

  # Cost-minimizing baseline: public endpoint enabled, restricted by firewall rules.
  # - "None" keeps public access but does not create any default firewall rule.
  # - Add firewall rules explicitly via -AllowAzureServices / -AllowIpRangeStart/-End.
  [ValidateSet("Disabled", "Enabled", "All", "None")]
  [string]$PublicAccess = "Enabled",

  [bool]$AllowAzureServices = $true,
  [string]$AllowIpRangeStart = "",
  [string]$AllowIpRangeEnd = "",
  [bool]$AllowCurrentClientIp = $true,

  [switch]$EmitSecrets,
  [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$script:CliBoundParams = @{} + $PSBoundParameters

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
  param([string]$RequestedPath)

  if (-not [string]::IsNullOrWhiteSpace($RequestedPath)) {
    $resolved = Resolve-Path $RequestedPath -ErrorAction Stop
    return $resolved.Path
  }

  $repoRoot = Resolve-RepoRoot
  $candidate = Join-Path $repoRoot ".env"
  if (Test-Path $candidate) {
    return (Resolve-Path $candidate -ErrorAction Stop).Path
  }

  return $null
}

function Get-EnvLines {
  param([string]$EnvPath)
  if ([string]::IsNullOrWhiteSpace($EnvPath) -or (-not (Test-Path $EnvPath))) {
    return @()
  }
  return ,@(Get-Content -Path $EnvPath)
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string[]]$Lines = @()
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
    [string[]]$Lines = @()
  )
  foreach ($key in $Keys) {
    $processValue = [Environment]::GetEnvironmentVariable($key)
    if (-not [string]::IsNullOrWhiteSpace($processValue)) {
      return $processValue
    }

    $envValue = Get-EnvValue -Key $key -Lines $Lines
    if (-not [string]::IsNullOrWhiteSpace($envValue)) {
      return $envValue
    }
  }
  return $null
}

function Set-EnvValues {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][hashtable]$Values
  )

  $lines = [System.Collections.Generic.List[string]]::new()
  if (Test-Path -LiteralPath $Path) {
    foreach ($line in (Get-Content -LiteralPath $Path)) {
      $lines.Add($line)
    }
  }

  foreach ($key in $Values.Keys) {
    $value = if ($null -eq $Values[$key]) { "" } else { [string]$Values[$key] }
    $updated = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
      if ($lines[$i] -match ("^" + [regex]::Escape($key) + "=")) {
        $lines[$i] = "$key=$value"
        $updated = $true
        break
      }
    }

    if (-not $updated) {
      $lines.Add("$key=$value")
    }
  }

  Set-Content -LiteralPath $Path -Value $lines -Encoding utf8
}

function Parse-EnvBool {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string]$Raw
  )

  if ([string]::IsNullOrWhiteSpace($Raw)) {
    return $null
  }

  $v = $Raw.Trim().ToLowerInvariant()
  if ($v -in @("1", "true", "yes", "y", "on")) { return $true }
  if ($v -in @("0", "false", "no", "n", "off")) { return $false }
  throw "Invalid boolean value for ${Key}: '$Raw'. Expected true/false."
}

function Try-ApplyStringEnvDefault {
  param(
    [Parameter(Mandatory = $true)][string]$ParamName,
    [Parameter(Mandatory = $true)][string[]]$Keys,
    [string]$CurrentValue,
    [string[]]$Lines = @()
  )

  if ($script:CliBoundParams.ContainsKey($ParamName)) {
    return $CurrentValue
  }

  $resolved = Get-EnvValueFirst -Keys $Keys -Lines $Lines
  if ([string]::IsNullOrWhiteSpace($resolved)) {
    return $CurrentValue
  }
  return $resolved.Trim()
}

function Try-ApplyBoolEnvDefault {
  param(
    [Parameter(Mandatory = $true)][string]$ParamName,
    [Parameter(Mandatory = $true)][string[]]$Keys,
    [bool]$CurrentValue,
    [string[]]$Lines = @()
  )

  if ($script:CliBoundParams.ContainsKey($ParamName)) {
    return $CurrentValue
  }

  foreach ($key in $Keys) {
    $raw = Get-EnvValueFirst -Keys @($key) -Lines $Lines
    if ([string]::IsNullOrWhiteSpace($raw)) { continue }
    $parsed = Parse-EnvBool -Key $key -Raw $raw
    if ($null -ne $parsed) { return [bool]$parsed }
  }

  return $CurrentValue
}

function Try-ApplyIntEnvDefault {
  param(
    [Parameter(Mandatory = $true)][string]$ParamName,
    [Parameter(Mandatory = $true)][string[]]$Keys,
    [int]$CurrentValue,
    [string[]]$Lines = @()
  )

  if ($script:CliBoundParams.ContainsKey($ParamName)) {
    return $CurrentValue
  }

  $resolved = Get-EnvValueFirst -Keys $Keys -Lines $Lines
  if ([string]::IsNullOrWhiteSpace($resolved)) {
    return $CurrentValue
  }
  $parsed = 0
  if (-not [int]::TryParse($resolved.Trim(), [ref]$parsed)) {
    throw "Invalid integer value for $($Keys[0]): '$resolved'."
  }
  return $parsed
}

function Parse-PostgresDsn {
  param([string]$Dsn)

  if ([string]::IsNullOrWhiteSpace($Dsn)) {
    return $null
  }

  try {
    $uri = [System.Uri]$Dsn
  }
  catch {
    return $null
  }

  if ($uri.Scheme -notin @("postgresql", "postgres")) {
    return $null
  }

  $user = ""
  $password = ""
  if (-not [string]::IsNullOrWhiteSpace($uri.UserInfo)) {
    $parts = $uri.UserInfo.Split(":", 2)
    if ($parts.Length -ge 1) {
      $user = [System.Uri]::UnescapeDataString($parts[0])
    }
    if ($parts.Length -ge 2) {
      $password = [System.Uri]::UnescapeDataString($parts[1])
    }
  }

  $database = $uri.AbsolutePath.Trim("/")
  $dbHost = $uri.Host
  $serverName = $dbHost
  if ($dbHost -match "^[^.]+\.postgres\.database\.azure\.com$") {
    $serverName = $dbHost.Split(".")[0]
  }

  return [pscustomobject]@{
    User       = $user
    Password   = $password
    Database   = $database
    Host       = $dbHost
    ServerName = $serverName
  }
}

$envPath = Resolve-EnvFilePath -RequestedPath $EnvFile
$envLines = Get-EnvLines -EnvPath $envPath
if ($envLines.Count -gt 0) {
  Write-Host "Loaded defaults from $(Split-Path -Leaf $envPath)"
}

$dsnFromEnv = Get-EnvValueFirst -Keys @("POSTGRES_DSN") -Lines $envLines
$parsedDsn = Parse-PostgresDsn -Dsn $dsnFromEnv
if ($null -ne $parsedDsn) {
  if (-not $script:CliBoundParams.ContainsKey("AdminUser") -and (-not [string]::IsNullOrWhiteSpace($parsedDsn.User))) {
    $AdminUser = $parsedDsn.User
  }
  if (-not $script:CliBoundParams.ContainsKey("AdminPassword") -and (-not [string]::IsNullOrWhiteSpace($parsedDsn.Password))) {
    $AdminPassword = $parsedDsn.Password
  }
  if (-not $script:CliBoundParams.ContainsKey("DatabaseName") -and (-not [string]::IsNullOrWhiteSpace($parsedDsn.Database))) {
    $DatabaseName = $parsedDsn.Database
  }
  if (-not $script:CliBoundParams.ContainsKey("ServerName") -and (-not [string]::IsNullOrWhiteSpace($parsedDsn.ServerName))) {
    $ServerName = $parsedDsn.ServerName
  }
}

$SubscriptionId = Try-ApplyStringEnvDefault -ParamName "SubscriptionId" -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID") -CurrentValue $SubscriptionId -Lines $envLines
$ResourceGroup = Try-ApplyStringEnvDefault -ParamName "ResourceGroup" -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP") -CurrentValue $ResourceGroup -Lines $envLines
$Location = Try-ApplyStringEnvDefault -ParamName "Location" -Keys @("AZURE_LOCATION", "AZURE_REGION", "LOCATION") -CurrentValue $Location -Lines $envLines
$ServerName = Try-ApplyStringEnvDefault -ParamName "ServerName" -Keys @("POSTGRES_SERVER_NAME") -CurrentValue $ServerName -Lines $envLines
$DatabaseName = Try-ApplyStringEnvDefault -ParamName "DatabaseName" -Keys @("POSTGRES_DATABASE_NAME") -CurrentValue $DatabaseName -Lines $envLines
$AdminUser = Try-ApplyStringEnvDefault -ParamName "AdminUser" -Keys @("POSTGRES_ADMIN_USER") -CurrentValue $AdminUser -Lines $envLines
$AdminPassword = Try-ApplyStringEnvDefault -ParamName "AdminPassword" -Keys @("POSTGRES_ADMIN_PASSWORD") -CurrentValue $AdminPassword -Lines $envLines
$BacktestServiceUser = Try-ApplyStringEnvDefault -ParamName "BacktestServiceUser" -Keys @("POSTGRES_BACKTEST_SERVICE_USER") -CurrentValue $BacktestServiceUser -Lines $envLines
$BacktestServicePassword = Try-ApplyStringEnvDefault -ParamName "BacktestServicePassword" -Keys @("POSTGRES_BACKTEST_SERVICE_PASSWORD") -CurrentValue $BacktestServicePassword -Lines $envLines
$SkuName = Try-ApplyStringEnvDefault -ParamName "SkuName" -Keys @("POSTGRES_SKU_NAME") -CurrentValue $SkuName -Lines $envLines
$Tier = Try-ApplyStringEnvDefault -ParamName "Tier" -Keys @("POSTGRES_TIER") -CurrentValue $Tier -Lines $envLines
$PostgresVersion = Try-ApplyStringEnvDefault -ParamName "PostgresVersion" -Keys @("POSTGRES_VERSION") -CurrentValue $PostgresVersion -Lines $envLines
$PublicAccess = Try-ApplyStringEnvDefault -ParamName "PublicAccess" -Keys @("POSTGRES_PUBLIC_ACCESS") -CurrentValue $PublicAccess -Lines $envLines
$AllowIpRangeStart = Try-ApplyStringEnvDefault -ParamName "AllowIpRangeStart" -Keys @("POSTGRES_ALLOW_IP_RANGE_START") -CurrentValue $AllowIpRangeStart -Lines $envLines
$AllowIpRangeEnd = Try-ApplyStringEnvDefault -ParamName "AllowIpRangeEnd" -Keys @("POSTGRES_ALLOW_IP_RANGE_END") -CurrentValue $AllowIpRangeEnd -Lines $envLines
$StorageSizeGiB = Try-ApplyIntEnvDefault -ParamName "StorageSizeGiB" -Keys @("POSTGRES_STORAGE_SIZE_GIB") -CurrentValue $StorageSizeGiB -Lines $envLines
$AllowAzureServices = Try-ApplyBoolEnvDefault -ParamName "AllowAzureServices" -Keys @("POSTGRES_ALLOW_AZURE_SERVICES") -CurrentValue $AllowAzureServices -Lines $envLines
$AllowCurrentClientIp = Try-ApplyBoolEnvDefault -ParamName "AllowCurrentClientIp" -Keys @("POSTGRES_ALLOW_CURRENT_CLIENT_IP") -CurrentValue $AllowCurrentClientIp -Lines $envLines
$ResetBeforeMigrations = Try-ApplyBoolEnvDefault -ParamName "ResetBeforeMigrations" -Keys @("POSTGRES_RESET_BEFORE_MIGRATIONS") -CurrentValue $ResetBeforeMigrations -Lines $envLines

function Get-PublicIp {
  try {
    $ip = (Invoke-WebRequest -Uri "https://api.ipify.org" -UseBasicParsing).Content.Trim()
    Write-Host "Detected public IP: $ip"
    return $ip
  }
  catch {
    Write-Warning "Failed to detect public IP: $_"
    return $null
  }
}


function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Invoke-Az {
  param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string[]]$Args
  )
  & az @Args
  if (-not $?) {
    throw "Azure CLI command failed: $Label"
  }
}

function Invoke-AzCapture {
  param(
    [Parameter(Mandatory = $true)][string]$Label,
    [Parameter(Mandatory = $true)][string[]]$Args
  )
  $output = & az @Args 2>&1
  return [pscustomobject]@{
    Label   = $Label
    Success = [bool]$?
    Output  = ($output | Out-String)
  }
}

function Test-AzRegionRestrictedError {
  param([string]$Text)
  if (-not $Text) { return $false }
  return (
    ($Text -match "location is restricted for provisioning of flexible servers") -or
    ($Text -match "Postgres Flexible Server provisioning is restricted") -or
    ($Text -match "Please try using another region")
  )
}

function Test-AzAlreadyExistsError {
  param([string]$Text)
  if (-not $Text) { return $false }
  return (
    ($Text -match "(?i)already exists") -or
    ($Text -match "(?i)resourcealreadyexists") -or
    ($Text -match "(?i)conflict")
  )
}

function Test-AzAuthorizationFailedError {
  param([string]$Text)
  if (-not $Text) { return $false }
  return (
    ($Text -match "(?i)authorizationfailed") -or
    ($Text -match "(?i)does not have authorization to perform action")
  )
}

function Get-AzProviderRegistrationState {
  param([Parameter(Mandatory = $true)][string]$Namespace)

  $providerShow = Invoke-AzCapture -Label "provider show $Namespace" -Args @(
    "provider", "show",
    "--namespace", $Namespace,
    "--query", "registrationState",
    "--only-show-errors",
    "-o", "tsv"
  )
  if (-not $providerShow.Success) {
    return $null
  }

  return $providerShow.Output.Trim()
}

function Ensure-AzProviderRegistered {
  param([Parameter(Mandatory = $true)][string]$Namespace)

  $registrationState = Get-AzProviderRegistrationState -Namespace $Namespace
  if ($registrationState -eq "Registered") {
    Write-Host "Provider already registered: $Namespace"
    return
  }

  $registerResult = Invoke-AzCapture -Label "provider register $Namespace" -Args @(
    "provider", "register",
    "--namespace", $Namespace,
    "--only-show-errors",
    "-o", "none"
  )
  if ($registerResult.Success) {
    return
  }

  if (Test-AzAuthorizationFailedError -Text $registerResult.Output) {
    $registrationState = Get-AzProviderRegistrationState -Namespace $Namespace
    if ($registrationState -eq "Registered") {
      Write-Warning "Azure CLI cannot register provider '$Namespace', but it is already registered. Continuing."
      return
    }
  }

  throw ("Azure CLI command failed: provider register $Namespace`n$($registerResult.Output)")
}

function New-RandomPassword {
  param([int]$Length = 32)
  $bytes = New-Object byte[] ($Length)
  [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
  # Base64 can include '+' and '/', which are allowed by Postgres password rules but can be awkward in shells.
  # Keep it URL-safe-ish and ensure we have multiple character classes.
  $raw = [Convert]::ToBase64String($bytes)
  $raw = $raw.Replace("+", "A").Replace("/", "b").Replace("=", "c")
  return $raw.Substring(0, [Math]::Min($raw.Length, $Length))
}

function Assert-PgIdentifier {
  param(
    [Parameter(Mandatory = $true)][string]$Value,
    [Parameter(Mandatory = $true)][string]$Label
  )
  $text = $Value
  if ($null -eq $text) { $text = "" }
  $text = $text.Trim()
  if (-not $text) { throw "$Label must be non-empty." }
  if ($text -notmatch '^[a-z][a-z0-9_]{0,62}$') {
    throw "$Label must match ^[a-z][a-z0-9_]{0,62}$ (got '$Value'). Use lowercase letters, digits, and underscores only."
  }
}

function ConvertTo-SqlLiteral {
  param([AllowNull()][string]$Value)

  if ($null -eq $Value) {
    return "NULL"
  }

  return "'" + $Value.Replace("'", "''") + "'"
}

function Invoke-Psql {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args
  )

  if ($UseDockerPsql) {
    Assert-CommandExists -Name "docker"
    $cmd = @("run", "--rm", "postgres:16-alpine", "psql") + $Args
    & docker @cmd
    if (-not $?) { throw "psql (docker) failed." }
    return
  }

  Assert-CommandExists -Name "psql"
  & psql @Args
  if (-not $?) { throw "psql failed." }
}

function Ensure-PostgresIndexes {
  param(
    [Parameter(Mandatory = $true)][string]$Dsn
  )

  $sql = @'
DO $$
BEGIN
  IF to_regclass('core.strategies') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_core_strategies_type ON core.strategies(type);
    CREATE INDEX IF NOT EXISTS idx_core_strategies_updated_at ON core.strategies(updated_at DESC);
  ELSIF to_regclass('platinum.strategies') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_platinum_strategies_type ON platinum.strategies(type);
    CREATE INDEX IF NOT EXISTS idx_platinum_strategies_updated_at ON platinum.strategies(updated_at DESC);
  ELSIF to_regclass('public.strategies') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_public_strategies_type ON public.strategies(type);
    CREATE INDEX IF NOT EXISTS idx_public_strategies_updated_at ON public.strategies(updated_at DESC);
  END IF;

  IF to_regclass('core.runs') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_core_runs_status_submitted_at
      ON core.runs(status, submitted_at DESC);
    CREATE INDEX IF NOT EXISTS idx_core_runs_completed_at
      ON core.runs(completed_at DESC);
  END IF;

  IF to_regclass('core.runtime_config') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_runtime_config_key
      ON core.runtime_config(key);

    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'core'
        AND table_name = 'runtime_config'
        AND column_name = 'enabled'
    ) THEN
      CREATE INDEX IF NOT EXISTS idx_runtime_config_scope_enabled
        ON core.runtime_config(scope, enabled);
    END IF;
  END IF;

  IF to_regclass('core.symbols') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_core_symbols_sector ON core.symbols(sector);
    CREATE INDEX IF NOT EXISTS idx_core_symbols_industry ON core.symbols(industry);
    CREATE INDEX IF NOT EXISTS idx_core_symbols_status ON core.symbols(status);
    CREATE INDEX IF NOT EXISTS idx_core_symbols_exchange ON core.symbols(exchange);
    CREATE INDEX IF NOT EXISTS idx_core_symbols_updated_at ON core.symbols(updated_at DESC);
  END IF;

  IF to_regclass('public.symbols') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_public_symbols_status ON public.symbols(status);
    CREATE INDEX IF NOT EXISTS idx_public_symbols_exchange ON public.symbols(exchange);
    CREATE INDEX IF NOT EXISTS idx_public_symbols_updated_at ON public.symbols(updated_at DESC);
    DROP INDEX IF EXISTS idx_public_symbols_source;
  END IF;

  IF to_regclass('gold.market_data') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_gold_market_data_symbol_date
      ON gold.market_data(symbol, date DESC);
    CREATE INDEX IF NOT EXISTS idx_gold_market_data_date_symbol
      ON gold.market_data(date DESC, symbol);
  END IF;

  IF to_regclass('gold.finance_data') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_gold_finance_data_symbol_date
      ON gold.finance_data(symbol, date DESC);
    CREATE INDEX IF NOT EXISTS idx_gold_finance_data_date_symbol
      ON gold.finance_data(date DESC, symbol);
  END IF;

  IF to_regclass('gold.earnings_data') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_gold_earnings_data_symbol_date
      ON gold.earnings_data(symbol, date DESC);
    CREATE INDEX IF NOT EXISTS idx_gold_earnings_data_date_symbol
      ON gold.earnings_data(date DESC, symbol);
  END IF;

  IF to_regclass('gold.price_target_data') IS NOT NULL THEN
    CREATE INDEX IF NOT EXISTS idx_gold_price_target_data_symbol_obs_date
      ON gold.price_target_data(symbol, obs_date DESC);
    CREATE INDEX IF NOT EXISTS idx_gold_price_target_data_obs_date_symbol
      ON gold.price_target_data(obs_date DESC, symbol);
  END IF;
END $$;
'@

  Invoke-Psql -Args @($Dsn, "-v", "ON_ERROR_STOP=1", "-c", $sql)
}

function Resolve-PostgresTier {
  param(
    [Parameter(Mandatory = $true)][string]$SkuName,
    [string]$TierOverride
  )
  if ($TierOverride) { return $TierOverride }

  $sku = $SkuName.ToLowerInvariant().Trim()
  if ($sku.StartsWith("standard_b")) { return "Burstable" }
  if ($sku.StartsWith("standard_d")) { return "GeneralPurpose" }
  if ($sku.StartsWith("standard_e")) { return "MemoryOptimized" }
  return "GeneralPurpose"
}

Assert-CommandExists -Name "az"

if (($ApplyMigrations -or $CreateAppUsers) -and (-not $UseDockerPsql)) {
  $hasLocalPsql = [bool](Get-Command "psql" -ErrorAction SilentlyContinue)
  if (-not $hasLocalPsql) {
    $hasDocker = [bool](Get-Command "docker" -ErrorAction SilentlyContinue)
    if ($hasDocker) {
      Write-Host "Local psql is not installed; falling back to Dockerized psql." -ForegroundColor Yellow
      $UseDockerPsql = $true
    }
    else {
      throw "Missing required command 'psql'. Install psql or Docker and retry."
    }
  }
}

Assert-PgIdentifier -Value $DatabaseName -Label "DatabaseName"
Assert-PgIdentifier -Value $BacktestServiceUser -Label "BacktestServiceUser"

$SkuName = $SkuName.ToLowerInvariant().Trim()
$selectedLocation = $Location

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = $env:AZURE_SUBSCRIPTION_ID
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = "eabd0bb1-8f36-4f27-ad86-8b33e02aaeb9"
}
Write-Host "Using subscription: $SubscriptionId"
Invoke-Az -Label "account set" -Args @("account", "set", "--subscription", $SubscriptionId, "--only-show-errors")

Write-Host "Ensuring required Azure resource providers are registered..."
$providers = @(
  "Microsoft.DBforPostgreSQL",
  "Microsoft.Network"
)
foreach ($p in $providers) {
  Ensure-AzProviderRegistered -Namespace $p
}

Write-Host "Ensuring resource group exists: $ResourceGroup ($Location)"
Invoke-Az -Label "group create" -Args @("group", "create", "--name", $ResourceGroup, "--location", $Location, "--only-show-errors", "-o", "none")

Write-Host "Ensuring Postgres Flexible Server exists: $ServerName"
$serverExists = $false
$serverShow = Invoke-AzCapture -Label "postgres flexible-server show" -Args @(
  "postgres", "flexible-server", "show",
  "--name", $ServerName,
  "--resource-group", $ResourceGroup,
  "--only-show-errors",
  "-o", "none"
)
if ($serverShow.Success) { $serverExists = $true }

if (-not $serverExists) {
  if (-not $AdminPassword) {
    $AdminPassword = New-RandomPassword -Length 32
  }

  $effectiveTier = Resolve-PostgresTier -SkuName $SkuName -TierOverride $Tier
  Write-Host "Resolved Postgres compute: sku=$SkuName tier=$effectiveTier (tierOverride='$Tier')"

  if ($SkuName.StartsWith("standard_b") -and ($effectiveTier -ne "Burstable")) {
    throw "Invalid tier '$effectiveTier' for Burstable SKU '$SkuName'. Use -Tier Burstable (or omit -Tier)."
  }

  $candidateLocations = @($Location) + $LocationFallback
  $candidateLocations = $candidateLocations |
  ForEach-Object { if ($null -eq $_) { "" } else { $_.Trim() } } |
  Where-Object { $_ } |
  Select-Object -Unique

  $created = $false
  foreach ($loc in $candidateLocations) {
    Write-Host "Attempting Postgres Flexible Server create in region: $loc (sku=$SkuName, tier=$effectiveTier)"
    $result = Invoke-AzCapture -Label "postgres flexible-server create ($loc)" -Args @(
      "postgres", "flexible-server", "create",
      "--name", $ServerName,
      "--resource-group", $ResourceGroup,
      "--location", $loc,
      "--version", $PostgresVersion,
      "--tier", $effectiveTier,
      "--sku-name", $SkuName,
      "--storage-size", "$StorageSizeGiB",
      "--admin-user", $AdminUser,
      "--admin-password", $AdminPassword,
      "--public-access", $PublicAccess,
      "--high-availability", "Disabled",
      "--backup-retention", "7",
      "--yes",
      "--only-show-errors",
      "-o", "none"
    )

    if ($result.Success) {
      $created = $true
      $selectedLocation = $loc
      break
    }

    if ((Test-AzRegionRestrictedError -Text $result.Output) -and ($loc -ne $candidateLocations[-1])) {
      Write-Host "Region '$loc' appears restricted for Postgres provisioning; retrying in next fallback region..."
      continue
    }

    throw ("Azure CLI command failed: $($result.Label)`n$($result.Output)")
  }

  if (-not $created) {
    throw "Failed to create Postgres Flexible Server '$ServerName' in any candidate region: $($candidateLocations -join ', ')"
  }
}
else {
  Write-Host "Server already exists; skipping create."
}

$requiresDbAdminAuth = [bool]($ApplyMigrations -or $CreateAppUsers)
if ($requiresDbAdminAuth -and (-not $AdminPassword)) {
  throw (
    "Server '$ServerName' already exists, but -AdminPassword was not provided. " +
    "Provide the existing admin password (or reset it via Azure) to run -ApplyMigrations/-CreateAppUsers."
  )
}

Write-Host "Ensuring database exists: $DatabaseName"
$dbShow = Invoke-AzCapture -Label "postgres flexible-server db show" -Args @(
  "postgres", "flexible-server", "db", "show",
  "--resource-group", $ResourceGroup,
  "--server-name", $ServerName,
  "--database-name", $DatabaseName,
  "--only-show-errors",
  "-o", "none"
)
if ($dbShow.Success) {
  Write-Host "Database already exists; skipping create."
}
else {
  $dbCreate = Invoke-AzCapture -Label "postgres flexible-server db create" -Args @(
    "postgres", "flexible-server", "db", "create",
    "--resource-group", $ResourceGroup,
    "--server-name", $ServerName,
    "--database-name", $DatabaseName,
    "--only-show-errors",
    "-o", "none"
  )
  if (-not $dbCreate.Success) {
    if (Test-AzAlreadyExistsError -Text $dbCreate.Output) {
      Write-Host "Database already exists; skipping create."
    }
    else {
      throw ("Azure CLI command failed: $($dbCreate.Label)`n$($dbCreate.Output)")
    }
  }
}

if ($AllowAzureServices) {
  Write-Host "Ensuring firewall rule allows Azure services (0.0.0.0)..."
  Invoke-Az -Label "postgres flexible-server firewall-rule create allow-azure-services" -Args @(
    "postgres", "flexible-server", "firewall-rule", "create",
    "--resource-group", $ResourceGroup,
    "--name", $ServerName,
    "--rule-name", "allow-azure-services",
    "--start-ip-address", "0.0.0.0",
    "--end-ip-address", "0.0.0.0",
    "--only-show-errors",
    "-o", "none"
  )
}

if ($AllowIpRangeStart) {
  $end = if ($AllowIpRangeEnd) { $AllowIpRangeEnd } else { $AllowIpRangeStart }
  Write-Host "Ensuring firewall rule allows IP range: $AllowIpRangeStart - $end"
  $fwShow = Invoke-AzCapture -Label "postgres flexible-server firewall-rule show allow-custom-ip-range" -Args @(
    "postgres", "flexible-server", "firewall-rule", "show",
    "--resource-group", $ResourceGroup,
    "--name", $ServerName,
    "--rule-name", "allow-custom-ip-range",
    "--only-show-errors",
    "-o", "none"
  )
  if (-not $fwShow.Success) {
    Invoke-Az -Label "postgres flexible-server firewall-rule create allow-custom-ip-range" -Args @(
      "postgres", "flexible-server", "firewall-rule", "create",
      "--resource-group", $ResourceGroup,
      "--name", $ServerName,
      "--rule-name", "allow-custom-ip-range",
      "--start-ip-address", $AllowIpRangeStart,
      "--end-ip-address", $end,
      "--only-show-errors",
      "-o", "none"
    )
  }
  else {
    Write-Host "Firewall rule already exists; skipping."
  }
}

if ($AllowCurrentClientIp) {
  $myIp = Get-PublicIp
  if ($myIp) {
    Write-Host "Ensuring firewall rule allows current client IP ($myIp)..."
    Invoke-Az -Label "postgres flexible-server firewall-rule create allow-current-client-ip" -Args @(
      "postgres", "flexible-server", "firewall-rule", "create",
      "--resource-group", $ResourceGroup,
      "--name", $ServerName,
      "--rule-name", "allow-current-client-ip",
      "--start-ip-address", $myIp,
      "--end-ip-address", $myIp,
      "--only-show-errors",
      "-o", "none"
    )
  }
}

$fqdn = & az postgres flexible-server show --name $ServerName --resource-group $ResourceGroup --only-show-errors --query fullyQualifiedDomainName -o tsv 2>$null
if ($null -eq $fqdn) { $fqdn = "" }
$fqdn = $fqdn.Trim()
if (-not $fqdn) {
  # Fallback that doesn't depend on `az` query output.
  $fqdn = "$ServerName.postgres.database.azure.com"
}

$adminDsn = ""
if ($AdminPassword) {
  $adminDsn = "postgresql://$AdminUser`:$AdminPassword@$fqdn`:5432/${DatabaseName}?sslmode=require"
}

if ($CreateAppUsers) {
  if (-not $BacktestServicePassword) { $BacktestServicePassword = New-RandomPassword -Length 32 }

  Write-Host "Creating least-privileged application roles..."

  $quotedBacktestServiceUser = ConvertTo-SqlLiteral -Value $BacktestServiceUser
  $quotedBacktestServicePassword = ConvertTo-SqlLiteral -Value $BacktestServicePassword

  $sqlTemplate = @'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {0}) THEN
    CREATE ROLE {1} LOGIN PASSWORD {2};
  END IF;
END
$$;

ALTER ROLE {1} WITH PASSWORD {2};

GRANT CONNECT ON DATABASE {3} TO {1};
'@
  $sql = $sqlTemplate -f $quotedBacktestServiceUser, $BacktestServiceUser, $quotedBacktestServicePassword, $DatabaseName

  Invoke-Psql -Args @($adminDsn, "-v", "ON_ERROR_STOP=1", "-c", $sql)
}

if ($ApplyMigrations) {
  $repoRoot = Resolve-RepoRoot
  $migrationsDir = (Resolve-Path (Join-Path $repoRoot "deploy/sql/postgres/migrations") -ErrorAction Stop).Path

  if ($ResetBeforeMigrations) {
    $resetScript = Join-Path $repoRoot "scripts\ops\data\reset_postgres_from_scratch.ps1"
    if (-not (Test-Path $resetScript)) {
      throw "Reset script not found at $resetScript"
    }

    Write-Host "Resetting existing database objects and reapplying repo-owned migrations..."
    & $resetScript -Dsn $adminDsn -MigrationsDir $migrationsDir -UseDockerPsql:$UseDockerPsql -Force
    if (-not $?) { throw "Database reset failed." }
  }
  else {
    Write-Host "Applying repo-owned migrations..."
    & (Join-Path $repoRoot "scripts\ops\data\apply_postgres_migrations.ps1") -Dsn $adminDsn -MigrationsDir $migrationsDir -UseDockerPsql:$UseDockerPsql
    if (-not $?) { throw "Migration apply failed." }
  }

  Write-Host "Ensuring supporting indexes..."
  Ensure-PostgresIndexes -Dsn $adminDsn

  Write-Host "Dropping previous migration ledger table..."
  Invoke-Psql -Args @($adminDsn, "-v", "ON_ERROR_STOP=1", "-c", "DROP TABLE IF EXISTS public.schema_migrations;")

  Write-Host "Verifying runtime-config debug-symbol schema state..."
  $verificationSql = @'
DO $$
BEGIN
  IF to_regclass('core.runtime_config') IS NULL THEN
    RAISE EXCEPTION 'Expected core.runtime_config to exist after migrations.';
  END IF;

  IF to_regclass('core.debug_symbols') IS NOT NULL THEN
    RAISE EXCEPTION 'Deprecated table core.debug_symbols still exists after migrations.';
  END IF;
END $$;
'@
  Invoke-Psql -Args @($adminDsn, "-v", "ON_ERROR_STOP=1", "-c", $verificationSql)
}
else {
  Write-Warning "Migrations were skipped. Runtime Python code no longer creates tables/schemas; run with -ApplyMigrations to provision DB objects."
}

$backtestServiceDsn = ""
if ($CreateAppUsers) {
  $backtestServiceDsn = "postgresql://$BacktestServiceUser`:$BacktestServicePassword@$fqdn`:5432/${DatabaseName}?sslmode=require"
}

$persistedDsn = ""
$persistedDsnSource = ""
if ($CreateAppUsers -and $ApplyMigrations -and (-not [string]::IsNullOrWhiteSpace($backtestServiceDsn))) {
  $persistedDsn = $backtestServiceDsn
  $persistedDsnSource = "backtest-service"
}
elseif (-not [string]::IsNullOrWhiteSpace($adminDsn)) {
  $persistedDsn = $adminDsn
  $persistedDsnSource = "admin"
}

$envUpdates = [ordered]@{
  POSTGRES_SERVER_NAME   = $ServerName
  POSTGRES_DATABASE_NAME = $DatabaseName
  POSTGRES_ADMIN_USER    = $AdminUser
}
if (-not [string]::IsNullOrWhiteSpace($AdminPassword)) {
  $envUpdates["POSTGRES_ADMIN_PASSWORD"] = $AdminPassword
}
if (-not [string]::IsNullOrWhiteSpace($persistedDsn)) {
  $envUpdates["POSTGRES_DSN"] = $persistedDsn
}

if ([string]::IsNullOrWhiteSpace($envPath)) {
  Write-Warning "EnvFile was not resolved, so Postgres outputs were not written back to an env file."
}
else {
  Set-EnvValues -Path $envPath -Values $envUpdates
  Write-Host "Updated env file with Postgres settings: $envPath" -ForegroundColor Green
  if (-not [string]::IsNullOrWhiteSpace($persistedDsnSource)) {
    Write-Host "POSTGRES_DSN source: $persistedDsnSource" -ForegroundColor DarkGray
  }
}

$outputs = [ordered]@{
  subscriptionId        = $SubscriptionId
  location              = $selectedLocation
  resourceGroup         = $ResourceGroup
  serverName            = $ServerName
  serverFqdn            = $fqdn
  databaseName          = $DatabaseName
  adminUser             = $AdminUser
  adminPassword         = if ($EmitSecrets) { $AdminPassword } else { "<redacted>" }
  envFile               = if ($envPath) { $envPath } else { "<not_written>" }
  persistedDsnSource    = if ($persistedDsnSource) { $persistedDsnSource } else { "<unavailable>" }
  resetBeforeMigrations = $ResetBeforeMigrations
  appUsers              = if ($CreateAppUsers) {
    [ordered]@{
      backtestServiceUser     = $BacktestServiceUser
      backtestServicePassword = if ($EmitSecrets) { $BacktestServicePassword } else { "<redacted>" }
    }
  }
  else {
    "<not_created>"
  }
  connectionStrings     = if ($EmitSecrets) {
    [ordered]@{
      adminDsn           = if ($adminDsn) { $adminDsn } else { "<unavailable>" }
      backtestServiceDsn = if ($backtestServiceDsn) { $backtestServiceDsn } else { "<not_created>" }
    }
  }
  else {
    "<redacted>"
  }
}

Write-Host ""
Write-Host "Provisioning complete. Outputs:"
$outputs | ConvertTo-Json -Depth 6
