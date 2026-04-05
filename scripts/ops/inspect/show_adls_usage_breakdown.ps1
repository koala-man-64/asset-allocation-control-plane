param(
  [string]$EnvFile = "",
  [string]$StorageAccountName = "",
  [string]$ConnectionString = "",
  [string]$StorageAccountKey = "",
  [string]$SasToken = "",
  [string[]]$FileSystems = @(),
  [switch]$AllFileSystems,
  [ValidateRange(1, 32)][int]$MaxDepth = 3,
  [ValidateRange(0, 500)][int]$TopPerLevel = 25
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

  throw "Env file not found. Create '.env' (recommended) or '.env.web', or pass -EnvFile."
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
      return Strip-Quotes -Value $matches[1]
    }
  }

  return $null
}

function Get-EnvValuesByPrefix {
  param(
    [Parameter(Mandatory = $true)][string]$Prefix,
    [string[]]$Lines
  )

  $values = @()
  foreach ($line in $Lines) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) { continue }
    if ($trimmed -match ("^" + [regex]::Escape($Prefix) + "[^=]+=(.*)$")) {
      $value = Strip-Quotes -Value $matches[1]
      if (-not [string]::IsNullOrWhiteSpace($value)) {
        $values += $value
      }
    }
  }

  return @($values | Sort-Object -Unique)
}

function Parse-AccountNameFromConnectionString {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return ""
  }

  $match = [regex]::Match($Value, "(?:^|;)AccountName=([^;]+)", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if ($match.Success) {
    return $match.Groups[1].Value.Trim()
  }

  return ""
}

function Get-AuthArgs {
  param(
    [string]$StorageAccountName,
    [string]$ConnectionString,
    [string]$StorageAccountKey,
    [string]$SasToken
  )

  $args = @()

  if (-not [string]::IsNullOrWhiteSpace($ConnectionString)) {
    $args += @("--connection-string", $ConnectionString)
    return $args
  }

  if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
    throw "Storage account name is required when connection string is not provided."
  }

  $args += @("--account-name", $StorageAccountName)

  if (-not [string]::IsNullOrWhiteSpace($StorageAccountKey)) {
    $args += @("--account-key", $StorageAccountKey)
    return $args
  }

  if (-not [string]::IsNullOrWhiteSpace($SasToken)) {
    $args += @("--sas-token", $SasToken)
    return $args
  }

  $args += @("--auth-mode", "login")
  return $args
}

function Protect-Text {
  param(
    [string]$Text,
    [string[]]$Secrets
  )

  $sanitized = if ($null -eq $Text) { "" } else { $Text }
  foreach ($secret in $Secrets) {
    if (-not [string]::IsNullOrWhiteSpace($secret)) {
      $sanitized = $sanitized.Replace($secret, "[REDACTED]")
    }
  }

  return $sanitized
}

function Invoke-AzJson {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args,
    [string[]]$Secrets,
    [switch]$AllowFailure
  )

  $output = & az @Args 2>&1
  $ok = ($LASTEXITCODE -eq 0)

  $text = ($output | Out-String)
  $safeText = Protect-Text -Text $text -Secrets $Secrets

  if (-not $ok) {
    if ($AllowFailure) {
      return @{ Ok = $false; Output = $safeText; Data = $null }
    }

    throw "Azure CLI command failed.`n$safeText"
  }

  if ([string]::IsNullOrWhiteSpace($safeText)) {
    return @{ Ok = $true; Output = $safeText; Data = @() }
  }

  try {
    $json = $safeText | ConvertFrom-Json
    return @{ Ok = $true; Output = $safeText; Data = $json }
  }
  catch {
    throw "Failed to parse Azure CLI JSON output. $($_.Exception.Message)"
  }
}

function New-UsageNode {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][AllowEmptyString()][string]$Path,
    [Parameter(Mandatory = $true)][int]$Depth
  )

  return [PSCustomObject]@{
    Name     = $Name
    Path     = $Path
    Depth    = $Depth
    Bytes    = [int64]0
    Children = @{}
  }
}

function Ensure-ChildNode {
  param(
    [Parameter(Mandatory = $true)]$Parent,
    [Parameter(Mandatory = $true)][string]$ChildName
  )

  if ($Parent.Children.ContainsKey($ChildName)) {
    return $Parent.Children[$ChildName]
  }

  $childPath = if ([string]::IsNullOrWhiteSpace($Parent.Path)) {
    $ChildName
  }
  else {
    "$($Parent.Path)/$ChildName"
  }

  $child = New-UsageNode -Name $ChildName -Path $childPath -Depth ($Parent.Depth + 1)
  $Parent.Children[$ChildName] = $child
  return $child
}

function Test-DateLikeSegment {
  param([string]$Segment)

  if ([string]::IsNullOrWhiteSpace($Segment)) {
    return $false
  }

  $value = $Segment.Trim()

  if ($value -match '^(?i)(date|dt|day)=\d{4}-\d{2}-\d{2}$') { return $true }
  if ($value -match '^(?i)(date|dt|day)=\d{8}$') { return $true }
  if ($value -match '^(?i)(year|yr)=\d{4}$') { return $true }
  if ($value -match '^(?i)(month|mon)=\d{1,2}$') { return $true }
  if ($value -match '^(?i)(day|dd)=\d{1,2}$') { return $true }
  if ($value -match '^\d{4}-\d{2}-\d{2}$') { return $true }
  if ($value -match '^\d{8}$') { return $true }
  if ($value -match '^\d{4}-\d{2}$') { return $true }

  return $false
}

function Test-SymbolLikeSegment {
  param([string]$Segment)

  if ([string]::IsNullOrWhiteSpace($Segment)) {
    return $false
  }

  $value = $Segment.Trim()

  if ($value -match '^(?i)(symbol|ticker|sym)=[^=]+$') {
    return $true
  }

  if (($value -match '^[A-Z0-9][A-Z0-9._-]{0,9}$') -and ($value -match '[A-Z]')) {
    return $true
  }

  return $false
}

function Normalize-DirectorySegment {
  param([string]$Segment)

  if (Test-DateLikeSegment -Segment $Segment) {
    return "[dates]"
  }

  if (Test-SymbolLikeSegment -Segment $Segment) {
    return "[symbols]"
  }

  return $Segment
}

function Normalize-DirectorySegments {
  param([string[]]$Segments)

  $normalized = New-Object System.Collections.Generic.List[string]

  foreach ($segment in $Segments) {
    if ([string]::IsNullOrWhiteSpace($segment)) {
      continue
    }

    $bucketed = Normalize-DirectorySegment -Segment $segment
    $isGroupedBucket = ($bucketed -eq "[dates]") -or ($bucketed -eq "[symbols]")

    if ($isGroupedBucket -and $normalized.Count -gt 0 -and $normalized[$normalized.Count - 1] -eq $bucketed) {
      continue
    }

    $normalized.Add($bucketed)
  }

  return @($normalized.ToArray())
}

function Format-Bytes {
  param([Int64]$Bytes)

  if ($Bytes -lt 0) {
    return "0 B"
  }

  $units = @("B", "KB", "MB", "GB", "TB", "PB")
  $size = [double]$Bytes
  $unitIndex = 0

  while ($size -ge 1024 -and $unitIndex -lt ($units.Count - 1)) {
    $size = $size / 1024
    $unitIndex++
  }

  if ($unitIndex -eq 0) {
    return "{0} {1}" -f [int64]$size, $units[$unitIndex]
  }

  return "{0:N2} {1}" -f $size, $units[$unitIndex]
}

function Add-NodeRows {
  param(
    [Parameter(Mandatory = $true)]$Node,
    [Parameter(Mandatory = $true)][Int64]$TotalBytes,
    [Parameter(Mandatory = $true)][int]$TopPerLevel,
    [Parameter(Mandatory = $true)]$Rows,
    [int]$IndentLevel = 0
  )

  $indent = if ($IndentLevel -le 0) { "" } else { ("  " * $IndentLevel) }
  $label = if ($IndentLevel -eq 0) { $Node.Name } else { "$indent|- $($Node.Name)" }
  $percentOfTotal = if ($TotalBytes -gt 0) { ([double]$Node.Bytes / [double]$TotalBytes) * 100.0 } else { 0.0 }

  $Rows.Add([PSCustomObject]@{
      Path           = $label
      Bytes          = [int64]$Node.Bytes
      HumanReadable  = Format-Bytes -Bytes $Node.Bytes
      PercentOfTotal = $percentOfTotal
    })

  $children = @($Node.Children.Values | Sort-Object -Property Bytes -Descending)
  if ($children.Count -eq 0) {
    return
  }

  $visible = $children
  $hidden = @()

  if ($TopPerLevel -gt 0 -and $children.Count -gt $TopPerLevel) {
    $visible = @($children | Select-Object -First $TopPerLevel)
    $hidden = @($children | Select-Object -Skip $TopPerLevel)
  }

  foreach ($child in $visible) {
    Add-NodeRows -Node $child -TotalBytes $TotalBytes -TopPerLevel $TopPerLevel -Rows $Rows -IndentLevel ($IndentLevel + 1)
  }

  if ($hidden.Count -gt 0) {
    $hiddenBytes = [int64](($hidden | Measure-Object -Property Bytes -Sum).Sum)
    $otherLabel = "{0}|- [other {1} item(s)]" -f ("  " * ($IndentLevel + 1)), $hidden.Count
    $otherPercent = if ($TotalBytes -gt 0) { ([double]$hiddenBytes / [double]$TotalBytes) * 100.0 } else { 0.0 }

    $Rows.Add([PSCustomObject]@{
        Path           = $otherLabel
        Bytes          = $hiddenBytes
        HumanReadable  = Format-Bytes -Bytes $hiddenBytes
        PercentOfTotal = $otherPercent
      })
  }
}

Assert-CommandExists -Name "az"

$resolvedEnvFile = Resolve-EnvFilePath -EnvFileOverride $EnvFile
$envLines = Get-Content $resolvedEnvFile
$envLabel = Split-Path -Leaf $resolvedEnvFile

if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
  $StorageAccountName = Get-EnvValue -Key "AZURE_STORAGE_ACCOUNT_NAME" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($ConnectionString)) {
  $ConnectionString = Get-EnvValue -Key "AZURE_STORAGE_CONNECTION_STRING" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($StorageAccountKey)) {
  $StorageAccountKey = Get-EnvValue -Key "AZURE_STORAGE_ACCOUNT_KEY" -Lines $envLines
  if ([string]::IsNullOrWhiteSpace($StorageAccountKey)) {
    $StorageAccountKey = Get-EnvValue -Key "AZURE_STORAGE_ACCESS_KEY" -Lines $envLines
  }
}
if ([string]::IsNullOrWhiteSpace($SasToken)) {
  $SasToken = Get-EnvValue -Key "AZURE_STORAGE_SAS_TOKEN" -Lines $envLines
}

if ([string]::IsNullOrWhiteSpace($StorageAccountName)) {
  $StorageAccountName = Parse-AccountNameFromConnectionString -Value $ConnectionString
}

if ([string]::IsNullOrWhiteSpace($StorageAccountName) -and [string]::IsNullOrWhiteSpace($ConnectionString)) {
  throw "Storage auth not configured. Set AZURE_STORAGE_ACCOUNT_NAME and use az login, or set AZURE_STORAGE_CONNECTION_STRING in $envLabel (or pass params)."
}

$secrets = @($ConnectionString, $StorageAccountKey, $SasToken) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
$authArgs = Get-AuthArgs -StorageAccountName $StorageAccountName -ConnectionString $ConnectionString -StorageAccountKey $StorageAccountKey -SasToken $SasToken

if ($FileSystems.Count -eq 0) {
  if ($AllFileSystems) {
    $listFsArgs = @(
      "storage", "container", "list",
      "--query", "[].name",
      "-o", "json",
      "--only-show-errors"
    ) + $authArgs

    $listFsResult = Invoke-AzJson -Args $listFsArgs -Secrets $secrets
    $FileSystems = @($listFsResult.Data)
  }
  else {
    $FileSystems = Get-EnvValuesByPrefix -Prefix "AZURE_CONTAINER_" -Lines $envLines
    if ($FileSystems.Count -eq 0) {
      $listFsArgs = @(
        "storage", "container", "list",
        "--query", "[].name",
        "-o", "json",
        "--only-show-errors"
      ) + $authArgs

      $listFsResult = Invoke-AzJson -Args $listFsArgs -Secrets $secrets
      $FileSystems = @($listFsResult.Data)
    }
  }
}

$FileSystems = @($FileSystems | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ } | Sort-Object -Unique)
if ($FileSystems.Count -eq 0) {
  throw "No containers/filesystems resolved. Pass -FileSystems or set AZURE_CONTAINER_* variables in $envLabel."
}

$accountLabel = if ([string]::IsNullOrWhiteSpace($StorageAccountName)) { "<from connection string>" } else { $StorageAccountName }
Write-Host "Loaded configuration from $envLabel" -ForegroundColor Cyan
Write-Host "Storage account: $accountLabel" -ForegroundColor Cyan
Write-Host "File systems: $($FileSystems -join ', ')" -ForegroundColor Cyan
Write-Host "View: container-level usage only" -ForegroundColor Cyan
Write-Host ""

$accountNode = New-UsageNode -Name "/" -Path "" -Depth 0

foreach ($fileSystem in $FileSystems) {
  Write-Host "Scanning file system: $fileSystem" -ForegroundColor Yellow

  $listBlobsArgs = @(
    "storage", "blob", "list",
    "--container-name", $fileSystem,
    "--num-results", "*",
    "--query", "[].{name:name,size:properties.contentLength}",
    "-o", "json",
    "--only-show-errors"
  ) + $authArgs

  $blobResult = Invoke-AzJson -Args $listBlobsArgs -Secrets $secrets -AllowFailure
  if (-not $blobResult.Ok) {
    Write-Warning "Skipping '$fileSystem'. Failed to list blobs: $($blobResult.Output.Trim())"
    continue
  }

  $fileSystemNode = New-UsageNode -Name $fileSystem -Path $fileSystem -Depth 1
  $accountNode.Children[$fileSystem] = $fileSystemNode

  $blobs = @($blobResult.Data)
  foreach ($blob in $blobs) {
    $name = [string]$blob.name
    if ([string]::IsNullOrWhiteSpace($name)) {
      continue
    }

    $size = [int64]0
    if ($null -ne $blob.size) {
      $size = [int64]$blob.size
    }

    $fileSystemNode.Bytes += $size
    $accountNode.Bytes += $size
  }
}

if ($accountNode.Bytes -le 0) {
  Write-Warning "No data found in the selected file systems."
  exit 0
}

$rows = @(
  $accountNode.Children.Values |
    Sort-Object -Property Bytes -Descending |
    ForEach-Object {
      $percentOfTotal = if ($accountNode.Bytes -gt 0) { ([double]$_.Bytes / [double]$accountNode.Bytes) * 100.0 } else { 0.0 }
      [PSCustomObject]@{
        Container      = $_.Name
        Used           = Format-Bytes -Bytes $_.Bytes
        Bytes          = [int64]$_.Bytes
        PercentOfTotal = $percentOfTotal
      }
    }
)

Write-Host ""
Write-Host "ADLS container usage summary" -ForegroundColor Green
Write-Host "Total used: $(Format-Bytes -Bytes $accountNode.Bytes) ($($accountNode.Bytes) bytes)" -ForegroundColor Green
Write-Host ""

$rows |
  Select-Object `
    @{ Name = "Container"; Expression = { $_.Container } }, `
    @{ Name = "Used"; Expression = { $_.Used } }, `
    @{ Name = "Bytes"; Expression = { $_.Bytes } }, `
    @{ Name = "% of Total"; Expression = { "{0:N2}%" -f $_.PercentOfTotal } } |
  Format-Table -AutoSize
