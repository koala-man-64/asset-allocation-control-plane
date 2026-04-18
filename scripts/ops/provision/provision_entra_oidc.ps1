param(
  [string]$EnvFile = "",
  [string]$SubscriptionId = "",
  [string]$ResourceGroup = "",
  [string]$ApiAppDisplayName = "asset-allocation-api",
  [string]$UiAppDisplayName = "asset-allocation-ui",
  [string]$ApiContainerAppName = "",
  [string]$AcrPullIdentityName = "",
  [string]$OperatorUserObjectId = "",
  [string]$LocalUiRedirectUri = "http://localhost:5174/auth/callback"
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

function Resolve-EnvFilePath {
  param([string]$RequestedPath)

  if (-not [string]::IsNullOrWhiteSpace($RequestedPath)) {
    return (Resolve-Path $RequestedPath -ErrorAction Stop).Path
  }

  $repoRoot = Resolve-RepoRoot
  $candidateWeb = Join-Path $repoRoot ".env.web"
  $candidateEnv = Join-Path $repoRoot ".env"

  if (Test-Path $candidateWeb) {
    return $candidateWeb
  }
  if (Test-Path $candidateEnv) {
    return $candidateEnv
  }
  return $candidateWeb
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

function Format-EnvValue {
  param([AllowNull()][string]$Value)

  if ($null -eq $Value) {
    return ""
  }
  if ($Value -eq "") {
    return ""
  }

  $escaped = $Value.Replace('"', '\"')
  return '"' + $escaped + '"'
}

function Set-EnvValues {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][hashtable]$Values
  )

  $lines = [System.Collections.Generic.List[string]]::new()
  if (Test-Path $Path) {
    foreach ($line in (Get-Content -Path $Path)) {
      $lines.Add($line)
    }
  }

  foreach ($key in $Values.Keys) {
    $formatted = Format-EnvValue -Value ([string]$Values[$key])
    $updated = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
      if ($lines[$i] -match ("^" + [regex]::Escape($key) + "=")) {
        $lines[$i] = "$key=$formatted"
        $updated = $true
        break
      }
    }

    if (-not $updated) {
      if ($lines.Count -gt 0 -and -not [string]::IsNullOrWhiteSpace($lines[$lines.Count - 1])) {
        $lines.Add("")
      }
      $lines.Add("$key=$formatted")
    }
  }

  Set-Content -Path $Path -Value $lines -Encoding utf8
}

function Invoke-AzCliRaw {
  param(
    [Parameter(Mandatory = $true)][string[]]$Arguments,
    [switch]$AllowFailure
  )

  $output = $null
  $exitCode = 0
  $text = ""
  try {
    $output = & az @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    $text = ($output | Out-String).Trim()
  }
  catch {
    $exitCode = if ($LASTEXITCODE -ne 0) { $LASTEXITCODE } else { 1 }
    $text = ($_ | Out-String).Trim()
    if (-not $AllowFailure) {
      throw "Azure CLI command failed (exit=$exitCode): az $($Arguments -join ' ')`n$text"
    }
  }

  if ($exitCode -ne 0 -and (-not $AllowFailure)) {
    throw "Azure CLI command failed (exit=$exitCode): az $($Arguments -join ' ')`n$text"
  }

  return [pscustomobject]@{
    ExitCode = $exitCode
    Output   = $text
  }
}

function Invoke-AzCliJson {
  param(
    [Parameter(Mandatory = $true)][string[]]$Arguments,
    [switch]$AllowFailure
  )

  $raw = Invoke-AzCliRaw -Arguments ($Arguments + @("-o", "json")) -AllowFailure:$AllowFailure
  if ($raw.ExitCode -ne 0) {
    return $null
  }
  if ([string]::IsNullOrWhiteSpace($raw.Output)) {
    return $null
  }
  return $raw.Output | ConvertFrom-Json
}

function Invoke-WithRetry {
  param(
    [Parameter(Mandatory = $true)][scriptblock]$ScriptBlock,
    [Parameter(Mandatory = $true)][string]$Description,
    [int]$MaxAttempts = 6,
    [int]$InitialDelaySeconds = 2
  )

  $attempt = 0
  $lastError = $null
  while ($attempt -lt $MaxAttempts) {
    $attempt += 1
    try {
      if ($attempt -gt 1) {
        Write-Host "$Description (attempt $attempt/$MaxAttempts)..." -ForegroundColor DarkGray
      }
      return & $ScriptBlock
    }
    catch {
      $lastError = $_
      if ($attempt -ge $MaxAttempts) {
        break
      }
      $delay = [Math]::Min($InitialDelaySeconds * [Math]::Pow(2, $attempt - 1), 20)
      Write-Warning ("{0} failed on attempt {1}/{2}: {3}" -f $Description, $attempt, $MaxAttempts, $_.Exception.Message)
      Write-Host "Waiting $delay second(s) before retry..." -ForegroundColor DarkGray
      Start-Sleep -Seconds ([int][Math]::Ceiling($delay))
    }
  }

  throw $lastError
}

function Invoke-GraphJson {
  param(
    [Parameter(Mandatory = $true)][ValidateSet("GET", "PATCH", "POST")][string]$Method,
    [Parameter(Mandatory = $true)][string]$Url,
    [object]$Body = $null,
    [switch]$AllowFailure
  )

  $arguments = @(
    "rest",
    "--method", $Method.ToLowerInvariant(),
    "--url", $Url,
    "--only-show-errors"
  )

  $tempBodyPath = $null
  if ($null -ne $Body) {
    $jsonBody = $Body | ConvertTo-Json -Depth 20 -Compress
    $tempBodyPath = Join-Path ([System.IO.Path]::GetTempPath()) ("asset-allocation-graph-{0}.json" -f ([guid]::NewGuid().Guid))
    $jsonBody | Set-Content -Path $tempBodyPath -Encoding utf8
    $arguments += @("--headers", "Content-Type=application/json", "--body", "@$tempBodyPath")
  }

  try {
    return Invoke-AzCliJson -Arguments $arguments -AllowFailure:$AllowFailure
  }
  finally {
    if ($tempBodyPath -and (Test-Path $tempBodyPath)) {
      Remove-Item -Path $tempBodyPath -Force -ErrorAction SilentlyContinue
    }
  }
}

function Ensure-LoggedIn {
  $account = Invoke-AzCliRaw -Arguments @("account", "show", "--query", "id", "-o", "tsv") -AllowFailure
  if ($account.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($account.Output)) {
    throw "Azure CLI is not logged in. Run 'az login --tenant <tenant-id>' and retry."
  }
}

function Resolve-FirstCsvToken {
  param([string]$Value)

  foreach ($item in [string]::Join(",", @($Value)).Split(",")) {
    $trimmed = $item.Trim()
    if (-not [string]::IsNullOrWhiteSpace($trimmed)) {
      return $trimmed
    }
  }
  return ""
}

function Resolve-OperatorUserAssignment {
  param(
    [string]$ExplicitObjectId,
    [string[]]$EnvLines = @(),
    [string]$EnvLabel = ".env"
  )

  if (-not [string]::IsNullOrWhiteSpace($ExplicitObjectId)) {
    return [pscustomobject]@{
      ObjectId          = $ExplicitObjectId.Trim()
      UserPrincipalName = ""
      Source            = "argument"
    }
  }

  $envObjectId = Get-EnvValueFirst -Keys @("ENTRA_OPERATOR_USER_OBJECT_ID") -Lines $EnvLines
  if (-not [string]::IsNullOrWhiteSpace($envObjectId)) {
    return [pscustomobject]@{
      ObjectId          = $envObjectId.Trim()
      UserPrincipalName = ""
      Source            = "env"
    }
  }

  $signedInUser = Invoke-AzCliJson -Arguments @(
    "ad", "signed-in-user", "show",
    "--query", "{id:id,userPrincipalName:userPrincipalName}",
    "--only-show-errors"
  ) -AllowFailure
  if ($null -ne $signedInUser -and (-not [string]::IsNullOrWhiteSpace([string]$signedInUser.id))) {
    return [pscustomobject]@{
      ObjectId          = [string]$signedInUser.id
      UserPrincipalName = [string]$signedInUser.userPrincipalName
      Source            = "signedInUser"
    }
  }

  $accountContext = Invoke-AzCliJson -Arguments @(
    "account", "show",
    "--query", "{name:user.name,type:user.type}",
    "--only-show-errors"
  ) -AllowFailure

  $accountSummary = ""
  if ($null -ne $accountContext) {
    $summaryParts = @()
    if (-not [string]::IsNullOrWhiteSpace([string]$accountContext.name)) {
      $summaryParts += "name=$([string]$accountContext.name)"
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$accountContext.type)) {
      $summaryParts += "type=$([string]$accountContext.type)"
    }
    if ($summaryParts.Count -gt 0) {
      $accountSummary = " Current az account user: $($summaryParts -join ', ')."
    }
  }

  throw "ENTRA_OPERATOR_USER_OBJECT_ID is required in $EnvLabel for direct operator role assignment. Automatic resolution via 'az ad signed-in-user show' also failed.$accountSummary If you are using a user login, run 'az ad signed-in-user show --query id -o tsv' and set ENTRA_OPERATOR_USER_OBJECT_ID explicitly."
}

function Get-ApplicationById {
  param([string]$Id)

  if ([string]::IsNullOrWhiteSpace($Id)) {
    return $null
  }
  return Invoke-AzCliJson -Arguments @("ad", "app", "show", "--id", $Id, "--only-show-errors") -AllowFailure
}

function Get-ExactDisplayNameApplications {
  param([Parameter(Mandatory = $true)][string]$DisplayName)

  $apps = Invoke-AzCliJson -Arguments @("ad", "app", "list", "--display-name", $DisplayName, "--only-show-errors")
  if ($null -eq $apps) {
    return @()
  }
  return @($apps | Where-Object { $_.displayName -eq $DisplayName })
}

function Ensure-Application {
  param(
    [Parameter(Mandatory = $true)][string]$DisplayName,
    [string]$ExistingId
  )

  if (-not [string]::IsNullOrWhiteSpace($ExistingId)) {
    $app = Get-ApplicationById -Id $ExistingId
    if ($null -eq $app) {
      throw "Expected application '$DisplayName' with identifier '$ExistingId' was not found."
    }
    return [pscustomobject]@{ App = $app; Created = $false }
  }

  $matches = @(Get-ExactDisplayNameApplications -DisplayName $DisplayName)
  if ($matches.Count -gt 1) {
    throw "Multiple Entra applications matched display name '$DisplayName'. Resolve duplicates or populate the env with the target client ID."
  }
  if ($matches.Count -eq 1) {
    Write-Host "Found existing Entra application '$DisplayName'." -ForegroundColor Cyan
    return [pscustomobject]@{ App = $matches[0]; Created = $false }
  }

  Write-Host "Creating Entra application '$DisplayName'..." -ForegroundColor Cyan
  $created = Invoke-AzCliJson -Arguments @(
    "ad", "app", "create",
    "--display-name", $DisplayName,
    "--sign-in-audience", "AzureADMyOrg",
    "--only-show-errors"
  )
  return [pscustomobject]@{ App = $created; Created = $true }
}

function Get-ServicePrincipalById {
  param([string]$AppId)

  if ([string]::IsNullOrWhiteSpace($AppId)) {
    return $null
  }
  return Invoke-AzCliJson -Arguments @("ad", "sp", "show", "--id", $AppId, "--only-show-errors") -AllowFailure
}

function Ensure-ServicePrincipal {
  param([Parameter(Mandatory = $true)][string]$AppId)

  $sp = Get-ServicePrincipalById -AppId $AppId
  if ($null -ne $sp) {
    Write-Host "Found existing service principal for appId '$AppId'." -ForegroundColor Cyan
    return [pscustomobject]@{ ServicePrincipal = $sp; Created = $false }
  }

  Write-Host "Creating service principal for appId '$AppId'..." -ForegroundColor Cyan
  $created = Invoke-WithRetry -Description "Create service principal for appId '$AppId'" -ScriptBlock {
    $createdSp = Invoke-AzCliJson -Arguments @("ad", "sp", "create", "--id", $AppId, "--only-show-errors")
    if ($null -eq $createdSp -or [string]::IsNullOrWhiteSpace([string]$createdSp.id)) {
      throw "Azure CLI returned no service principal payload for appId '$AppId'."
    }
    return $createdSp
  }
  return [pscustomobject]@{ ServicePrincipal = $created; Created = $true }
}

function Get-GraphApplication {
  param([Parameter(Mandatory = $true)][string]$ObjectId)

  return Invoke-GraphJson -Method GET -Url "https://graph.microsoft.com/v1.0/applications/$ObjectId"
}

function ConvertTo-AppRolePayload {
  param(
    [object]$Role,
    [string]$FallbackId
  )

  $allowedMemberTypes = @()
  foreach ($item in @($Role.allowedMemberTypes)) {
    if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) {
      $allowedMemberTypes += [string]$item
    }
  }

  return [ordered]@{
    allowedMemberTypes = $allowedMemberTypes
    description        = [string]$Role.description
    displayName        = [string]$Role.displayName
    id                 = if ($Role.id) { [string]$Role.id } else { $FallbackId }
    isEnabled          = if ($null -eq $Role.isEnabled) { $true } else { [bool]$Role.isEnabled }
    value              = [string]$Role.value
  }
}

function ConvertTo-ScopePayload {
  param(
    [object]$Scope,
    [string]$FallbackId
  )

  return [ordered]@{
    adminConsentDescription = [string]$Scope.adminConsentDescription
    adminConsentDisplayName = [string]$Scope.adminConsentDisplayName
    id                      = if ($Scope.id) { [string]$Scope.id } else { $FallbackId }
    isEnabled               = if ($null -eq $Scope.isEnabled) { $true } else { [bool]$Scope.isEnabled }
    type                    = if ($Scope.type) { [string]$Scope.type } else { "User" }
    userConsentDescription  = [string]$Scope.userConsentDescription
    userConsentDisplayName  = [string]$Scope.userConsentDisplayName
    value                   = [string]$Scope.value
  }
}

function Ensure-ApiApplicationConfiguration {
  param([Parameter(Mandatory = $true)][object]$Application)

  Write-Host "Configuring API app registration '$($Application.displayName)'..." -ForegroundColor Cyan
  $graphApp = Get-GraphApplication -ObjectId $Application.id
  if ($null -eq $graphApp) {
    throw "Failed to query Graph for API application object '$($Application.id)'."
  }

  $scopeId = ""
  $existingScopes = @()
  foreach ($scope in @($graphApp.api.oauth2PermissionScopes)) {
    if ($null -eq $scope) { continue }
    if ([string]$scope.value -eq "user_impersonation") {
      $scopeId = [string]$scope.id
      continue
    }
    $existingScopes += (ConvertTo-ScopePayload -Scope $scope -FallbackId ([guid]::NewGuid().Guid))
  }
  if ([string]::IsNullOrWhiteSpace($scopeId)) {
    $scopeId = [guid]::NewGuid().Guid
  }
  $targetScope = [ordered]@{
    adminConsentDescription = "Allow the application to access the Asset Allocation API on behalf of the signed-in user."
    adminConsentDisplayName = "Access Asset Allocation API"
    id                      = $scopeId
    isEnabled               = $true
    type                    = "User"
    userConsentDescription  = "Allow the application to access the Asset Allocation API on your behalf."
    userConsentDisplayName  = "Access Asset Allocation API"
    value                   = "user_impersonation"
  }

  $appRoleId = ""
  $existingRoles = @()
  foreach ($role in @($graphApp.appRoles)) {
    if ($null -eq $role) { continue }
    if ([string]$role.value -eq "AssetAllocation.Access") {
      $appRoleId = [string]$role.id
      continue
    }
    $existingRoles += (ConvertTo-AppRolePayload -Role $role -FallbackId ([guid]::NewGuid().Guid))
  }
  if ([string]::IsNullOrWhiteSpace($appRoleId)) {
    $appRoleId = [guid]::NewGuid().Guid
  }
  $targetRole = [ordered]@{
    allowedMemberTypes = @("User", "Application")
    description        = "Access the Asset Allocation operator API and UI."
    displayName        = "AssetAllocation.Access"
    id                 = $appRoleId
    isEnabled          = $true
    value              = "AssetAllocation.Access"
  }

  $patch = [ordered]@{
    signInAudience = "AzureADMyOrg"
    identifierUris = @("api://$($Application.appId)")
    api            = [ordered]@{
      requestedAccessTokenVersion = 2
      oauth2PermissionScopes      = @($existingScopes + $targetScope)
    }
    appRoles       = @($existingRoles + $targetRole)
  }

  Invoke-GraphJson -Method PATCH -Url "https://graph.microsoft.com/v1.0/applications/$($Application.id)" -Body $patch | Out-Null
  return [pscustomobject]@{
    ScopeId   = $scopeId
    AppRoleId = $appRoleId
  }
}

function Ensure-UiApplicationConfiguration {
  param(
    [Parameter(Mandatory = $true)][object]$Application,
    [Parameter(Mandatory = $true)][string]$PublicRedirectUri,
    [Parameter(Mandatory = $true)][string]$PublicPostLogoutRedirectUri,
    [Parameter(Mandatory = $true)][string]$LocalRedirectUri
  )

  Write-Host "Configuring UI SPA app registration '$($Application.displayName)'..." -ForegroundColor Cyan
  $graphApp = Get-GraphApplication -ObjectId $Application.id
  if ($null -eq $graphApp) {
    throw "Failed to query Graph for UI application object '$($Application.id)'."
  }

  $redirects = [System.Collections.Generic.List[string]]::new()
  foreach ($value in @($graphApp.spa.redirectUris)) {
    $candidate = [string]$value
    if (-not [string]::IsNullOrWhiteSpace($candidate) -and (-not $redirects.Contains($candidate))) {
      $redirects.Add($candidate)
    }
  }
  foreach ($required in @($PublicRedirectUri, $LocalRedirectUri)) {
    if (-not $redirects.Contains($required)) {
      $redirects.Add($required)
    }
  }

  $patch = [ordered]@{
    signInAudience = "AzureADMyOrg"
    spa            = [ordered]@{
      redirectUris = @($redirects)
    }
    web            = [ordered]@{
      logoutUrl = $PublicPostLogoutRedirectUri
    }
  }

  Invoke-GraphJson -Method PATCH -Url "https://graph.microsoft.com/v1.0/applications/$($Application.id)" -Body $patch | Out-Null
}

function Ensure-UiDelegatedPermission {
  param(
    [Parameter(Mandatory = $true)][string]$UiAppId,
    [Parameter(Mandatory = $true)][string]$ApiAppId,
    [Parameter(Mandatory = $true)][string]$ScopeId
  )

  Write-Host "Ensuring UI delegated permission to API scope..." -ForegroundColor Cyan
  $requiredResourceAccess = Invoke-AzCliJson -Arguments @(
    "ad", "app", "show",
    "--id", $UiAppId,
    "--query", "requiredResourceAccess",
    "--only-show-errors"
  )

  $hasScope = $false
  foreach ($resource in @($requiredResourceAccess)) {
    if ([string]$resource.resourceAppId -ne $ApiAppId) {
      continue
    }
    foreach ($access in @($resource.resourceAccess)) {
      if ([string]$access.id -eq $ScopeId -and [string]$access.type -eq "Scope") {
        $hasScope = $true
        break
      }
    }
  }

  if (-not $hasScope) {
    Invoke-AzCliRaw -Arguments @(
      "ad", "app", "permission", "add",
      "--id", $UiAppId,
      "--api", $ApiAppId,
      "--api-permissions", "$ScopeId=Scope",
      "--only-show-errors"
    ) | Out-Null
  }

  Invoke-AzCliRaw -Arguments @(
    "ad", "app", "permission", "admin-consent",
    "--id", $UiAppId,
    "--only-show-errors"
  ) | Out-Null
}

function Ensure-AppRoleAssignmentRequired {
  param([Parameter(Mandatory = $true)][string]$ServicePrincipalObjectId)

  Write-Host "Requiring app-role assignment on service principal '$ServicePrincipalObjectId'..." -ForegroundColor Cyan
  Invoke-GraphJson -Method PATCH -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$ServicePrincipalObjectId" -Body @{
    appRoleAssignmentRequired = $true
  } | Out-Null
}

function Get-ExistingAppRoleAssignments {
  param([Parameter(Mandatory = $true)][string]$ResourceServicePrincipalObjectId)

  $response = Invoke-GraphJson -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$ResourceServicePrincipalObjectId/appRoleAssignedTo"
  if ($null -eq $response) {
    return @()
  }
  return @($response.value)
}

function Ensure-AppRoleAssignment {
  param(
    [Parameter(Mandatory = $true)][string]$ResourceServicePrincipalObjectId,
    [Parameter(Mandatory = $true)][string]$PrincipalObjectId,
    [Parameter(Mandatory = $true)][string]$AppRoleId
  )

  $existingAssignments = @(Get-ExistingAppRoleAssignments -ResourceServicePrincipalObjectId $ResourceServicePrincipalObjectId)
  foreach ($assignment in $existingAssignments) {
    if ([string]$assignment.principalId -eq $PrincipalObjectId -and [string]$assignment.appRoleId -eq $AppRoleId) {
      Write-Host "App role already assigned to principal '$PrincipalObjectId'." -ForegroundColor Cyan
      return $false
    }
  }

  Write-Host "Assigning app role to principal '$PrincipalObjectId'..." -ForegroundColor Cyan
  $payload = @{
    appRoleId  = $AppRoleId
    principalId = $PrincipalObjectId
    resourceId = $ResourceServicePrincipalObjectId
  }

  Invoke-GraphJson -Method POST -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$ResourceServicePrincipalObjectId/appRoleAssignedTo" -Body $payload | Out-Null
  return $true
}

function Resolve-ManagedIdentityPrincipalId {
  param(
    [Parameter(Mandatory = $true)][string]$IdentityName,
    [Parameter(Mandatory = $true)][string]$ResourceGroupName
  )

  $principalId = Invoke-AzCliRaw -Arguments @(
    "identity", "show",
    "--name", $IdentityName,
    "--resource-group", $ResourceGroupName,
    "--query", "principalId",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ($principalId.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($principalId.Output)) {
    throw "Unable to resolve principalId for managed identity '$IdentityName' in resource group '$ResourceGroupName'."
  }
  return $principalId.Output.Trim()
}

function Resolve-PublicRedirectUri {
  param(
    [AllowEmptyString()][string]$ExplicitRedirectUri = "",
    [Parameter(Mandatory = $true)][string]$ApiContainerApp,
    [Parameter(Mandatory = $true)][string]$ResourceGroupName
  )

  if (-not [string]::IsNullOrWhiteSpace($ExplicitRedirectUri)) {
    $candidate = $ExplicitRedirectUri.Trim()
    try {
      $uri = [System.Uri]$candidate
    }
    catch {
      throw "UI_OIDC_REDIRECT_URI must be an absolute https:// URI."
    }
    if ((-not $uri.IsAbsoluteUri) -or $uri.Scheme -ne "https" -or [string]::IsNullOrWhiteSpace($uri.Host)) {
      throw "UI_OIDC_REDIRECT_URI must be an absolute https:// URI."
    }
    return $candidate
  }

  $managedEnvironmentId = Invoke-AzCliRaw -Arguments @(
    "containerapp", "show",
    "--name", $ApiContainerApp,
    "--resource-group", $ResourceGroupName,
    "--query", "properties.managedEnvironmentId",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ($managedEnvironmentId.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($managedEnvironmentId.Output)) {
    throw "Failed to resolve the Container Apps managed environment for '$ApiContainerApp'. Set UI_OIDC_REDIRECT_URI explicitly or provision the API Container App first."
  }

  $defaultDomain = Invoke-AzCliRaw -Arguments @(
    "containerapp", "env", "show",
    "--ids", $managedEnvironmentId.Output.Trim(),
    "--query", "properties.defaultDomain",
    "-o", "tsv",
    "--only-show-errors"
  )
  if ($defaultDomain.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($defaultDomain.Output)) {
    throw "Failed to resolve the Container Apps environment default domain. Set UI_OIDC_REDIRECT_URI explicitly."
  }

  return "https://$ApiContainerApp.$($defaultDomain.Output.Trim())/auth/callback"
}

function Resolve-PostLogoutRedirectUri {
  param([Parameter(Mandatory = $true)][string]$RedirectUri)

  try {
    $uri = [System.Uri]$RedirectUri
  }
  catch {
    throw "Unable to derive the post-logout redirect URI from '$RedirectUri'."
  }

  if ((-not $uri.IsAbsoluteUri) -or [string]::IsNullOrWhiteSpace($uri.Scheme) -or [string]::IsNullOrWhiteSpace($uri.Host)) {
    throw "Unable to derive the post-logout redirect URI from '$RedirectUri'."
  }

  return [System.Uri]::new($uri, "/auth/logout-complete").AbsoluteUri
}

$envPath = Resolve-EnvFilePath -RequestedPath $EnvFile
$envLines = Get-EnvLines -Path $envPath
$envLabel = Split-Path -Leaf $envPath

Ensure-LoggedIn

$tenantId = Get-EnvValueFirst -Keys @("AZURE_TENANT_ID") -Lines $envLines
if ([string]::IsNullOrWhiteSpace($tenantId)) {
  $tenantId = $env:AZURE_TENANT_ID
}
if ([string]::IsNullOrWhiteSpace($tenantId)) {
  throw "AZURE_TENANT_ID is required in $envLabel or the current process."
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = $env:AZURE_SUBSCRIPTION_ID
}
if (-not [string]::IsNullOrWhiteSpace($SubscriptionId)) {
  Invoke-AzCliRaw -Arguments @("account", "set", "--subscription", $SubscriptionId, "--only-show-errors") | Out-Null
}

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = Get-EnvValueFirst -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = "AssetAllocationRG"
}

if ([string]::IsNullOrWhiteSpace($ApiContainerAppName)) {
  $ApiContainerAppName = Get-EnvValueFirst -Keys @("API_CONTAINER_APP_NAME", "CONTAINER_APP_API_NAME") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($ApiContainerAppName)) {
  $ApiContainerAppName = "asset-allocation-api"
}

if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = Get-EnvValueFirst -Keys @("ACR_PULL_IDENTITY_NAME", "ACR_PULL_USER_ASSIGNED_IDENTITY_NAME") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($AcrPullIdentityName)) {
  $AcrPullIdentityName = "asset-allocation-acr-pull-mi"
}

$operatorUserAssignment = Resolve-OperatorUserAssignment `
  -ExplicitObjectId $OperatorUserObjectId `
  -EnvLines $envLines `
  -EnvLabel $envLabel
$OperatorUserObjectId = $operatorUserAssignment.ObjectId

$apiAudienceHint = Resolve-FirstCsvToken (Get-EnvValue -Key "API_OIDC_AUDIENCE" -Lines $envLines)
$uiClientIdHint = Resolve-FirstCsvToken (Get-EnvValue -Key "UI_OIDC_CLIENT_ID" -Lines $envLines)
$explicitRedirectUri = Get-EnvValue -Key "UI_OIDC_REDIRECT_URI" -Lines $envLines

Write-Host "Provisioning Entra OIDC applications" -ForegroundColor Cyan
Write-Host "Environment file: $envLabel"
Write-Host "Tenant ID: $tenantId"
Write-Host "Subscription ID: $SubscriptionId"
Write-Host "Resource group: $ResourceGroup"
Write-Host "API app display name: $ApiAppDisplayName"
Write-Host "UI app display name: $UiAppDisplayName"
Write-Host "API container app: $ApiContainerAppName"
Write-Host "Managed identity: $AcrPullIdentityName"
Write-Host "Operator user source: $($operatorUserAssignment.Source)"
Write-Host ""

$apiAppResult = Ensure-Application -DisplayName $ApiAppDisplayName -ExistingId $apiAudienceHint
$apiApp = $apiAppResult.App
$apiSpResult = Ensure-ServicePrincipal -AppId $apiApp.appId
$apiServicePrincipal = $apiSpResult.ServicePrincipal
$apiConfig = Ensure-ApiApplicationConfiguration -Application $apiApp
Ensure-AppRoleAssignmentRequired -ServicePrincipalObjectId $apiServicePrincipal.id

$publicRedirectUri = Resolve-PublicRedirectUri `
  -ExplicitRedirectUri $explicitRedirectUri `
  -ApiContainerApp $ApiContainerAppName `
  -ResourceGroupName $ResourceGroup
$publicPostLogoutRedirectUri = Resolve-PostLogoutRedirectUri -RedirectUri $publicRedirectUri

$uiAppResult = Ensure-Application -DisplayName $UiAppDisplayName -ExistingId $uiClientIdHint
$uiApp = $uiAppResult.App
$uiSpResult = Ensure-ServicePrincipal -AppId $uiApp.appId
$null = $uiSpResult.ServicePrincipal
Ensure-UiApplicationConfiguration `
  -Application $uiApp `
  -PublicRedirectUri $publicRedirectUri `
  -PublicPostLogoutRedirectUri $publicPostLogoutRedirectUri `
  -LocalRedirectUri $LocalUiRedirectUri
Ensure-UiDelegatedPermission -UiAppId $uiApp.appId -ApiAppId $apiApp.appId -ScopeId $apiConfig.ScopeId

$managedIdentityPrincipalId = Resolve-ManagedIdentityPrincipalId -IdentityName $AcrPullIdentityName -ResourceGroupName $ResourceGroup
$operatorAssignmentCreated = Ensure-AppRoleAssignment `
  -ResourceServicePrincipalObjectId $apiServicePrincipal.id `
  -PrincipalObjectId $OperatorUserObjectId `
  -AppRoleId $apiConfig.AppRoleId
$runtimeAssignmentCreated = Ensure-AppRoleAssignment `
  -ResourceServicePrincipalObjectId $apiServicePrincipal.id `
  -PrincipalObjectId $managedIdentityPrincipalId `
  -AppRoleId $apiConfig.AppRoleId

$authority = "https://login.microsoftonline.com/$tenantId"
$issuer = "$authority/v2.0"

$envUpdates = [ordered]@{
  ENTRA_OPERATOR_USER_OBJECT_ID  = $OperatorUserObjectId
  API_OIDC_ISSUER             = $issuer
  API_OIDC_AUDIENCE           = [string]$apiApp.appId
  API_OIDC_REQUIRED_ROLES     = "AssetAllocation.Access"
  API_OIDC_REQUIRED_SCOPES    = ""
  UI_OIDC_CLIENT_ID           = [string]$uiApp.appId
  UI_OIDC_AUTHORITY           = $authority
  UI_OIDC_SCOPES              = "api://$($apiApp.appId)/user_impersonation openid profile offline_access"
  UI_OIDC_REDIRECT_URI        = $publicRedirectUri
  ASSET_ALLOCATION_API_SCOPE  = "api://$($apiApp.appId)/.default"
}
Set-EnvValues -Path $envPath -Values $envUpdates

$outputs = [ordered]@{
  envFilePath                         = $envPath
  apiAppDisplayName                   = $ApiAppDisplayName
  apiAppObjectId                      = $apiApp.id
  apiAppClientId                      = $apiApp.appId
  apiServicePrincipalObjectId         = $apiServicePrincipal.id
  uiAppDisplayName                    = $UiAppDisplayName
  uiAppObjectId                       = $uiApp.id
  uiAppClientId                       = $uiApp.appId
  oidcAuthority                       = $authority
  oidcIssuer                          = $issuer
  uiRedirectUri                       = $publicRedirectUri
  uiPostLogoutRedirectUri             = $publicPostLogoutRedirectUri
  apiDelegatedScopeId                 = $apiConfig.ScopeId
  apiAppRoleId                        = $apiConfig.AppRoleId
  operatorUserObjectId                = $OperatorUserObjectId
  operatorUserPrincipalName           = $operatorUserAssignment.UserPrincipalName
  operatorUserResolutionSource        = $operatorUserAssignment.Source
  managedIdentityPrincipalId          = $managedIdentityPrincipalId
  operatorRoleAssignmentCreated       = [bool]$operatorAssignmentCreated
  managedIdentityRoleAssignmentCreated = [bool]$runtimeAssignmentCreated
}

Write-Host ""
Write-Host "Entra OIDC provisioning complete. Outputs:" -ForegroundColor Green
$outputs | ConvertTo-Json -Depth 5
