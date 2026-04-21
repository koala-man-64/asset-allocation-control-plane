param(
    [string]$EnvFilePath = "",
    [switch]$DryRun,
    [string[]]$Set = @(),
    [string]$DispatchAppPrivateKeyFilePath = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($EnvFilePath)) {
    $EnvFilePath = Join-Path $repoRoot ".env.web"
}

$contractPath = Join-Path $repoRoot "docs\ops\env-contract.csv"
$templatePath = Join-Path $repoRoot ".env.template"

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Invoke-TextCommand {
    param([Parameter(Mandatory = $true)][string]$FilePath, [Parameter(Mandatory = $true)][string[]]$ArgumentList)
    if (-not (Test-CommandAvailable -Name $FilePath)) { return "" }
    try {
        $result = & $FilePath @ArgumentList 2>$null
        return (($result | Out-String).Trim())
    } catch {
        return ""
    }
}

function Invoke-JsonCommand {
    param([Parameter(Mandatory = $true)][string]$FilePath, [Parameter(Mandatory = $true)][string[]]$ArgumentList)
    $text = Invoke-TextCommand -FilePath $FilePath -ArgumentList $ArgumentList
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return $text | ConvertFrom-Json } catch { return $null }
}

function Parse-EnvFile {
    param([Parameter(Mandatory = $true)][string]$Path)
    $map = @{}
    if (-not (Test-Path $Path)) { return $map }
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or $line -notmatch "^([^=]+)=(.*)$") { continue }
        $map[$matches[1].Trim()] = $matches[2]
    }
    return $map
}

function Load-ContractRows {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path $Path)) { throw "Env contract not found at $Path" }
    return @(Import-Csv -Path $Path | Where-Object { $_.template -eq "true" -and $_.github_storage -in @("var", "secret") })
}

function ConvertFrom-SecureStringPlain {
    param([Parameter(Mandatory = $true)][System.Security.SecureString]$Secure)
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

function Normalize-EnvValue {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return "" }
    return $Value.Replace("`r", "").Replace("`n", "\n")
}

function Normalize-SelfPlaceholderValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [AllowNull()][string]$Value
    )
    $normalized = Normalize-EnvValue -Value $Value
    if ($normalized -eq ('${' + $Name + '}')) { return "" }
    return $normalized
}

function Register-NormalizedQuotedScalarValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Source
    )
    if ([string]::IsNullOrWhiteSpace($Key) -or [string]::IsNullOrWhiteSpace($Source)) { return }
    $entryKey = "$Source::$Key"
    if (-not $script:NormalizedQuotedScalarValues.ContainsKey($entryKey)) {
        $script:NormalizedQuotedScalarValues[$entryKey] = [pscustomobject]@{
            Key    = $Key
            Source = $Source
        }
    }
}

function Normalize-QuotedScalarValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [AllowNull()][string]$Value,
        [Parameter(Mandatory = $true)][string]$Source
    )
    $candidate = (Normalize-EnvValue -Value $Value).Trim()
    if ([string]::IsNullOrWhiteSpace($candidate) -or $candidate.Length -lt 2) { return $candidate }

    $quote = $candidate.Substring(0, 1)
    if (($quote -ne '"' -and $quote -ne "'") -or $candidate.Substring($candidate.Length - 1, 1) -ne $quote) {
        return $candidate
    }

    $inner = $candidate.Substring(1, $candidate.Length - 2).Trim()
    if ([string]::IsNullOrWhiteSpace($inner)) { return $candidate }
    if ($inner.Contains("\n")) { return $candidate }
    if ($inner.StartsWith("{") -or $inner.StartsWith("[")) { return $candidate }

    Register-NormalizedQuotedScalarValue -Key $Key -Source $Source
    return $inner
}

function Write-NormalizedQuotedScalarWarnings {
    foreach ($entryKey in ($script:NormalizedQuotedScalarValues.Keys | Sort-Object)) {
        $entry = $script:NormalizedQuotedScalarValues[$entryKey]
        Write-Warning ("Normalized quoted scalar value for {0} from {1}." -f $entry.Key, $entry.Source)
    }
}

function Normalize-EnvValueForKey {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [AllowNull()][string]$Value
    )
    $normalized = Normalize-SelfPlaceholderValue -Name $Name -Value $Value
    if ($Name -like "*_REQUIRED_ROLES") {
        return ($normalized -replace '^[''"]+', '' -replace '[''"]+$', '')
    }
    return $normalized
}

function Test-TruthyValue {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return $false }
    return @("1", "true", "t", "yes", "y", "on") -contains $Value.Trim().ToLowerInvariant()
}

function Resolve-ExistingFilePath {
    param([Parameter(Mandatory = $true)][string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { throw "File path must not be empty." }
    $candidate = $Path.Trim()
    if (-not [System.IO.Path]::IsPathRooted($candidate)) {
        $candidate = Join-Path $repoRoot $candidate
    }
    if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        throw "File not found: $candidate"
    }
    return (Resolve-Path -LiteralPath $candidate -ErrorAction Stop).Path
}

function Read-SecretFileValue {
    param([Parameter(Mandatory = $true)][string]$Path)
    $resolvedPath = Resolve-ExistingFilePath -Path $Path
    $raw = Get-Content -LiteralPath $resolvedPath -Raw -ErrorAction Stop
    return (Normalize-EnvValue -Value ($raw.TrimEnd("`r", "`n")))
}

function Get-ResourceLeafName {
    param([AllowNull()][string]$ResourceId)
    if ([string]::IsNullOrWhiteSpace($ResourceId)) { return "" }
    if ($ResourceId -match "/(?<name>[^/]+)$") { return $matches["name"] }
    return $ResourceId.Trim()
}

function Get-ObjectPropertyValue {
    param(
        [AllowNull()]$Object,
        [Parameter(Mandatory = $true)][string]$PropertyName
    )
    if ($null -eq $Object) { return $null }
    if ($Object -is [System.Collections.IDictionary] -and $Object.Contains($PropertyName)) {
        return $Object[$PropertyName]
    }
    if ($null -eq $Object.PSObject) { return $null }
    $property = $Object.PSObject.Properties[$PropertyName]
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Get-ObjectStringProperty {
    param(
        [AllowNull()]$Object,
        [Parameter(Mandatory = $true)][string]$PropertyName
    )
    $value = Get-ObjectPropertyValue -Object $Object -PropertyName $PropertyName
    if ($null -eq $value) { return "" }
    return [string]$value
}

function Get-NestedObjectPropertyValue {
    param(
        [AllowNull()]$Object,
        [Parameter(Mandatory = $true)][string[]]$PropertyPath
    )
    $current = $Object
    foreach ($propertyName in $PropertyPath) {
        $current = Get-ObjectPropertyValue -Object $current -PropertyName $propertyName
        if ($null -eq $current) { return $null }
    }
    return $current
}

$overrideMap = @{}
foreach ($entry in $Set) {
    if ($entry -match "^([^=]+)=(.*)$") {
        $overrideMap[$matches[1].Trim()] = $matches[2]
    }
}

$secretFileOverrideMap = @{}
if (-not [string]::IsNullOrWhiteSpace($DispatchAppPrivateKeyFilePath)) {
    $secretFileOverrideMap["DISPATCH_APP_PRIVATE_KEY"] = Read-SecretFileValue -Path $DispatchAppPrivateKeyFilePath
}

$existingMap = Parse-EnvFile -Path $EnvFilePath
$templateMap = Parse-EnvFile -Path $templatePath
$contractRows = Load-ContractRows -Path $contractPath

$script:NormalizedQuotedScalarValues = @{}
$script:AzureAccount = $null
$script:AzureResourceGroup = $null
$script:ResourceGroupName = $null
$script:GitHubRepo = $null
$script:GitHubVariables = $null
$script:GitHubSecretNames = $null
$script:GitOwner = $null
$script:GitHubRepoDefaultBranches = @{}
$script:Identities = $null
$script:AcrRegistries = $null
$script:ContainerApps = $null
$script:ContainerAppDetails = @{}
$script:ApiContainerAppName = $null
$script:UiContainerAppName = $null
$script:ContainerAppRuntimeEnv = $null
$script:ContainerAppsEnv = $null
$script:ContainerAppEnvironment = $null
$script:ContainerAppEnvironmentName = $null
$script:StorageAccounts = $null
$script:LogAnalyticsWorkspaces = $null
$script:PreferredLogAnalyticsWorkspace = $null
$script:PostgresServers = $null
$script:PreferredPostgresServer = $null
$script:PostgresDatabases = @{}
$script:EntraApps = @{}
$script:RequiredEnvKeys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($requiredKey in @(
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_SUBSCRIPTION_ID",
    "RESOURCE_GROUP",
    "ACR_NAME",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "API_OIDC_ISSUER",
    "API_OIDC_AUDIENCE",
    "UI_OIDC_CLIENT_ID",
    "UI_OIDC_AUTHORITY",
    "UI_OIDC_SCOPES",
    "UI_OIDC_REDIRECT_URI",
    "DISPATCH_APP_ID",
    "ALPHA_VANTAGE_API_KEY",
    "AZURE_STORAGE_CONNECTION_STRING",
    "DISPATCH_APP_PRIVATE_KEY",
    "MASSIVE_API_KEY",
    "POSTGRES_ADMIN_USER",
    "POSTGRES_ADMIN_PASSWORD"
)) {
    [void]$script:RequiredEnvKeys.Add($requiredKey)
}

function Get-ResolvedAiRelayEnabled {
    $candidates = @()
    if ($overrideMap.ContainsKey("AI_RELAY_ENABLED")) {
        $candidate = Normalize-EnvValueForKey -Name "AI_RELAY_ENABLED" -Value $overrideMap["AI_RELAY_ENABLED"]
        if (-not [string]::IsNullOrWhiteSpace($candidate)) { $candidates += $candidate }
    }
    if ($existingMap.ContainsKey("AI_RELAY_ENABLED")) {
        $candidate = Normalize-EnvValueForKey -Name "AI_RELAY_ENABLED" -Value $existingMap["AI_RELAY_ENABLED"]
        if (-not [string]::IsNullOrWhiteSpace($candidate)) { $candidates += $candidate }
    }
    $githubValue = Get-GitHubVariableValue -Name "AI_RELAY_ENABLED"
    if (-not [string]::IsNullOrWhiteSpace($githubValue)) { $candidates += $githubValue }
    if ($templateMap.ContainsKey("AI_RELAY_ENABLED")) {
        $candidate = Normalize-EnvValueForKey -Name "AI_RELAY_ENABLED" -Value $templateMap["AI_RELAY_ENABLED"]
        if (-not [string]::IsNullOrWhiteSpace($candidate)) { $candidates += $candidate }
    }

    foreach ($candidate in $candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
        return (Test-TruthyValue -Value $candidate)
    }
    return $false
}

function Get-RequirementLevel {
    param([Parameter(Mandatory = $true)][string]$Name)
    if ($Name -in @("AI_RELAY_API_KEY", "AI_RELAY_REQUIRED_ROLES")) {
        if (Get-ResolvedAiRelayEnabled) { return "required" }
        return "optional"
    }
    if ($script:RequiredEnvKeys.Contains($Name)) { return "required" }
    return "optional"
}

function Format-SuggestedDisplayValue {
    param(
        [AllowNull()][string]$Value,
        [Parameter(Mandatory = $true)][string]$Requirement,
        [bool]$IsSecret = $false
    )
    $normalized = Normalize-EnvValue -Value $Value
    if (-not [string]::IsNullOrWhiteSpace($normalized)) {
        if ($IsSecret) { return "<redacted>" }
        return $normalized
    }
    if ($Requirement -eq "required") { return "<set manually>" }
    return "<blank>"
}

function Test-ResolvedValuePresent {
    param(
        [AllowNull()][string]$Value,
        [string]$Source = "",
        [bool]$PromptRequired = $false
    )
    if ($Source -eq "github-secret") { return $true }
    if ($PromptRequired) { return $false }
    return (-not [string]::IsNullOrWhiteSpace((Normalize-EnvValue -Value $Value)))
}

function Get-GitHubRepoInfo {
    if ($null -eq $script:GitHubRepo) {
        $repo = Invoke-JsonCommand -FilePath "gh" -ArgumentList @("repo", "view", "--json", "name,nameWithOwner,owner,defaultBranchRef")
        if ($null -ne $repo) {
            $script:GitHubRepo = $repo
        } else {
            $remote = Invoke-TextCommand -FilePath "git" -ArgumentList @("-C", $repoRoot, "config", "--get", "remote.origin.url")
            $owner = ""
            $name = ""
            if ($remote -match "github\.com[:/](?<owner>[^/]+)/(?<repo>[^/.]+)(?:\.git)?$") {
                $owner = $matches["owner"]
                $name = $matches["repo"]
            }
            $nameWithOwner = if ($owner -and $name) { "$owner/$name" } else { "" }
            $script:GitHubRepo = [pscustomobject]@{
                name             = $name
                nameWithOwner    = $nameWithOwner
                owner            = [pscustomobject]@{ login = $owner }
                defaultBranchRef = [pscustomobject]@{ name = "" }
            }
        }
    }
    return $script:GitHubRepo
}

function Get-GitHubVariables {
    if ($null -eq $script:GitHubVariables) {
        $map = @{}
        $variables = Invoke-JsonCommand -FilePath "gh" -ArgumentList @("variable", "list", "--json", "name,value")
        foreach ($entry in @($variables)) {
            if ($null -eq $entry) { continue }
            $name = if ($entry.PSObject.Properties["name"]) { [string]$entry.name } else { "" }
            if ([string]::IsNullOrWhiteSpace($name)) { continue }
            $value = if ($entry.PSObject.Properties["value"] -and $null -ne $entry.value) {
                Normalize-QuotedScalarValue -Key $name -Value ([string]$entry.value) -Source "github"
            }
            else {
                ""
            }
            $map[$name] = $value
        }
        $script:GitHubVariables = $map
    }
    return $script:GitHubVariables
}

function Get-GitHubVariableValue {
    param([Parameter(Mandatory = $true)][string]$Name)
    $map = Get-GitHubVariables
    if ($map.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace($map[$Name])) {
        return (Normalize-EnvValueForKey -Name $Name -Value $map[$Name])
    }
    return ""
}

function Get-GitHubSecretNames {
    if ($null -eq $script:GitHubSecretNames) {
        $names = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        $secrets = Invoke-JsonCommand -FilePath "gh" -ArgumentList @("secret", "list", "--json", "name")
        foreach ($entry in @($secrets)) {
            if ($null -eq $entry) { continue }
            $name = if ($entry.PSObject.Properties["name"]) { [string]$entry.name } else { "" }
            if ([string]::IsNullOrWhiteSpace($name)) { continue }
            [void]$names.Add($name)
        }
        $script:GitHubSecretNames = $names
    }
    return ,$script:GitHubSecretNames
}

function Test-GitHubSecretExists {
    param([Parameter(Mandatory = $true)][string]$Name)
    return (Get-GitHubSecretNames).Contains($Name)
}

function Get-ConfiguredValue {
    param([Parameter(Mandatory = $true)][string[]]$Keys, [string]$Fallback = "")
    foreach ($key in $Keys) {
        if ($overrideMap.ContainsKey($key)) {
            $overrideValue = Normalize-EnvValueForKey -Name $key -Value $overrideMap[$key]
            if (-not [string]::IsNullOrWhiteSpace($overrideValue)) { return $overrideValue }
        }
        if ($existingMap.ContainsKey($key)) {
            $existingValue = Normalize-EnvValueForKey -Name $key -Value $existingMap[$key]
            if (-not [string]::IsNullOrWhiteSpace($existingValue)) { return $existingValue }
        }
        $githubValue = Get-GitHubVariableValue -Name $key
        if (-not [string]::IsNullOrWhiteSpace($githubValue)) { return $githubValue }
        if ($templateMap.ContainsKey($key)) {
            $templateValue = Normalize-EnvValueForKey -Name $key -Value $templateMap[$key]
            if (-not [string]::IsNullOrWhiteSpace($templateValue)) { return $templateValue }
        }
    }
    return (Normalize-EnvValueForKey -Name $Keys[0] -Value $Fallback)
}

function Get-AzureAccount {
    if ($null -eq $script:AzureAccount) {
        $script:AzureAccount = Invoke-JsonCommand -FilePath "az" -ArgumentList @("account", "show", "-o", "json")
    }
    return $script:AzureAccount
}

function Get-ResourceGroupName {
    if ($null -eq $script:ResourceGroupName) {
        $script:ResourceGroupName = Get-ConfiguredValue -Keys @("RESOURCE_GROUP") -Fallback "AssetAllocationRG"
    }
    return $script:ResourceGroupName
}

function Get-AzureResourceGroup {
    if ($null -eq $script:AzureResourceGroup) {
        $name = Get-ResourceGroupName
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $script:AzureResourceGroup = Invoke-JsonCommand -FilePath "az" -ArgumentList @("group", "show", "--name", $name, "-o", "json")
        }
    }
    return $script:AzureResourceGroup
}

function Get-GitOwner {
    if ($null -eq $script:GitOwner) {
        $repo = Get-GitHubRepoInfo
        $owner = ""
        if ($repo -and $repo.owner -and $repo.owner.login) {
            $owner = [string]$repo.owner.login
        }
        if ([string]::IsNullOrWhiteSpace($owner)) {
            $remote = Invoke-TextCommand -FilePath "git" -ArgumentList @("-C", $repoRoot, "config", "--get", "remote.origin.url")
            if ($remote -match "github\.com[:/](?<owner>[^/]+)/(?<repo>[^/.]+)(?:\.git)?$") {
                $owner = $matches["owner"]
            }
        }
        $script:GitOwner = $owner
    }
    return $script:GitOwner
}

function Get-RepoSlug {
    param([Parameter(Mandatory = $true)][string]$RepoName)
    $owner = Get-GitOwner
    if ($owner) { return "$owner/$RepoName" }
    return ""
}

function Get-GitHubRepoDefaultBranch {
    param([Parameter(Mandatory = $true)][string]$RepoSlug)
    if ([string]::IsNullOrWhiteSpace($RepoSlug)) { return "" }
    if (-not $script:GitHubRepoDefaultBranches.ContainsKey($RepoSlug)) {
        $branch = ""
        $currentRepo = Get-GitHubRepoInfo
        if ($currentRepo -and [string]$currentRepo.nameWithOwner -eq $RepoSlug -and $currentRepo.defaultBranchRef -and $currentRepo.defaultBranchRef.name) {
            $branch = [string]$currentRepo.defaultBranchRef.name
        }
        if ([string]::IsNullOrWhiteSpace($branch)) {
            $repo = Invoke-JsonCommand -FilePath "gh" -ArgumentList @("repo", "view", $RepoSlug, "--json", "defaultBranchRef")
            if ($repo -and $repo.defaultBranchRef -and $repo.defaultBranchRef.name) {
                $branch = [string]$repo.defaultBranchRef.name
            }
        }
        $script:GitHubRepoDefaultBranches[$RepoSlug] = $branch
    }
    return [string]$script:GitHubRepoDefaultBranches[$RepoSlug]
}

function Get-ItemsFromAzure {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    $items = Invoke-JsonCommand -FilePath "az" -ArgumentList $Arguments
    if ($items) { return @($items) }
    return @()
}

function Get-UserAssignedIdentities {
    if ($null -eq $script:Identities) {
        $script:Identities = Get-ItemsFromAzure -Arguments @("identity", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:Identities
}

function Get-AcrRegistries {
    if ($null -eq $script:AcrRegistries) {
        $script:AcrRegistries = Get-ItemsFromAzure -Arguments @("acr", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:AcrRegistries
}

function Get-ContainerApps {
    if ($null -eq $script:ContainerApps) {
        $script:ContainerApps = Get-ItemsFromAzure -Arguments @("containerapp", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:ContainerApps
}

function Get-ContainerAppsEnvironments {
    if ($null -eq $script:ContainerAppsEnv) {
        $script:ContainerAppsEnv = Get-ItemsFromAzure -Arguments @("containerapp", "env", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:ContainerAppsEnv
}

function Get-StorageAccounts {
    if ($null -eq $script:StorageAccounts) {
        $script:StorageAccounts = Get-ItemsFromAzure -Arguments @("storage", "account", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:StorageAccounts
}

function Get-LogAnalyticsWorkspaces {
    if ($null -eq $script:LogAnalyticsWorkspaces) {
        $script:LogAnalyticsWorkspaces = Get-ItemsFromAzure -Arguments @("monitor", "log-analytics", "workspace", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:LogAnalyticsWorkspaces
}

function Get-PostgresServers {
    if ($null -eq $script:PostgresServers) {
        $script:PostgresServers = Get-ItemsFromAzure -Arguments @("postgres", "flexible-server", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:PostgresServers
}

function Select-PreferredItem {
    param(
        [AllowNull()]$Items,
        [string]$Preferred = "",
        [string[]]$Contains = @(),
        [switch]$AllowSingleItemFallback
    )
    $list = @($Items)
    if ($list.Count -eq 0) { return $null }
    if (-not [string]::IsNullOrWhiteSpace($Preferred)) {
        $exact = @($list | Where-Object { (Get-ObjectStringProperty -Object $_ -PropertyName "name") -eq $Preferred } | Select-Object -First 1)
        if ($exact.Count -gt 0) { return $exact[0] }
    }
    foreach ($needle in $Contains) {
        $match = @($list | Where-Object { (Get-ObjectStringProperty -Object $_ -PropertyName "name") -like "*$needle*" } | Select-Object -First 1)
        if ($match.Count -gt 0) { return $match[0] }
    }
    if ($AllowSingleItemFallback -and $list.Count -eq 1) {
        $singleName = Get-ObjectStringProperty -Object $list[0] -PropertyName "name"
        if (-not [string]::IsNullOrWhiteSpace($singleName)) { return $list[0] }
    }
    return $null
}

function Get-ApiContainerAppName {
    if ($null -eq $script:ApiContainerAppName) {
        $configuredName = Get-ConfiguredValue -Keys @("API_APP_NAME") -Fallback "asset-allocation-api"
        $selected = $null
        if (-not [string]::IsNullOrWhiteSpace($configuredName)) {
            $selected = Select-PreferredItem -Items (Get-ContainerApps) -Preferred $configuredName
        }
        if ($null -eq $selected) {
            $selected = Select-PreferredItem -Items (Get-ContainerApps) -Preferred "asset-allocation-api" -Contains @("asset", "api") -AllowSingleItemFallback
        }
        $script:ApiContainerAppName = if ($selected) { [string]$selected.name } else { $configuredName }
    }
    return $script:ApiContainerAppName
}

function Get-UiContainerAppName {
    if ($null -eq $script:UiContainerAppName) {
        $configuredName = Get-ConfiguredValue -Keys @("UI_APP_NAME", "CONTAINER_APP_UI_NAME") -Fallback "asset-allocation-ui"
        $selected = $null
        if (-not [string]::IsNullOrWhiteSpace($configuredName)) {
            $selected = Select-PreferredItem -Items (Get-ContainerApps) -Preferred $configuredName
        }
        if ($null -eq $selected) {
            $selected = Select-PreferredItem -Items (Get-ContainerApps) -Preferred "asset-allocation-ui" -Contains @("asset", "ui") -AllowSingleItemFallback
        }
        $script:UiContainerAppName = if ($selected) { [string]$selected.name } else { $configuredName }
    }
    return $script:UiContainerAppName
}

function Get-ContainerApp {
    param([Parameter(Mandatory = $true)][string]$AppName)
    if ([string]::IsNullOrWhiteSpace($AppName)) { return $null }
    if (-not $script:ContainerAppDetails.ContainsKey($AppName)) {
        $app = Invoke-JsonCommand -FilePath "az" -ArgumentList @("containerapp", "show", "--name", $AppName, "--resource-group", (Get-ResourceGroupName), "-o", "json")
        if ($null -eq $app) {
            $match = Select-PreferredItem -Items (Get-ContainerApps) -Preferred $AppName -Contains @("asset", "api") -AllowSingleItemFallback
            if ($match) { $app = $match }
        }
        $script:ContainerAppDetails[$AppName] = $app
    }
    return $script:ContainerAppDetails[$AppName]
}

function Get-ContainerAppIdentityName {
    param([AllowNull()]$App, [string]$PreferredName = "")
    $userAssignedIdentities = Get-NestedObjectPropertyValue -Object $App -PropertyPath @("identity", "userAssignedIdentities")
    if ($null -eq $userAssignedIdentities) { return "" }
    $entries = @($userAssignedIdentities.PSObject.Properties)
    if (-not [string]::IsNullOrWhiteSpace($PreferredName)) {
        foreach ($entry in $entries) {
            $name = Get-ResourceLeafName -ResourceId ([string]$entry.Name)
            if ($name -eq $PreferredName) { return $name }
        }
    }
    if ($entries.Count -gt 0) { return (Get-ResourceLeafName -ResourceId ([string]$entries[0].Name)) }
    return ""
}

function Get-ContainerAppIdentityClientId {
    param([AllowNull()]$App, [string]$IdentityName = "")
    $userAssignedIdentities = Get-NestedObjectPropertyValue -Object $App -PropertyPath @("identity", "userAssignedIdentities")
    if ($null -eq $userAssignedIdentities) { return "" }
    $entries = @($userAssignedIdentities.PSObject.Properties)
    if (-not [string]::IsNullOrWhiteSpace($IdentityName)) {
        foreach ($entry in $entries) {
            $name = Get-ResourceLeafName -ResourceId ([string]$entry.Name)
            if ($name -ne $IdentityName) { continue }
            if ($entry.Value -and $entry.Value.clientId) { return [string]$entry.Value.clientId }
        }
    }
    foreach ($entry in $entries) {
        if ($entry.Value -and $entry.Value.clientId) { return [string]$entry.Value.clientId }
    }
    return ""
}

function Get-UserAssignedIdentityClientId {
    param([string]$IdentityName = "")
    if ([string]::IsNullOrWhiteSpace($IdentityName)) { return "" }
    foreach ($identity in @(Get-UserAssignedIdentities)) {
        if ([string]$identity.name -ne $IdentityName) { continue }
        if ($identity.clientId) { return [string]$identity.clientId }
    }
    return ""
}

function Resolve-AcrPullIdentityClientId {
    param([string]$IdentityName = "")
    if ([string]::IsNullOrWhiteSpace($IdentityName)) { return "" }
    $app = Get-ContainerApp -AppName (Get-ApiContainerAppName)
    $clientId = Get-ContainerAppIdentityClientId -App $app -IdentityName $IdentityName
    if (-not [string]::IsNullOrWhiteSpace($clientId)) { return $clientId }
    return (Get-UserAssignedIdentityClientId -IdentityName $IdentityName)
}

function Assert-DeployAzureClientIdIsNotAcrPullIdentity {
    param([Parameter(Mandatory = $true)][object[]]$Results)

    $deployClientId = ""
    $acrPullIdentityName = ""
    foreach ($result in $Results) {
        if ($result.Name -eq "AZURE_CLIENT_ID") {
            $deployClientId = Normalize-EnvValue -Value ([string]$result.Value)
            continue
        }
        if ($result.Name -eq "ACR_PULL_IDENTITY_NAME") {
            $acrPullIdentityName = Normalize-EnvValue -Value ([string]$result.Value)
        }
    }

    if ([string]::IsNullOrWhiteSpace($deployClientId) -or [string]::IsNullOrWhiteSpace($acrPullIdentityName)) { return }
    $acrPullClientId = Resolve-AcrPullIdentityClientId -IdentityName $acrPullIdentityName
    if ([string]::IsNullOrWhiteSpace($acrPullClientId)) { return }
    if ($deployClientId -ne $acrPullClientId) { return }

    throw "AZURE_CLIENT_ID points at the ACR pull managed identity '$acrPullIdentityName'. Set AZURE_CLIENT_ID to the GitHub Actions Azure app registration client id, not the runtime pull identity."
}

function Get-ContainerAppRedirectUri {
    param([AllowNull()]$App)
    $fqdn = [string](Get-NestedObjectPropertyValue -Object $App -PropertyPath @("properties", "configuration", "ingress", "fqdn"))
    if ([string]::IsNullOrWhiteSpace($fqdn)) { return "" }
    return "https://$fqdn/auth/callback"
}

function Convert-ToAbsoluteOrigin {
    param(
        [AllowNull()][string]$Value,
        [switch]$AssumeHttpsHostname
    )
    $candidate = (Normalize-EnvValue -Value $Value).Trim()
    if ([string]::IsNullOrWhiteSpace($candidate)) { return "" }
    if ($AssumeHttpsHostname -and $candidate -notmatch "^[a-zA-Z][a-zA-Z0-9+\-.]*://") {
        $candidate = "https://$candidate"
    }
    try {
        $uri = [Uri]$candidate
    } catch {
        return ""
    }
    if (-not $uri.IsAbsoluteUri) { return "" }
    if ($uri.Scheme -notin @("http", "https")) { return "" }
    if ([string]::IsNullOrWhiteSpace($uri.Host)) { return "" }
    return $uri.GetLeftPart([System.UriPartial]::Authority).TrimEnd("/")
}

function Get-ContainerAppIngressOrigin {
    param([AllowNull()]$App)
    $fqdn = [string](Get-NestedObjectPropertyValue -Object $App -PropertyPath @("properties", "configuration", "ingress", "fqdn"))
    if ([string]::IsNullOrWhiteSpace($fqdn)) { return "" }
    return (Convert-ToAbsoluteOrigin -Value $fqdn -AssumeHttpsHostname)
}

function Get-ApiPublicBaseUrl {
    $app = Get-ContainerApp -AppName (Get-ApiContainerAppName)
    if ($null -eq $app) { return "" }
    return (Get-ContainerAppIngressOrigin -App $app)
}

function Join-DistinctCsvValues {
    param([AllowNull()][string[]]$Values)
    $seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $ordered = New-Object System.Collections.Generic.List[string]
    foreach ($value in @($Values)) {
        $normalizedValue = Normalize-EnvValue -Value $value
        foreach ($part in ($normalizedValue -split ",")) {
            $candidate = $part.Trim().TrimEnd("/")
            if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
            if ($seen.Add($candidate)) {
                [void]$ordered.Add($candidate)
            }
        }
    }
    return ($ordered -join ",")
}

function Get-ApiCorsAllowOrigins {
    $uiPublicHostname = Get-ConfiguredValue -Keys @("UI_PUBLIC_HOSTNAME")
    $uiRedirectUri = Get-ConfiguredValue -Keys @("UI_OIDC_REDIRECT_URI")
    $uiApp = Get-ContainerApp -AppName (Get-UiContainerAppName)
    $uiAppRedirectUri = Get-ContainerAppRedirectUri -App $uiApp

    return (Join-DistinctCsvValues -Values @(
        (Convert-ToAbsoluteOrigin -Value $uiPublicHostname -AssumeHttpsHostname),
        (Convert-ToAbsoluteOrigin -Value $uiRedirectUri),
        (Convert-ToAbsoluteOrigin -Value $uiAppRedirectUri)
    ))
}

function Get-ContainerAppEnvironmentName {
    if ($null -eq $script:ContainerAppEnvironmentName) {
        $app = Get-ContainerApp -AppName (Get-ApiContainerAppName)
        $environmentName = ""
        $appProperties = Get-ObjectPropertyValue -Object $app -PropertyName "properties"
        if ($null -ne $appProperties) {
            $environmentName = Get-ResourceLeafName -ResourceId (Get-ObjectStringProperty -Object $appProperties -PropertyName "managedEnvironmentId")
            if ([string]::IsNullOrWhiteSpace($environmentName)) {
                $environmentName = Get-ResourceLeafName -ResourceId (Get-ObjectStringProperty -Object $appProperties -PropertyName "environmentId")
            }
        }
        if ([string]::IsNullOrWhiteSpace($environmentName)) {
            $configuredName = Get-ConfiguredValue -Keys @("CONTAINER_APPS_ENVIRONMENT_NAME") -Fallback "asset-allocation-env"
            $selected = Select-PreferredItem -Items (Get-ContainerAppsEnvironments) -Preferred $configuredName -Contains @("asset", "env") -AllowSingleItemFallback
            if ($selected) { $environmentName = [string]$selected.name }
        }
        $script:ContainerAppEnvironmentName = $environmentName
    }
    return $script:ContainerAppEnvironmentName
}

function Get-ContainerAppEnvironment {
    if ($null -eq $script:ContainerAppEnvironment) {
        $envName = Get-ContainerAppEnvironmentName
        $environment = $null
        if (-not [string]::IsNullOrWhiteSpace($envName)) {
            $environment = Invoke-JsonCommand -FilePath "az" -ArgumentList @("containerapp", "env", "show", "--name", $envName, "--resource-group", (Get-ResourceGroupName), "-o", "json")
        }
        if ($null -eq $environment) {
            $environment = Select-PreferredItem -Items (Get-ContainerAppsEnvironments) -Preferred $envName -Contains @("asset", "env") -AllowSingleItemFallback
        }
        $script:ContainerAppEnvironment = $environment
    }
    return $script:ContainerAppEnvironment
}

function Get-ManagedEnvironmentWorkspaceId {
    $environment = Get-ContainerAppEnvironment
    $customerId = Get-NestedObjectPropertyValue -Object $environment -PropertyPath @("properties", "appLogsConfiguration", "logAnalyticsConfiguration", "customerId")
    if ($null -eq $customerId) { return "" }
    return [string]$customerId
}

function Get-PreferredLogAnalyticsWorkspace {
    if ($null -eq $script:PreferredLogAnalyticsWorkspace) {
        $workspace = $null
        $workspaceId = Get-ManagedEnvironmentWorkspaceId
        if (-not [string]::IsNullOrWhiteSpace($workspaceId)) {
            $match = @((Get-LogAnalyticsWorkspaces) | Where-Object { (Get-ObjectStringProperty -Object $_ -PropertyName "customerId") -eq $workspaceId } | Select-Object -First 1)
            if ($match.Count -gt 0) { $workspace = $match[0] }
        }
        if ($null -eq $workspace) {
            $configuredName = Get-ConfiguredValue -Keys @("LOG_ANALYTICS_WORKSPACE_NAME") -Fallback "asset-allocation-law"
            $workspace = Select-PreferredItem -Items (Get-LogAnalyticsWorkspaces) -Preferred $configuredName -Contains @("asset", "law", "log") -AllowSingleItemFallback
        }
        $script:PreferredLogAnalyticsWorkspace = $workspace
    }
    return $script:PreferredLogAnalyticsWorkspace
}

function Get-PreferredPostgresServer {
    if ($null -eq $script:PreferredPostgresServer) {
        $configuredName = Get-ConfiguredValue -Keys @("POSTGRES_SERVER_NAME") -Fallback "pg-asset-allocation"
        $script:PreferredPostgresServer = Select-PreferredItem -Items (Get-PostgresServers) -Preferred $configuredName -Contains @("pg", "asset", "alloc") -AllowSingleItemFallback
    }
    return $script:PreferredPostgresServer
}

function Get-PostgresDatabases {
    param([Parameter(Mandatory = $true)][string]$ServerName)
    if ([string]::IsNullOrWhiteSpace($ServerName)) { return @() }
    if (-not $script:PostgresDatabases.ContainsKey($ServerName)) {
        $script:PostgresDatabases[$ServerName] = Get-ItemsFromAzure -Arguments @("postgres", "flexible-server", "db", "list", "--resource-group", (Get-ResourceGroupName), "--server-name", $ServerName, "-o", "json")
    }
    return @($script:PostgresDatabases[$ServerName])
}

function Get-PreferredPostgresDatabaseName {
    $server = Get-PreferredPostgresServer
    if ($null -eq $server -or [string]::IsNullOrWhiteSpace([string]$server.name)) { return "" }

    $configuredName = Get-ConfiguredValue -Keys @("POSTGRES_DATABASE_NAME") -Fallback "asset_allocation"
    $selected = Select-PreferredItem -Items (Get-PostgresDatabases -ServerName ([string]$server.name)) -Preferred $configuredName -Contains @("asset", "allocation") -AllowSingleItemFallback
    if ($selected -and $selected.name) { return [string]$selected.name }
    return $configuredName
}

function Get-ContainerAppRuntimeEnvMap {
    if ($null -eq $script:ContainerAppRuntimeEnv) {
        $map = @{}
        $app = Get-ContainerApp -AppName (Get-ApiContainerAppName)
        $template = Get-NestedObjectPropertyValue -Object $app -PropertyPath @("properties", "template")
        if ($null -ne $template) {
            # Use deployed runtime env as a high-signal bootstrap source for repo vars.
            foreach ($container in @(Get-ObjectPropertyValue -Object $template -PropertyName "containers")) {
                $envEntries = @(Get-ObjectPropertyValue -Object $container -PropertyName "env")
                foreach ($entry in $envEntries) {
                    $name = Get-ObjectStringProperty -Object $entry -PropertyName "name"
                    if ([string]::IsNullOrWhiteSpace($name)) { continue }
                    $value = Normalize-QuotedScalarValue -Key $name -Value (Get-ObjectStringProperty -Object $entry -PropertyName "value") -Source "azure-runtime"
                    $value = Normalize-EnvValueForKey -Name $name -Value $value
                    if ([string]::IsNullOrWhiteSpace($value)) { continue }
                    if (-not $map.ContainsKey($name)) { $map[$name] = $value }
                }
            }
            if (-not $map.ContainsKey("UI_OIDC_REDIRECT_URI")) {
                $uiApp = Get-ContainerApp -AppName (Get-UiContainerAppName)
                $redirectUri = Get-ContainerAppRedirectUri -App $uiApp
                if (-not [string]::IsNullOrWhiteSpace($redirectUri)) { $map["UI_OIDC_REDIRECT_URI"] = $redirectUri }
            }
            if (-not $map.ContainsKey("API_PUBLIC_BASE_URL")) {
                $apiPublicBaseUrl = Get-ApiPublicBaseUrl
                if (-not [string]::IsNullOrWhiteSpace($apiPublicBaseUrl)) { $map["API_PUBLIC_BASE_URL"] = $apiPublicBaseUrl }
            }
        }
        $script:ContainerAppRuntimeEnv = $map
    }
    return $script:ContainerAppRuntimeEnv
}

function Get-RuntimeEnvValue {
    param([Parameter(Mandatory = $true)][string]$Key)
    $map = Get-ContainerAppRuntimeEnvMap
    if ($map.ContainsKey($Key) -and -not [string]::IsNullOrWhiteSpace($map[$Key])) { return $map[$Key] }
    return ""
}

function Get-EntraApp {
    param([Parameter(Mandatory = $true)][string]$DisplayName)
    if (-not $script:EntraApps.ContainsKey($DisplayName)) {
        $selected = $null
        $apps = Invoke-JsonCommand -FilePath "az" -ArgumentList @("ad", "app", "list", "--display-name", $DisplayName, "-o", "json")
        if ($apps) {
            $exact = @($apps | Where-Object { (Get-ObjectStringProperty -Object $_ -PropertyName "displayName") -eq $DisplayName } | Select-Object -First 1)
            if ($exact.Count -gt 0) {
                $selected = $exact[0]
            } elseif (@($apps).Count -eq 1) {
                $selected = @($apps)[0]
            }
        }
        $script:EntraApps[$DisplayName] = $selected
    }
    return $script:EntraApps[$DisplayName]
}

function Get-ApiEntraApp {
    $displayName = Get-ApiContainerAppName
    if ([string]::IsNullOrWhiteSpace($displayName)) { $displayName = "asset-allocation-api" }
    return (Get-EntraApp -DisplayName $displayName)
}

function Get-UiEntraApp {
    return (Get-EntraApp -DisplayName "asset-allocation-ui")
}

function Get-OidcAuthority {
    $account = Get-AzureAccount
    if ($account -and $account.tenantId) { return "https://login.microsoftonline.com/$([string]$account.tenantId)" }
    return ""
}

function Get-OidcIssuer {
    $authority = Get-OidcAuthority
    if (-not [string]::IsNullOrWhiteSpace($authority)) { return "$authority/v2.0" }
    return ""
}

function New-Resolution {
    param(
        [string]$Key = "",
        [AllowEmptyString()][string]$Value = "",
        [string]$Source = "default",
        [bool]$PromptRequired = $false
    )
    $normalizedValue = Normalize-EnvValue -Value $Value
    if (-not [string]::IsNullOrWhiteSpace($Key) -and $Source -in @("github", "azure", "azure-runtime")) {
        $normalizedValue = Normalize-QuotedScalarValue -Key $Key -Value $normalizedValue -Source $Source
    }
    return @{ Value = $normalizedValue; Source = $Source; PromptRequired = $PromptRequired }
}

function Resolve-DiscoveredValue {
    param([Parameter(Mandatory = $true)][string]$Key)

    $githubValue = Get-GitHubVariableValue -Name $Key
    if (-not [string]::IsNullOrWhiteSpace($githubValue)) {
        return (New-Resolution -Key $Key -Value $githubValue -Source "github")
    }

    $runtimeValue = Get-RuntimeEnvValue -Key $Key
    if (-not [string]::IsNullOrWhiteSpace($runtimeValue)) {
        return (New-Resolution -Key $Key -Value $runtimeValue -Source "azure-runtime")
    }

    switch ($Key) {
        "AZURE_TENANT_ID" {
            $account = Get-AzureAccount
            if ($account -and $account.tenantId) { return (New-Resolution -Key $Key -Value ([string]$account.tenantId) -Source "azure") }
        }
        "AZURE_SUBSCRIPTION_ID" {
            $account = Get-AzureAccount
            if ($account -and $account.id) { return (New-Resolution -Key $Key -Value ([string]$account.id) -Source "azure") }
        }
        "RESOURCE_GROUP" {
            $resourceGroupName = Get-ResourceGroupName
            if (-not [string]::IsNullOrWhiteSpace($resourceGroupName)) { return (New-Resolution -Key $Key -Value $resourceGroupName -Source "azure") }
        }
        "AZURE_LOCATION" {
            $resourceGroup = Get-AzureResourceGroup
            if ($resourceGroup -and $resourceGroup.location) { return (New-Resolution -Key $Key -Value ([string]$resourceGroup.location).ToLowerInvariant() -Source "azure") }
        }
        "ACR_NAME" {
            $configuredName = Get-ConfiguredValue -Keys @("ACR_NAME") -Fallback "assetallocationacr"
            $selected = Select-PreferredItem -Items (Get-AcrRegistries) -Preferred $configuredName -Contains @("asset", "acr") -AllowSingleItemFallback
            if ($selected) { return (New-Resolution -Key $Key -Value ([string]$selected.name) -Source "azure") }
        }
        "ACR_PULL_IDENTITY_NAME" {
            $app = Get-ContainerApp -AppName (Get-ApiContainerAppName)
            $configuredName = Get-ConfiguredValue -Keys @("ACR_PULL_IDENTITY_NAME") -Fallback "asset-allocation-acr-pull-mi"
            $identityName = Get-ContainerAppIdentityName -App $app -PreferredName $configuredName
            if (-not [string]::IsNullOrWhiteSpace($identityName)) { return (New-Resolution -Key $Key -Value $identityName -Source "azure") }
            $selected = Select-PreferredItem -Items (Get-UserAssignedIdentities) -Preferred $configuredName -Contains @("acr", "pull") -AllowSingleItemFallback
            if ($selected) { return (New-Resolution -Key $Key -Value ([string]$selected.name) -Source "azure") }
        }
        "SERVICE_ACCOUNT_NAME" {
            $configuredName = Get-ConfiguredValue -Keys @("SERVICE_ACCOUNT_NAME") -Fallback ""
            $selected = Select-PreferredItem -Items (Get-UserAssignedIdentities) -Preferred $configuredName -Contains @("service", "sa")
            if ($selected) { return (New-Resolution -Key $Key -Value ([string]$selected.name) -Source "azure") }
        }
        "CONTAINER_APPS_ENVIRONMENT_NAME" {
            $envName = Get-ContainerAppEnvironmentName
            if (-not [string]::IsNullOrWhiteSpace($envName)) { return (New-Resolution -Key $Key -Value $envName -Source "azure") }
        }
        "LOG_ANALYTICS_WORKSPACE_NAME" {
            $workspace = Get-PreferredLogAnalyticsWorkspace
            if ($workspace -and $workspace.name) { return (New-Resolution -Key $Key -Value ([string]$workspace.name) -Source "azure") }
        }
        "AZURE_STORAGE_ACCOUNT_NAME" {
            $configuredName = Get-ConfiguredValue -Keys @("AZURE_STORAGE_ACCOUNT_NAME") -Fallback "assetallocstorage001"
            $selected = Select-PreferredItem -Items (Get-StorageAccounts) -Preferred $configuredName -Contains @("asset", "storage") -AllowSingleItemFallback
            if ($selected) { return (New-Resolution -Key $Key -Value ([string]$selected.name) -Source "azure") }
        }
        "POSTGRES_SERVER_NAME" {
            $server = Get-PreferredPostgresServer
            if ($server -and $server.name) { return (New-Resolution -Key $Key -Value ([string]$server.name) -Source "azure") }
        }
        "POSTGRES_DATABASE_NAME" {
            $databaseName = Get-PreferredPostgresDatabaseName
            if (-not [string]::IsNullOrWhiteSpace($databaseName)) { return (New-Resolution -Key $Key -Value $databaseName -Source "azure") }
        }
        "POSTGRES_ADMIN_USER" {
            $server = Get-PreferredPostgresServer
            if ($server -and $server.administratorLogin) { return (New-Resolution -Key $Key -Value ([string]$server.administratorLogin) -Source "azure") }
        }
        "AZURE_CLIENT_ID" {
            return $null
        }
        "API_APP_NAME" {
            $appName = Get-ApiContainerAppName
            if (-not [string]::IsNullOrWhiteSpace($appName)) { return (New-Resolution -Key $Key -Value $appName -Source "azure") }
        }
        "API_OIDC_ISSUER" {
            $issuer = Get-OidcIssuer
            if (-not [string]::IsNullOrWhiteSpace($issuer)) { return (New-Resolution -Key $Key -Value $issuer -Source "azure") }
        }
        "API_OIDC_AUDIENCE" {
            $app = Get-ApiEntraApp
            if ($app -and $app.appId) { return (New-Resolution -Key $Key -Value ([string]$app.appId) -Source "azure") }
        }
        "UI_OIDC_CLIENT_ID" {
            $app = Get-UiEntraApp
            if ($app -and $app.appId) { return (New-Resolution -Key $Key -Value ([string]$app.appId) -Source "azure") }
        }
        "UI_OIDC_AUTHORITY" {
            $authority = Get-OidcAuthority
            if (-not [string]::IsNullOrWhiteSpace($authority)) { return (New-Resolution -Key $Key -Value $authority -Source "azure") }
        }
        "UI_OIDC_SCOPES" {
            $app = Get-ApiEntraApp
            if ($app -and $app.appId) {
                return (New-Resolution -Key $Key -Value "api://$([string]$app.appId)/user_impersonation openid profile offline_access" -Source "azure")
            }
        }
        "UI_OIDC_REDIRECT_URI" {
            $app = Get-ContainerApp -AppName (Get-UiContainerAppName)
            $redirectUri = Get-ContainerAppRedirectUri -App $app
            if (-not [string]::IsNullOrWhiteSpace($redirectUri)) { return (New-Resolution -Key $Key -Value $redirectUri -Source "azure") }
        }
        "API_CORS_ALLOW_ORIGINS" {
            $origins = Get-ApiCorsAllowOrigins
            if (-not [string]::IsNullOrWhiteSpace($origins)) {
                return (New-Resolution -Key $Key -Value $origins -Source "azure")
            }
        }
        "API_PUBLIC_BASE_URL" {
            $origin = Get-ApiPublicBaseUrl
            if (-not [string]::IsNullOrWhiteSpace($origin)) {
                return (New-Resolution -Key $Key -Value $origin -Source "azure")
            }
        }
        "CONTRACTS_REPOSITORY" {
            $slug = Get-RepoSlug -RepoName "asset-allocation-contracts"
            if (-not [string]::IsNullOrWhiteSpace($slug)) { return (New-Resolution -Key $Key -Value $slug -Source "github") }
        }
        "CONTRACTS_REF" {
            $slug = Get-RepoSlug -RepoName "asset-allocation-contracts"
            $branch = Get-GitHubRepoDefaultBranch -RepoSlug $slug
            if (-not [string]::IsNullOrWhiteSpace($branch)) { return (New-Resolution -Key $Key -Value $branch -Source "github") }
        }
        "JOBS_REPOSITORY" {
            $slug = Get-RepoSlug -RepoName "asset-allocation-jobs"
            if (-not [string]::IsNullOrWhiteSpace($slug)) { return (New-Resolution -Key $Key -Value $slug -Source "github") }
        }
        "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID" {
            $workspaceId = Get-ManagedEnvironmentWorkspaceId
            if (-not [string]::IsNullOrWhiteSpace($workspaceId)) { return (New-Resolution -Key $Key -Value $workspaceId -Source "azure") }
            $workspace = Get-PreferredLogAnalyticsWorkspace
            if ($workspace -and $workspace.customerId) { return (New-Resolution -Key $Key -Value ([string]$workspace.customerId) -Source "azure") }
        }
    }
    return (New-Resolution -Key $Key)
}

function Prompt-PlainValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Suggestion = "",
        [string]$Description = "",
        [Parameter(Mandatory = $true)][string]$Requirement
    )
    if ($Description) { Write-Host "# $Description" -ForegroundColor DarkGray }
    Write-Host ("# Requirement: {0}" -f $Requirement) -ForegroundColor DarkGray
    Write-Host ("# Suggested default: {0}" -f (Format-SuggestedDisplayValue -Value $Suggestion -Requirement $Requirement)) -ForegroundColor DarkGray
    $promptLabel = if ([string]::IsNullOrWhiteSpace($Suggestion)) { $Name } else { "$Name [$Suggestion]" }
    while ($true) {
        $input = Read-Host $promptLabel
        if (-not [string]::IsNullOrWhiteSpace($input)) { return $input }
        if (-not [string]::IsNullOrWhiteSpace($Suggestion)) { return $Suggestion }
        if ($Requirement -ne "required") { return "" }
        Write-Host ("# {0} is required and cannot be blank." -f $Name) -ForegroundColor Yellow
    }
}

function Prompt-SecretValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Description = "",
        [Parameter(Mandatory = $true)][string]$Requirement
    )
    if ($Description) { Write-Host "# $Description" -ForegroundColor DarkGray }
    Write-Host ("# Requirement: {0}" -f $Requirement) -ForegroundColor DarkGray
    Write-Host ("# Suggested default: {0}" -f (Format-SuggestedDisplayValue -Value "" -Requirement $Requirement -IsSecret $true)) -ForegroundColor DarkGray
    while ($true) {
        $secure = Read-Host "$Name [secret]" -AsSecureString
        $value = ConvertFrom-SecureStringPlain -Secure $secure
        if (-not [string]::IsNullOrWhiteSpace($value) -or $Requirement -ne "required") { return $value }
        Write-Host ("# {0} is required and cannot be blank." -f $Name) -ForegroundColor Yellow
    }
}

function Prompt-SecretFileValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Description = "",
        [Parameter(Mandatory = $true)][string]$Requirement
    )
    if ($Description) { Write-Host "# $Description" -ForegroundColor DarkGray }
    Write-Host ("# Requirement: {0}" -f $Requirement) -ForegroundColor DarkGray
    Write-Host "# Suggested default: <path to PEM/private-key file>" -ForegroundColor DarkGray
    while ($true) {
        $path = Read-Host "$Name file path"
        if (-not [string]::IsNullOrWhiteSpace($path)) { return (Read-SecretFileValue -Path $path) }
        if ($Requirement -ne "required") { return "" }
        Write-Host ("# {0} is required and cannot be blank." -f $Name) -ForegroundColor Yellow
    }
}

$results = New-Object System.Collections.Generic.List[object]
foreach ($row in $contractRows) {
    $name = $row.name
    $description = (($row.notes | Out-String).Trim())
    $isSecret = $row.github_storage -eq "secret"
    $requirement = Get-RequirementLevel -Name $name
    $defaultValue = if ($templateMap.ContainsKey($name)) { Normalize-EnvValueForKey -Name $name -Value $templateMap[$name] } else { "" }

    if ($secretFileOverrideMap.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($secretFileOverrideMap[$name])) {
        $value = Normalize-EnvValue -Value $secretFileOverrideMap[$name]
        $results.Add([pscustomobject]@{ Name = $name; Value = $value; SuggestedValue = $value; Requirement = $requirement; Source = "file"; IsSecret = $isSecret; PromptRequired = $false })
        continue
    }
    $existingValue = if ($existingMap.ContainsKey($name)) { Normalize-EnvValueForKey -Name $name -Value $existingMap[$name] } else { "" }
    if (-not [string]::IsNullOrWhiteSpace($existingValue)) {
        $value = $existingValue
        $results.Add([pscustomobject]@{ Name = $name; Value = $value; SuggestedValue = $value; Requirement = $requirement; Source = "existing"; IsSecret = $isSecret; PromptRequired = $false })
        continue
    }
    $overrideValue = if ($overrideMap.ContainsKey($name)) { Normalize-EnvValueForKey -Name $name -Value $overrideMap[$name] } else { "" }
    if (-not [string]::IsNullOrWhiteSpace($overrideValue)) {
        $value = $overrideValue
        $results.Add([pscustomobject]@{ Name = $name; Value = $value; SuggestedValue = $value; Requirement = $requirement; Source = "prompted"; IsSecret = $isSecret; PromptRequired = $false })
        continue
    }

    if ($isSecret -and (Test-GitHubSecretExists -Name $name)) {
        $results.Add([pscustomobject]@{ Name = $name; Value = ""; SuggestedValue = ""; Requirement = $requirement; Source = "github-secret"; IsSecret = $true; PromptRequired = $false })
        continue
    }

    if (-not $isSecret) {
        $discovered = Resolve-DiscoveredValue -Key $name
        $discoveredValue = Get-ObjectStringProperty -Object $discovered -PropertyName "Value"
        $discoveredSource = Get-ObjectStringProperty -Object $discovered -PropertyName "Source"
        if (-not [string]::IsNullOrWhiteSpace($discoveredValue)) {
            $results.Add([pscustomobject]@{ Name = $name; Value = $discoveredValue; SuggestedValue = $discoveredValue; Requirement = $requirement; Source = $discoveredSource; IsSecret = $false; PromptRequired = $false })
            continue
        }
        if ($DryRun) {
            $results.Add([pscustomobject]@{ Name = $name; Value = $defaultValue; SuggestedValue = $defaultValue; Requirement = $requirement; Source = "default"; IsSecret = $false; PromptRequired = $true })
            continue
        }
        $value = Prompt-PlainValue -Name $name -Suggestion $defaultValue -Description $description -Requirement $requirement
        $source = if ([string]::IsNullOrWhiteSpace($value) -or $value -eq $defaultValue) { "default" } else { "prompted" }
        $results.Add([pscustomobject]@{ Name = $name; Value = (Normalize-EnvValueForKey -Name $name -Value $value); SuggestedValue = $defaultValue; Requirement = $requirement; Source = $source; IsSecret = $false; PromptRequired = $false })
        continue
    }

    if ($DryRun) {
        $results.Add([pscustomobject]@{ Name = $name; Value = $defaultValue; SuggestedValue = $defaultValue; Requirement = $requirement; Source = "default"; IsSecret = $true; PromptRequired = $true })
        continue
    }
    $secretValue = if ($name -eq "DISPATCH_APP_PRIVATE_KEY") {
        Prompt-SecretFileValue -Name $name -Description $description -Requirement $requirement
    } else {
        Prompt-SecretValue -Name $name -Description $description -Requirement $requirement
    }
    $secretSource = if ([string]::IsNullOrWhiteSpace($secretValue)) {
        "default"
    } elseif ($name -eq "DISPATCH_APP_PRIVATE_KEY") {
        "file"
    } else {
        "prompted"
    }
    $results.Add([pscustomobject]@{ Name = $name; Value = (Normalize-EnvValue -Value $secretValue); SuggestedValue = $defaultValue; Requirement = $requirement; Source = $secretSource; IsSecret = $true; PromptRequired = $false })
}

Assert-DeployAzureClientIdIsNotAcrPullIdentity -Results $results.ToArray()

$lines = foreach ($result in $results) { "{0}={1}" -f $result.Name, $result.Value }
Write-Host "Target env file: $EnvFilePath" -ForegroundColor Cyan
foreach ($result in $results) {
    $displayValue = if ($result.IsSecret) { "<redacted>" } else { $result.Value }
    $suggestedDisplay = Format-SuggestedDisplayValue -Value $result.SuggestedValue -Requirement $result.Requirement -IsSecret $result.IsSecret
    $valuePresent = Test-ResolvedValuePresent -Value $result.Value -Source $result.Source -PromptRequired $result.PromptRequired
    Write-Host ("{0}={1} [requirement={2}; suggested={3}; source={4}; prompt_required={5}; value_present={6}]" -f $result.Name, $displayValue, $result.Requirement, $suggestedDisplay, $result.Source, $result.PromptRequired.ToString().ToLowerInvariant(), $valuePresent.ToString().ToLowerInvariant())
}
Write-NormalizedQuotedScalarWarnings
if ($DryRun) {
    $requiredPending = @($results | Where-Object { $_.PromptRequired -and $_.Requirement -eq "required" })
    $optionalPending = @($results | Where-Object { $_.PromptRequired -and $_.Requirement -eq "optional" })
    Write-Host ""
    if ($requiredPending.Count -gt 0) {
        Write-Host ("# Required values still needing input: {0}" -f ($requiredPending.Name -join ", ")) -ForegroundColor Yellow
    } else {
        Write-Host "# Required values are fully resolved." -ForegroundColor Green
    }
    if ($optionalPending.Count -gt 0) {
        Write-Host ("# Optional values still available for input: {0}" -f ($optionalPending.Name -join ", ")) -ForegroundColor DarkYellow
    } else {
        Write-Host "# Optional values are fully resolved." -ForegroundColor Green
    }
    Write-Host ""
    Write-Host "# Preview (.env.web)" -ForegroundColor Cyan
    foreach ($result in $results) {
        $displayValue = if ($result.IsSecret) { "<redacted>" } else { $result.Value }
        Write-Host ("{0}={1}" -f $result.Name, $displayValue)
    }
    return
}
Set-Content -Path $EnvFilePath -Value $lines -Encoding utf8
Write-Host "Wrote $EnvFilePath" -ForegroundColor Green
