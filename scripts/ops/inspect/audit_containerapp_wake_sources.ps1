[CmdletBinding()]
param(
  [string]$SubscriptionId = "",
  [string]$ResourceGroup = "",
  [string[]]$AppNames = @("asset-allocation-api"),
  [int]$LookbackHours = 24,
  [string]$EnvFile = "",
  [switch]$SkipRepoScan,
  [switch]$FailOnHighFindings,
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Usage {
  @"
Usage: audit_containerapp_wake_sources.ps1 [-SubscriptionId <sub>] [-ResourceGroup <rg>] [-AppNames <app1,app2>] [-LookbackHours <n>] [-EnvFile <path>] [-SkipRepoScan] [-FailOnHighFindings]

Audits potential wake-up sources for Azure Container Apps, including:
- Scale configuration (min/max replicas, scale rules)
- Ingress exposure and revision mode
- Probe settings (liveness/readiness/startup)
- Current replica state
- Request metrics over a lookback window
- Activity log events that may trigger revisions/restarts
- Repo/workflow sources that may probe app endpoints

Default target app:
- asset-allocation-api
"@
}

if ($Help) {
  Write-Usage
  exit 0
}

if ($LookbackHours -le 0) {
  throw "LookbackHours must be greater than 0."
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

function Write-Section {
  param([Parameter(Mandatory = $true)][string]$Title)
  Write-Host ""
  Write-Host "==== $Title ====" -ForegroundColor Cyan
}

function Resolve-AzCliPath {
  if ($env:OS -eq "Windows_NT") {
    $azCmd = Get-Command az.cmd -ErrorAction SilentlyContinue
    if ($azCmd -and $azCmd.Source) { return $azCmd.Source }
  }
  $az = Get-Command az -ErrorAction SilentlyContinue
  if ($az -and $az.Source) { return $az.Source }
  return $null
}

function Invoke-AzRaw {
  param(
    [Parameter(Mandatory = $true)][string[]]$AzArgs,
    [switch]$AllowFailure
  )
  if (-not $script:AzCliPath) {
    throw "Azure CLI path is not initialized."
  }

  Write-Verbose ("az " + ($AzArgs -join " "))
  $global:LASTEXITCODE = 0
  # When failures are allowed, suppress stderr to avoid terminating native-command
  # errors in some PowerShell hosts while still checking exit code.
  if ($AllowFailure) {
    $raw = & $script:AzCliPath @AzArgs 2>$null
  }
  else {
    $raw = & $script:AzCliPath @AzArgs 2>&1
  }
  $exitCode = $global:LASTEXITCODE
  $text = ""
  if ($raw) {
    if ($raw -is [array]) {
      $text = ($raw -join "`n")
    }
    else {
      $text = [string]$raw
    }
  }

  if ($exitCode -ne 0 -and -not $AllowFailure) {
    throw "az command failed (exit=$exitCode): az $($AzArgs -join ' ')`n$text"
  }

  return [PSCustomObject]@{
    ExitCode = $exitCode
    Output = $text
  }
}

function Invoke-AzJson {
  param(
    [Parameter(Mandatory = $true)][string[]]$AzArgs,
    [switch]$AllowFailure
  )

  $result = Invoke-AzRaw -AzArgs $AzArgs -AllowFailure:$AllowFailure
  if ($result.ExitCode -ne 0) {
    return $null
  }
  if ([string]::IsNullOrWhiteSpace($result.Output)) {
    return $null
  }
  try {
    return $result.Output | ConvertFrom-Json
  }
  catch {
    if ($AllowFailure) {
      Write-Warning "Failed to parse JSON output for: az $($AzArgs -join ' ')"
      return $null
    }
    throw
  }
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
  return $candidateWeb
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
    [string[]]$Lines
  )
  foreach ($key in $Keys) {
    $value = Get-EnvValue -Key $key -Lines $Lines
    if (-not [string]::IsNullOrWhiteSpace($value)) { return $value }
  }
  return $null
}

function Get-PropertyValue {
  param(
    [Parameter(Mandatory = $false)]$Object,
    [Parameter(Mandatory = $true)][string]$PropertyName
  )
  if ($null -eq $Object) { return $null }
  $prop = $Object.PSObject.Properties[$PropertyName]
  if ($null -eq $prop) { return $null }
  return $prop.Value
}

function Get-ContainerEnvVar {
  param(
    [Parameter(Mandatory = $true)]$EnvItems,
    [Parameter(Mandatory = $true)][string]$Name
  )
  foreach ($item in @($EnvItems)) {
    if ([string]::Equals([string]$item.name, $Name, [System.StringComparison]::OrdinalIgnoreCase)) {
      if ($null -ne $item.value) { return [string]$item.value }
      if ($null -ne $item.secretRef) { return "secretRef:$($item.secretRef)" }
      return ""
    }
  }
  return $null
}

function Describe-ScaleRule {
  param($Rule)
  if ($null -eq $Rule) { return "unknown rule" }

  $name = [string](Get-PropertyValue -Object $Rule -PropertyName "name")
  $http = Get-PropertyValue -Object $Rule -PropertyName "http"
  if ($null -ne $http) {
    $meta = @()
    $httpMetadata = Get-PropertyValue -Object $http -PropertyName "metadata"
    if ($null -ne $httpMetadata) {
      foreach ($prop in $httpMetadata.PSObject.Properties) {
        $meta += "$($prop.Name)=$($prop.Value)"
      }
    }
    return "http name=$name " + ($meta -join " ")
  }

  $tcp = Get-PropertyValue -Object $Rule -PropertyName "tcp"
  if ($null -ne $tcp) {
    $connections = Get-PropertyValue -Object $tcp -PropertyName "concurrentConnections"
    return "tcp name=$name concurrentConnections=$connections"
  }

  $custom = Get-PropertyValue -Object $Rule -PropertyName "custom"
  if ($null -ne $custom) {
    $meta = @()
    $customMetadata = Get-PropertyValue -Object $custom -PropertyName "metadata"
    if ($null -ne $customMetadata) {
      foreach ($prop in $customMetadata.PSObject.Properties) {
        $meta += "$($prop.Name)=$($prop.Value)"
      }
    }
    $customType = Get-PropertyValue -Object $custom -PropertyName "type"
    return "custom name=$name type=$customType " + ($meta -join " ")
  }

  $queue = Get-PropertyValue -Object $Rule -PropertyName "azureQueue"
  if ($null -ne $queue) {
    $queueLength = Get-PropertyValue -Object $queue -PropertyName "queueLength"
    return "azureQueue name=$name queueLength=$queueLength"
  }
  return "unknown name=$name"
}

function Add-Finding {
  param(
    [ValidateSet("High", "Medium", "Info")][string]$Severity,
    [string]$App,
    [string]$Source,
    [string]$Evidence,
    [string]$Recommendation
  )

  $script:Findings.Add([PSCustomObject]@{
      Severity = $Severity
      App = $App
      Source = $Source
      Evidence = $Evidence
      Recommendation = $Recommendation
    })
}

function Get-SeverityRank {
  param([string]$Severity)
  switch ($Severity) {
    "High" { return 1 }
    "Medium" { return 2 }
    default { return 3 }
  }
}

$script:AzCliPath = Resolve-AzCliPath
if (-not $script:AzCliPath) {
  throw "Azure CLI not found. Install Azure CLI and retry."
}

$account = Invoke-AzRaw -AzArgs @("account", "show", "--query", "id", "-o", "tsv") -AllowFailure
if ($account.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($account.Output)) {
  throw "Azure CLI is not logged in. Run 'az login' and retry."
}

$envPath = Resolve-EnvFilePath -EnvFileOverride $EnvFile
$envLines = @()
if (Test-Path $envPath) { $envLines = Get-Content $envPath }

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = Get-EnvValueFirst -Keys @("AZURE_SUBSCRIPTION_ID", "SUBSCRIPTION_ID") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $candidateSubs = @($env:AZURE_SUBSCRIPTION_ID, $env:SUBSCRIPTION_ID) | Where-Object {
    -not [string]::IsNullOrWhiteSpace($_)
  }
  if ($candidateSubs.Count -gt 0) {
    $SubscriptionId = [string]$candidateSubs[0]
  }
}
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = ($account.Output -replace "`r", "").Trim()
}

if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = Get-EnvValueFirst -Keys @("RESOURCE_GROUP", "AZURE_RESOURCE_GROUP", "SYSTEM_HEALTH_ARM_RESOURCE_GROUP") -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = $env:RESOURCE_GROUP
}
if ([string]::IsNullOrWhiteSpace($ResourceGroup)) {
  $ResourceGroup = "AssetAllocationRG"
}

$null = Invoke-AzRaw -AzArgs @("account", "set", "--subscription", $SubscriptionId)

$normalizedApps = @($AppNames | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim() } | Sort-Object -Unique)
if ($normalizedApps.Count -eq 0) {
  throw "No app names were provided."
}

$script:Findings = New-Object System.Collections.Generic.List[object]
$appAuditResults = New-Object System.Collections.Generic.List[object]

Write-Section "Container App Wake-Source Audit"
Write-Host ("Subscription  : {0}" -f $SubscriptionId)
Write-Host ("Resource Group: {0}" -f $ResourceGroup)
Write-Host ("Apps          : {0}" -f ($normalizedApps -join ", "))
Write-Host ("Lookback      : {0} hour(s)" -f $LookbackHours)
Write-Host ("Repo Scan     : {0}" -f ($(if ($SkipRepoScan) { "disabled" } else { "enabled" })))

$startTimeUtc = (Get-Date).ToUniversalTime().AddHours(-1 * $LookbackHours)
$endTimeUtc = (Get-Date).ToUniversalTime()
$startIso = $startTimeUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
$endIso = $endTimeUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
$timespan = "{0}/{1}" -f $startIso, $endIso

Write-Verbose ("Metric timespan: {0}" -f $timespan)

foreach ($appName in $normalizedApps) {
  Write-Section ("App: {0}" -f $appName)

  $app = Invoke-AzJson -AzArgs @("containerapp", "show", "--name", $appName, "--resource-group", $ResourceGroup, "-o", "json") -AllowFailure
  if ($null -eq $app) {
    Write-Host ("[ERROR] Container App not found or unreadable: {0}" -f $appName) -ForegroundColor Red
    Add-Finding -Severity High -App $appName -Source "ContainerApp.Show" -Evidence "az containerapp show failed." -Recommendation "Verify app name/resource group and your RBAC permissions."
    continue
  }

  $props = Get-PropertyValue -Object $app -PropertyName "properties"
  $template = Get-PropertyValue -Object $props -PropertyName "template"
  $configuration = Get-PropertyValue -Object $props -PropertyName "configuration"
  $scale = Get-PropertyValue -Object $template -PropertyName "scale"
  $rules = @()
  $rulesValue = Get-PropertyValue -Object $scale -PropertyName "rules"
  if ($null -ne $rulesValue) { $rules = @($rulesValue) }

  $containers = @()
  $containersValue = Get-PropertyValue -Object $template -PropertyName "containers"
  if ($null -ne $containersValue) { $containers = @($containersValue) }
  $containerSummaries = @()
  foreach ($container in $containers) {
    $containerName = [string](Get-PropertyValue -Object $container -PropertyName "name")
    if ([string]::IsNullOrWhiteSpace($containerName)) { $containerName = "(unnamed)" }

    $containerProbes = @()
    $containerProbesValue = Get-PropertyValue -Object $container -PropertyName "probes"
    if ($null -ne $containerProbesValue) { $containerProbes = @($containerProbesValue) }

    $containerEnvItems = @()
    $containerEnvItemsValue = Get-PropertyValue -Object $container -PropertyName "env"
    if ($null -ne $containerEnvItemsValue) { $containerEnvItems = @($containerEnvItemsValue) }

    $containerSummaries += [PSCustomObject]@{
      Name = $containerName
      Probes = $containerProbes
      EnvItems = $containerEnvItems
    }
  }

  $ingress = Get-PropertyValue -Object $configuration -PropertyName "ingress"
  $ingressExternal = $false
  $ingressExternalValue = Get-PropertyValue -Object $ingress -PropertyName "external"
  if ($null -ne $ingressExternalValue) { $ingressExternal = [bool]$ingressExternalValue }
  $ingressFqdn = [string](Get-PropertyValue -Object $ingress -PropertyName "fqdn")
  $revisionMode = [string](Get-PropertyValue -Object $configuration -PropertyName "activeRevisionsMode")

  $minReplicas = 0
  $minReplicasValue = Get-PropertyValue -Object $scale -PropertyName "minReplicas"
  if ($null -ne $minReplicasValue) { $minReplicas = [int]$minReplicasValue }

  $maxReplicas = 0
  $maxReplicasValue = Get-PropertyValue -Object $scale -PropertyName "maxReplicas"
  if ($null -ne $maxReplicasValue) { $maxReplicas = [int]$maxReplicasValue }

  $resourceId = [string](Get-PropertyValue -Object $app -PropertyName "id")
  $runningState = [string](Get-PropertyValue -Object $props -PropertyName "runningStatus")
  if ([string]::IsNullOrWhiteSpace($runningState)) {
    $runningState = [string](Get-PropertyValue -Object $props -PropertyName "runningState")
  }

  Write-Host ("Ingress      : external={0} fqdn={1}" -f $ingressExternal, ($(if ($ingressFqdn) { $ingressFqdn } else { "-" })))
  Write-Host ("Scale        : minReplicas={0} maxReplicas={1} ruleCount={2}" -f $minReplicas, $maxReplicas, $rules.Count)
  Write-Host ("Revisions    : mode={0} latestReadyRevision={1}" -f $revisionMode, [string](Get-PropertyValue -Object $props -PropertyName "latestReadyRevisionName"))
  Write-Host ("Provisioning : state={0} runningState={1}" -f [string](Get-PropertyValue -Object $props -PropertyName "provisioningState"), $(if ($runningState) { $runningState } else { "-" }))
  if ($containerSummaries.Count -gt 0) {
    $containerNames = @($containerSummaries | ForEach-Object { $_.Name })
    Write-Host ("Containers   : {0}" -f ($containerNames -join ", "))
  }

  if ($minReplicas -gt 0) {
    Add-Finding -Severity High -App $appName -Source "Scale.MinReplicas" -Evidence ("minReplicas={0}" -f $minReplicas) -Recommendation "Set minReplicas=0 to permit scale-to-zero."
  }
  else {
    Add-Finding -Severity Info -App $appName -Source "Scale.MinReplicas" -Evidence "minReplicas=0 (scale-to-zero enabled)." -Recommendation "No change needed unless low-latency always-on behavior is desired."
  }

  if ($ingressExternal) {
    Add-Finding -Severity Medium -App $appName -Source "Ingress.External" -Evidence "external=true allows internet traffic to wake replicas." -Recommendation "If public access is not required, set ingress.external=false or restrict ingress upstream."
  }
  else {
    Add-Finding -Severity Info -App $appName -Source "Ingress.External" -Evidence "external=false (internal only)." -Recommendation "No change needed."
  }

  if ($rules.Count -eq 0) {
    Write-Host "Scale Rules  : none configured (HTTP default behavior only)."
    Add-Finding -Severity Info -App $appName -Source "Scale.Rules" -Evidence "No explicit scale rules found in template.scale.rules." -Recommendation "No change needed."
  }
  else {
    Write-Host "Scale Rules  :"
    foreach ($rule in $rules) {
      $desc = Describe-ScaleRule -Rule $rule
      Write-Host ("  - {0}" -f $desc)
      Add-Finding -Severity Medium -App $appName -Source "Scale.Rule" -Evidence ("Configured rule: {0}" -f $desc) -Recommendation "Validate each rule threshold against intended wake behavior."
    }
  }

  $hasProbes = $false
  foreach ($containerInfo in $containerSummaries) {
    if (@($containerInfo.Probes).Count -gt 0) {
      $hasProbes = $true
      break
    }
  }

  if ($hasProbes) {
    Write-Host "Probes       :"
    foreach ($containerInfo in $containerSummaries) {
      foreach ($probe in @($containerInfo.Probes)) {
        $probeType = [string](Get-PropertyValue -Object $probe -PropertyName "type")
        $httpGet = Get-PropertyValue -Object $probe -PropertyName "httpGet"
        $path = [string](Get-PropertyValue -Object $httpGet -PropertyName "path")
        $periodValue = Get-PropertyValue -Object $probe -PropertyName "periodSeconds"
        $delayValue = Get-PropertyValue -Object $probe -PropertyName "initialDelaySeconds"
        $period = if ($null -ne $periodValue) { [int]$periodValue } else { 0 }
        $delay = if ($null -ne $delayValue) { [int]$delayValue } else { 0 }
        Write-Host ("  - container={0} type={1} path={2} initialDelay={3}s period={4}s" -f $containerInfo.Name, $probeType, $path, $delay, $period)
        if ($path -eq "/" -and $containerInfo.Name -match "api") {
          Add-Finding -Severity Info -App $appName -Source "Probe.Path" -Evidence ("container={0} {1} probe uses '/' path." -f $containerInfo.Name, $probeType) -Recommendation "Consider a lightweight /healthz endpoint to reduce probe overhead."
        }
      }
    }
  }
  else {
    Write-Host "Probes       : none"
  }

  foreach ($containerInfo in $containerSummaries) {
    $monitoredContainerApps = Get-ContainerEnvVar -EnvItems $containerInfo.EnvItems -Name "SYSTEM_HEALTH_ARM_CONTAINERAPPS"
    if (-not [string]::IsNullOrWhiteSpace($monitoredContainerApps)) {
      Write-Host ("Env          : container={0} SYSTEM_HEALTH_ARM_CONTAINERAPPS={1}" -f $containerInfo.Name, $monitoredContainerApps)
      Add-Finding -Severity Medium -App $appName -Source "Env.SYSTEM_HEALTH_ARM_CONTAINERAPPS" -Evidence ("container={0} may probe listed apps and wake them when probe=true." -f $containerInfo.Name) -Recommendation "Use probe=false by default for container-app status checks or narrow monitored app list."
    }

    $startupTargets = Get-ContainerEnvVar -EnvItems $containerInfo.EnvItems -Name "JOB_STARTUP_API_CONTAINER_APPS"
    if ([string]::IsNullOrWhiteSpace($startupTargets)) {
      $startupTargets = Get-ContainerEnvVar -EnvItems $containerInfo.EnvItems -Name "API_CONTAINER_APP_NAME"
    }
    if (-not [string]::IsNullOrWhiteSpace($startupTargets)) {
      Write-Host ("Env          : container={0} startup-targets={1}" -f $containerInfo.Name, $startupTargets)
      Add-Finding -Severity Medium -App $appName -Source "Env.JOB_STARTUP_API_CONTAINER_APPS" -Evidence ("container={0} can call ARM start for apps='{1}' when startup health checks fail." -f $containerInfo.Name, $startupTargets) -Recommendation "Confirm startup wake targets are limited to required apps and that RBAC is least privilege."
    }
  }

  $replicas = Invoke-AzJson -AzArgs @("containerapp", "replica", "list", "--name", $appName, "--resource-group", $ResourceGroup, "-o", "json") -AllowFailure
  if ($null -eq $replicas) {
    Write-Host "Replicas     : unable to read replica list." -ForegroundColor Yellow
    Add-Finding -Severity Medium -App $appName -Source "Replica.List" -Evidence "Could not query current replicas." -Recommendation "Ensure Azure CLI version supports 'containerapp replica list' and that your RBAC allows read."
  }
  else {
    $replicaList = @($replicas)
    Write-Host ("Replicas     : activeCount={0}" -f $replicaList.Count)
    foreach ($replica in $replicaList) {
      $repProps = Get-PropertyValue -Object $replica -PropertyName "properties"
      $repName = [string](Get-PropertyValue -Object $replica -PropertyName "name")
      $repRunningState = [string](Get-PropertyValue -Object $repProps -PropertyName "runningState")
      $repProvisioningState = [string](Get-PropertyValue -Object $repProps -PropertyName "provisioningState")
      Write-Host ("  - name={0} runningState={1} provisioningState={2}" -f $repName, $repRunningState, $repProvisioningState)
    }
    if ($replicaList.Count -gt 0) {
      Add-Finding -Severity Info -App $appName -Source "Replica.ActiveNow" -Evidence ("Current active replica count is {0}." -f $replicaList.Count) -Recommendation "Correlate with request metrics and activity logs below to identify wake source."
    }
  }

  $metrics = Invoke-AzJson -AzArgs @(
    "monitor", "metrics", "list",
    "--resource", $resourceId,
    "--metric", "Requests",
    "--aggregation", "Total",
    "--interval", "PT5M",
    "--start-time", $startIso,
    "--end-time", $endIso,
    "-o", "json"
  ) -AllowFailure

  if ($null -eq $metrics) {
    Write-Host "Metrics      : unable to query Requests metric." -ForegroundColor Yellow
    Add-Finding -Severity Medium -App $appName -Source "Metrics.Requests" -Evidence "Could not retrieve request metrics." -Recommendation "Confirm Microsoft.Insights metrics read permissions."
  }
  else {
    $metricValues = @()
    $metricValuesRaw = Get-PropertyValue -Object $metrics -PropertyName "value"
    if ($null -ne $metricValuesRaw) { $metricValues = @($metricValuesRaw) }
    $dataPoints = @()
    foreach ($metric in $metricValues) {
      $seriesValues = @()
      $seriesRaw = Get-PropertyValue -Object $metric -PropertyName "timeseries"
      if ($null -ne $seriesRaw) { $seriesValues = @($seriesRaw) }
      foreach ($series in $seriesValues) {
        $seriesData = Get-PropertyValue -Object $series -PropertyName "data"
        if ($null -ne $seriesData) {
          $dataPoints += @($seriesData)
        }
      }
    }

    $totals = @()
    foreach ($point in $dataPoints) {
      $totalValue = Get-PropertyValue -Object $point -PropertyName "total"
      if ($null -ne $totalValue) {
        $totals += [double]$totalValue
        continue
      }
      $countValue = Get-PropertyValue -Object $point -PropertyName "count"
      if ($null -ne $countValue) {
        $totals += [double]$countValue
      }
    }

    $sumRequests = 0.0
    $peakRequests = 0.0
    $nonZeroBins = 0
    if ($totals.Count -gt 0) {
      $sumRequests = ($totals | Measure-Object -Sum).Sum
      $peakRequests = ($totals | Measure-Object -Maximum).Maximum
      $nonZeroBins = @($totals | Where-Object { $_ -gt 0 }).Count
    }

    Write-Host ("Metrics      : Requests sum={0} peak5m={1} nonZeroBins={2}" -f $sumRequests, $peakRequests, $nonZeroBins)
    if ($sumRequests -gt 0) {
      Add-Finding -Severity Medium -App $appName -Source "Metrics.Requests" -Evidence ("Observed inbound requests in last {0}h (sum={1}, nonZeroBins={2})." -f $LookbackHours, $sumRequests, $nonZeroBins) -Recommendation "Inspect access logs / callers to identify expected vs unexpected traffic."
    }
    else {
      Add-Finding -Severity Info -App $appName -Source "Metrics.Requests" -Evidence ("No request traffic observed in last {0}h." -f $LookbackHours) -Recommendation "No ingress wake source observed in selected window."
    }
  }

  $activity = Invoke-AzJson -AzArgs @(
    "monitor", "activity-log", "list",
    "--resource-id", $resourceId,
    "--offset", ("{0}h" -f $LookbackHours),
    "--max-events", "200",
    "-o", "json"
  ) -AllowFailure

  if ($null -eq $activity) {
    Write-Host "Activity Logs: unable to query activity logs." -ForegroundColor Yellow
    Add-Finding -Severity Info -App $appName -Source "ActivityLog" -Evidence "Activity log query not available in this context." -Recommendation "Ensure Microsoft.Insights/activityLogs/read permission if this signal is required."
  }
  else {
    $events = @($activity)
    $interesting = @()
    foreach ($event in $events) {
      $operationName = Get-PropertyValue -Object $event -PropertyName "operationName"
      $op = [string](Get-PropertyValue -Object $operationName -PropertyName "localizedValue")
      if ([string]::IsNullOrWhiteSpace($op)) {
        $op = [string](Get-PropertyValue -Object $operationName -PropertyName "value")
      }
      $opLower = $op.ToLowerInvariant()
      if ($opLower.Contains("write") -or $opLower.Contains("start") -or $opLower.Contains("stop") -or $opLower.Contains("restart") -or $opLower.Contains("revision")) {
        $interesting += $event
      }
    }

    Write-Host ("Activity Logs: totalEvents={0} interestingEvents={1}" -f $events.Count, $interesting.Count)
    foreach ($evt in ($interesting | Select-Object -First 5)) {
      $evtOpName = Get-PropertyValue -Object $evt -PropertyName "operationName"
      $when = [string](Get-PropertyValue -Object $evt -PropertyName "eventTimestamp")
      $op = [string](Get-PropertyValue -Object $evtOpName -PropertyName "localizedValue")
      if ([string]::IsNullOrWhiteSpace($op)) { $op = [string](Get-PropertyValue -Object $evtOpName -PropertyName "value") }
      $caller = [string](Get-PropertyValue -Object $evt -PropertyName "caller")
      Write-Host ("  - time={0} op={1} caller={2}" -f $when, $op, $(if ($caller) { $caller } else { "-" }))
    }
    if ($interesting.Count -gt 0) {
      Add-Finding -Severity Info -App $appName -Source "ActivityLog.ManagementOps" -Evidence ("Found {0} management operations (write/start/stop/revision) in lookback window." -f $interesting.Count) -Recommendation "Confirm these operations are expected deploy/ops actions."
    }
  }

  $appAuditResults.Add([PSCustomObject]@{
      AppName = $appName
      ResourceId = $resourceId
      IngressExternal = $ingressExternal
      IngressFqdn = $ingressFqdn
      MinReplicas = $minReplicas
      MaxReplicas = $maxReplicas
      ScaleRuleCount = $rules.Count
      RevisionMode = $revisionMode
      RunningState = $runningState
    })
}

if (-not $SkipRepoScan) {
  Write-Section "Repository Wake-Source Scan"

  $repoRoot = Resolve-RepoRoot
  Write-Host ("Repo Root    : {0}" -f $repoRoot)

  $scanTargets = @(
    (Join-Path $repoRoot ".github/workflows")
    (Join-Path $repoRoot "api")
    (Join-Path $repoRoot "ui")
    (Join-Path $repoRoot "scripts")
    (Join-Path $repoRoot "deploy")
    (Join-Path $repoRoot "docs")
  ) | Where-Object { Test-Path $_ }

  $files = @()
  foreach ($target in $scanTargets) {
    $files += Get-ChildItem -Path $target -Recurse -File -ErrorAction SilentlyContinue |
      Where-Object {
        $_.FullName -notmatch "[\\/]\.git[\\/]" -and
        $_.FullName -notmatch "[\\/]node_modules[\\/]" -and
        $_.FullName -notmatch "[\\/]\\.pnpm-store[\\/]" -and
        $_.FullName -notmatch "[\\/]coverage[\\/]" -and
        $_.FullName -notmatch "[\\/]dist[\\/]"
      }
  }

  $patterns = @(
    [PSCustomObject]@{
      Name = "Direct cloud UI/API URL probes"
      Regex = "asset-allocation-ui\..*azurecontainerapps\.io|asset-allocation-api\..*azurecontainerapps\.io|verify_ui_api_health\.py"
      Severity = "Medium"
      Recommendation = "Confirm these scripts are only run manually or under controlled automation."
    },
    [PSCustomObject]@{
      Name = "Container-app probe=true calls"
      Regex = "/system/container-apps\?probe=true|getContainerApps\(\s*\{\s*probe\s*:\s*true"
      Severity = "Medium"
      Recommendation = "Use probe=false default and explicit opt-in for live probes."
    },
    [PSCustomObject]@{
      Name = "Scheduled workflow triggers"
      Regex = "^\s*schedule\s*:"
      Severity = "Medium"
      Recommendation = "Review scheduled jobs to ensure they do not call UI/API health endpoints unintentionally."
    }
  )

  foreach ($pattern in $patterns) {
    $matches = @(
      Select-String -Path $files.FullName -Pattern $pattern.Regex -CaseSensitive:$false -AllMatches -ErrorAction SilentlyContinue
    )
    Write-Host ("Pattern      : {0}" -f $pattern.Name)
    Write-Host ("Match Count  : {0}" -f $matches.Count)

    foreach ($match in ($matches | Select-Object -First 12)) {
      $relative = $match.Path.Replace($repoRoot, ".").TrimStart("\", "/")
      $lineText = ($match.Line.Trim() -replace "\s+", " ")
      Write-Host ("  - {0}:{1} :: {2}" -f $relative, $match.LineNumber, $lineText)
    }

    if ($matches.Count -gt 0) {
      Add-Finding -Severity $pattern.Severity -App "repo" -Source ("RepoScan: {0}" -f $pattern.Name) -Evidence ("Found {0} match(es)." -f $matches.Count) -Recommendation $pattern.Recommendation
    }
    else {
      Add-Finding -Severity Info -App "repo" -Source ("RepoScan: {0}" -f $pattern.Name) -Evidence "No matches found." -Recommendation "No action needed."
    }
  }
}

Write-Section "Audit Summary"

if ($appAuditResults.Count -gt 0) {
  Write-Host "App posture snapshot:"
  $appAuditResults |
    Sort-Object AppName |
    Select-Object AppName, IngressExternal, MinReplicas, MaxReplicas, ScaleRuleCount, RevisionMode, RunningState |
    Format-Table -AutoSize | Out-String | Write-Host
}

$ordered = @($script:Findings | Sort-Object @{ Expression = { Get-SeverityRank -Severity $_.Severity } }, App, Source)
if ($ordered.Count -eq 0) {
  Write-Host "No findings were produced." -ForegroundColor Yellow
  exit 0
}

$highCount = @($ordered | Where-Object { $_.Severity -eq "High" }).Count
$mediumCount = @($ordered | Where-Object { $_.Severity -eq "Medium" }).Count
$infoCount = @($ordered | Where-Object { $_.Severity -eq "Info" }).Count

Write-Host ("Findings: High={0} Medium={1} Info={2}" -f $highCount, $mediumCount, $infoCount)
Write-Host ""

foreach ($f in $ordered) {
  $color = switch ($f.Severity) {
    "High" { "Red" }
    "Medium" { "Yellow" }
    default { "Gray" }
  }
  Write-Host ("[{0}] app={1} source={2}" -f $f.Severity.ToUpperInvariant(), $f.App, $f.Source) -ForegroundColor $color
  Write-Host ("  evidence      : {0}" -f $f.Evidence)
  Write-Host ("  recommendation: {0}" -f $f.Recommendation)
}

if ($FailOnHighFindings -and $highCount -gt 0) {
  Write-Error ("High-severity findings detected: {0}" -f $highCount)
  exit 2
}

Write-Host ""
Write-Host "Audit complete." -ForegroundColor Green
