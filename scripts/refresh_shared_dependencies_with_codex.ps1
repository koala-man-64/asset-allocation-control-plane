param(
    [string]$RepoRoot = "",
    [ValidateSet("danger-full-access", "full-auto")]
    [string]$ExecutionMode = "danger-full-access",
    [string]$Model = "",
    [string]$OutputRoot = "",
    [switch]$EnableWebSearch,
    [switch]$Ephemeral,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-FullPath {
    param([Parameter(Mandatory = $true)][string]$Path)
    return [System.IO.Path]::GetFullPath($Path)
}

function Resolve-CodexExecutable {
    $codex = Get-Command "codex" -ErrorAction Stop
    if ($codex.CommandType -eq [System.Management.Automation.CommandTypes]::ExternalScript -and $codex.Path.EndsWith(".ps1")) {
        $cmdPath = [System.IO.Path]::ChangeExtension($codex.Path, ".cmd")
        if (Test-Path $cmdPath) {
            return $cmdPath
        }
    }

    $codexCmd = Get-Command "codex.cmd" -ErrorAction SilentlyContinue
    if ($null -ne $codexCmd -and -not [string]::IsNullOrWhiteSpace($codexCmd.Path)) {
        return $codexCmd.Path
    }

    if (-not [string]::IsNullOrWhiteSpace($codex.Path)) {
        return $codex.Path
    }

    throw "Unable to resolve a runnable codex executable."
}

if (-not (Test-CommandAvailable -Name "codex")) {
    throw "codex CLI was not found on PATH."
}

$codexExecutable = Resolve-CodexExecutable

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

$resolvedRepoRoot = Get-FullPath -Path $RepoRoot
if (-not (Test-Path $resolvedRepoRoot)) {
    throw "Repo root does not exist: $resolvedRepoRoot"
}

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $resolvedRepoRoot "artifacts\codex\shared-dependency-refresh"
}

$resolvedOutputRoot = Get-FullPath -Path $OutputRoot
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDirectory = Join-Path $resolvedOutputRoot $timestamp
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null

$promptPath = Join-Path $runDirectory "prompt.md"
$consoleLogPath = Join-Path $runDirectory "console.log"
$finalMessagePath = Join-Path $runDirectory "final-message.md"
$metadataPath = Join-Path $runDirectory "run-metadata.json"

$prompt = @'
Respect this repository's AGENTS.md and all repo-local instructions.

Use the repo-local agents exactly as named, and keep the workflow compliant:
1. `delivery-orchestrator-agent`
2. `gateway-bookkeeper`
3. `project-workflow-enforcer-agent`
4. `application-project-analyst-technical-explainer` only if more repo context is needed
5. Select only the smallest justified specialist review set
6. `delivery-engineer-agent` as the single primary implementation owner
7. Run any justified hardening agents
8. Run validation in parallel as justified
9. `qa-release-gate-agent` as the final quality gate
10. `technical-writer-dev-advocate` only if docs changes are needed; otherwise mark it `N/A` with a reason

Task:
- Retrieve the latest published versions of `asset-allocation-contracts` and `asset-allocation-runtime-common`.
- Prefer package index evidence such as `python -m pip index versions ...` over guessing.
- Start from the assumption that this is downstream adoption work. State explicitly either `This is a contracts-repo-first change.` or `This is local-only and does not require contracts repo routing.`
- Update every source-of-truth version reference that should move with those releases, including dependency manifests, Docker defaults, tests, docs, and any repo scripts that encode the old versions.
- Make any necessary compatibility fixes required by the new published versions.
- Do not revert unrelated user changes already present in the worktree.
- Before implementation, produce a compact plan and a sign-off table for every selected agent. Do not implement until every selected agent signs off or you record an explicit blocker.
- Do not route the task to every available agent. Select the smallest compliant set and explain why each selected agent is needed.
- After sign-off, implement the change end-to-end, validate it, and leave the repo in a coherent state.
- Run the strongest justified verification. At minimum, run shared dependency compatibility validation plus targeted tests for the touched surfaces.
- If adoption of the latest published versions is blocked by missing upstream contract authoring or unpublished prerequisites, stop before making incompatible local edits and report the precise blocker.

Final response requirements:
- Lead with the outcome.
- Include the selected agents and their sign-off status.
- Include the versions adopted.
- Include the files changed.
- Include the validation performed and results.
- Call out blockers or residual risk.
'@

Set-Content -Path $promptPath -Value $prompt -Encoding utf8

$metadata = [ordered]@{
    repo_root = $resolvedRepoRoot
    execution_mode = $ExecutionMode
    model = $Model
    enable_web_search = [bool]$EnableWebSearch
    ephemeral = [bool]$Ephemeral
    run_directory = $runDirectory
    prompt_path = $promptPath
    final_message_path = $finalMessagePath
    created_at = (Get-Date).ToString("o")
}
$metadata | ConvertTo-Json -Depth 4 | Set-Content -Path $metadataPath -Encoding utf8

$codexArgs = @()
if ($EnableWebSearch) {
    $codexArgs += "--search"
}

$codexArgs += @(
    "exec",
    "-C", $resolvedRepoRoot,
    "-o", $finalMessagePath
)

if (-not [string]::IsNullOrWhiteSpace($Model)) {
    $codexArgs += @("-m", $Model)
}

if ($Ephemeral) {
    $codexArgs += "--ephemeral"
}

switch ($ExecutionMode) {
    "danger-full-access" {
        $codexArgs += "--dangerously-bypass-approvals-and-sandbox"
    }
    "full-auto" {
        $codexArgs += "--full-auto"
    }
}

$codexArgs += "-"

Write-Host "Run directory: $runDirectory"
Write-Host "Prompt file: $promptPath"
Write-Host "Final message file: $finalMessagePath"
Write-Host "Execution mode: $ExecutionMode"
Write-Host "Codex executable: $codexExecutable"

if ($DryRun) {
    Write-Host ""
    Write-Host "Dry run. Would execute:"
    Write-Host ($codexExecutable + " " + ($codexArgs -join " "))
    Write-Host ""
    Write-Host "Prompt preview:"
    Write-Host $prompt
    exit 0
}

$promptText = Get-Content -Path $promptPath -Raw
$promptText | & $codexExecutable @codexArgs 2>&1 | Tee-Object -FilePath $consoleLogPath
$exitCode = $LASTEXITCODE

if ($exitCode -ne 0) {
    throw "codex exec failed with exit code $exitCode. See $consoleLogPath"
}

Write-Host ""
Write-Host "Codex refresh session completed."
Write-Host "Console log: $consoleLogPath"
Write-Host "Final message: $finalMessagePath"
