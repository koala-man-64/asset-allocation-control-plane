from __future__ import annotations

import re
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _provisioner_text() -> str:
    return (
        _repo_root()
        / "scripts"
        / "ops"
        / "provision"
        / "provision_entra_oidc.ps1"
    ).read_text(encoding="utf-8")


def _role_definition_line(text: str, role_value: str) -> str:
    pattern = rf'^\s*\[pscustomobject\]@\{{[^\n]*Value = "{re.escape(role_value)}"[^\n]*$'
    match = re.search(pattern, text, flags=re.MULTILINE)
    assert match is not None, f"missing app role definition for {role_value}"
    return match.group(0)


def test_control_plane_keeps_repo_local_env_bootstrap_scripts() -> None:
    repo_root = _repo_root()
    assert (repo_root / "scripts" / "setup-env.ps1").exists()
    assert (repo_root / "scripts" / "sync-all-to-github.ps1").exists()


def test_interactive_azure_orchestrator_wraps_existing_scripts() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_interactive.ps1"
    text = script.read_text(encoding="utf-8")

    assert "validate_azure_permissions.ps1" in text, (
        "interactive orchestrator must expose the existing Azure permission validation step"
    )
    assert "provision_azure.ps1" in text, (
        "interactive orchestrator must route shared infra through provision_azure.ps1"
    )
    assert 'Add-SwitchArgument -Arguments $sharedArgs -Name "SkipPostgresPrompt" -Enabled $true' in text, (
        "interactive orchestrator must suppress the embedded Postgres prompt when delegating shared infra"
    )
    assert "provision_azure_postgres.ps1" in text, (
        "interactive orchestrator must route Postgres through the dedicated Postgres provisioner"
    )
    assert "configure_cost_guardrails.ps1" in text, (
        "interactive orchestrator must expose the cost guardrails deployment step"
    )
    assert "provision_entra_oidc.ps1" in text, (
        "interactive orchestrator must expose the Entra OIDC provisioning step"
    )
    assert "validate_acr_pull.ps1" in text, (
        "interactive orchestrator must expose the post-provision ACR validation step"
    )


def test_interactive_azure_orchestrator_uses_child_powershell_processes() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_interactive.ps1"
    text = script.read_text(encoding="utf-8")

    assert "Resolve-PowerShellExe" in text, (
        "interactive orchestrator must resolve a child PowerShell executable"
    )
    assert "-ExecutionPolicy Bypass -File $ScriptPath @Arguments" in text, (
        "interactive orchestrator must launch child scripts via a separate PowerShell process"
    )
    assert "Continue to the next step?" in text, (
        "interactive orchestrator must allow the operator to continue after a failed child step"
    )
    assert "Tee-Object -FilePath $logPath" in text, (
        "interactive orchestrator must capture child-script output into step log files"
    )
    assert "Session logs:" in text, (
        "interactive orchestrator must surface the session log directory to the operator"
    )


def test_interactive_azure_orchestrator_offers_github_sync_for_env_web() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_interactive.ps1"
    text = script.read_text(encoding="utf-8")

    assert "Sync .env.web values to GitHub vars/secrets now?" in text, (
        "interactive orchestrator must offer an optional GitHub sync step"
    )
    assert "sync-all-to-github.ps1" in text, (
        "interactive orchestrator must route GitHub sync through the shared helper"
    )


def test_entra_oidc_provisioner_covers_app_registrations_permissions_and_env_updates() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_entra_oidc.ps1"
    text = script.read_text(encoding="utf-8")

    assert 'az ad app create' in text or '"ad", "app", "create"' in text, (
        "Entra provisioner must create app registrations when they do not exist"
    )
    assert 'az ad sp create' in text or '"ad", "sp", "create"' in text, (
        "Entra provisioner must create service principals when they do not exist"
    )
    assert "ENTRA_OPERATOR_USER_OBJECT_ID" in text, (
        "Entra provisioner must assign the operator user from ENTRA_OPERATOR_USER_OBJECT_ID"
    )
    assert "appRoleAssignmentRequired" in text, (
        "Entra provisioner must require app-role assignment on the API enterprise app"
    )
    assert "user_impersonation" in text, (
        "Entra provisioner must expose the delegated user_impersonation scope"
    )
    assert "AssetAllocation.AiRelay.Use" in text, (
        "Entra provisioner must create the dedicated AI relay app role"
    )
    assert "admin-consent" in text, (
        "Entra provisioner must grant admin consent for the UI delegated permission"
    )
    assert "ASSET_ALLOCATION_API_SCOPE" in text, (
        "Entra provisioner must write the managed-identity API scope back into the env file"
    )
    assert "UI_OIDC_REDIRECT_URI" in text, (
        "Entra provisioner must write the resolved redirect URI into the env file"
    )
    assert "UI_PUBLIC_HOSTNAME" in text, (
        "Entra provisioner must persist the stable UI hostname when custom-domain cutover is configured"
    )
    assert "AI_RELAY_REQUIRED_ROLES" in text, (
        "Entra provisioner must persist the AI relay role requirement into the env file"
    )
    assert 'Get-EnvValueFirst -Keys @("AZURE_CLIENT_ID")' in text, (
        "Entra provisioner must resolve the GitHub Actions Azure client id from env state"
    )
    assert 'Deploy Azure client ID:' in text, (
        "Entra provisioner should report which Azure deploy principal receives protected smoke access"
    )
    assert '$deploySpResult = Ensure-ServicePrincipal -AppId $deployAzureClientId' in text, (
        "Entra provisioner must resolve the GitHub Actions deploy service principal"
    )
    assert '-PrincipalObjectId $deployServicePrincipal.id `' in text, (
        "Entra provisioner must assign the API access app role to the GitHub Actions deploy principal"
    )
    assert "Resolve-ManagedIdentityClientId" in text, (
        "Entra provisioner must resolve the ACR pull managed identity client id so it can reject AZURE_CLIENT_ID drift"
    )
    assert "$deployAzureClientId -eq $acrPullIdentityClientId" in text, (
        "Entra provisioner must fail when AZURE_CLIENT_ID points at the runtime ACR pull identity"
    )
    assert "Set AZURE_CLIENT_ID to the GitHub Actions Azure app registration client id" in text, (
        "Entra provisioner should explain how to fix an ACR-pull-versus-deploy-client-id mismatch"
    )
    assert 'logoutUrl = $PublicPostLogoutRedirectUri' in text, (
        "Entra provisioner must register the logout-complete landing route on the UI app registration"
    )
    assert "/auth/logout-complete" in text, (
        "Entra provisioner must derive the logout-complete landing path from the UI redirect origin"
    )


def test_entra_oidc_provisioner_defines_safe_trade_desk_and_account_policy_roles() -> None:
    text = _provisioner_text()
    expected_operator_assignment = {
        "AssetAllocation.TradeDesk.Read": True,
        "AssetAllocation.TradeDesk.Preview": False,
        "AssetAllocation.TradeDesk.Place": False,
        "AssetAllocation.TradeDesk.Cancel": False,
        "AssetAllocation.TradeDesk.Live": False,
        "AssetAllocation.AccountPolicy.Read": True,
        "AssetAllocation.AccountPolicy.Write": False,
        "AssetAllocation.TradeConfirmation.Release": False,
    }

    access_line = _role_definition_line(text, "AssetAllocation.Access")
    assert 'AllowedMemberTypes = @("User", "Application")' in access_line, (
        "Access must remain assignable to deploy/runtime service principals"
    )

    for role_value, should_assign_to_operator in expected_operator_assignment.items():
        line = _role_definition_line(text, role_value)
        expected_assignment = "$true" if should_assign_to_operator else "$false"
        assert f"AssignToOperator = {expected_assignment}" in line, (
            f"{role_value} has the wrong default operator assignment policy"
        )
        assert 'AllowedMemberTypes = @("User")' in line, (
            f"{role_value} should be assignable only to users/groups by default"
        )


def test_entra_oidc_provisioner_pages_app_role_assignment_lookup() -> None:
    text = _provisioner_text()

    assert 'PSObject.Properties["@odata.nextLink"]' in text, (
        "app-role assignment lookup must follow Microsoft Graph pagination"
    )
    assert 'while (-not [string]::IsNullOrWhiteSpace($url))' in text, (
        "app-role assignment lookup must keep reading Graph pages until no nextLink remains"
    )
    assert "Invoke-GraphJson -Method GET -Url $url" in text, (
        "pagination must request the current page URL rather than a fixed first page"
    )


def test_entra_oidc_provisioner_auto_resolves_and_persists_operator_user() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_entra_oidc.ps1"
    text = script.read_text(encoding="utf-8")

    assert "Resolve-OperatorUserAssignment" in text, (
        "Entra provisioner must centralize operator-user resolution"
    )
    assert '"ad", "signed-in-user", "show"' in text, (
        "Entra provisioner must auto-resolve the signed-in operator user when the env is blank"
    )
    assert "Operator user source:" in text, (
        "Entra provisioner should report how the operator user was resolved"
    )
    assert "ENTRA_OPERATOR_USER_OBJECT_ID  = $OperatorUserObjectId" in text, (
        "Entra provisioner must persist the resolved operator user object ID back into the env file"
    )
    assert "Invoke-WithRetry" in text, (
        "Entra provisioner must retry eventually consistent Entra operations"
    )
    assert "Creating service principal for appId" in text, (
        "Entra provisioner should log service-principal creation attempts"
    )
    assert '"--body", "@$tempBodyPath"' in text, (
        "Entra provisioner must send Graph write payloads via a temp file for Windows-safe az rest calls"
    )
    assert "[AllowEmptyString()][string]$ExplicitRedirectUri = \"\"" in text, (
        "Entra provisioner must allow an empty explicit redirect URI so it can derive the callback automatically"
    )
    assert '[string]$UiContainerAppName = ""' in text, (
        "Entra provisioner must allow the UI Container App name to override redirect discovery"
    )
    assert '[string]$UiPublicHostname = ""' in text, (
        "Entra provisioner must allow an explicit stable UI hostname to override redirect discovery"
    )
    assert 'Write-Host "UI container app: $UiContainerAppName"' in text, (
        "Entra provisioner should report which UI Container App drives redirect discovery"
    )
    assert 'Write-Host "UI public hostname:' in text, (
        "Entra provisioner should report whether redirect discovery is using a stable UI hostname"
    )
    assert 'return "https://$candidateHost/auth/callback"' in text, (
        "Entra provisioner must derive the public callback from the stable UI hostname when configured"
    )
    assert '"--query", "properties.configuration.ingress.fqdn"' in text, (
        "Entra provisioner must resolve the redirect from the UI Container App ingress FQDN"
    )
    assert 'return "https://$($uiIngressFqdn.Output.Trim())/auth/callback"' in text, (
        "Entra provisioner must derive the public callback from the UI Container App FQDN"
    )


def test_permission_validator_allows_signed_in_user_fallback_for_operator_assignment() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "validate" / "validate_azure_permissions.ps1"
    text = script.read_text(encoding="utf-8")

    assert "Resolve-SignedInUser" in text, (
        "Azure permission validation must support the signed-in-user fallback"
    )
    assert '"ad", "signed-in-user", "show"' in text, (
        "Azure permission validation must probe the signed-in user when ENTRA_OPERATOR_USER_OBJECT_ID is unset"
    )
    assert "auto-resolved from signed-in user" in text, (
        "Azure permission validation should report when the operator user was auto-resolved"
    )
    assert 'applications?`$top=1' in text, (
        "Azure permission validation must keep the Graph application read probe Windows-safe"
    )
    assert 'servicePrincipals?`$top=1' in text, (
        "Azure permission validation must keep the Graph service principal read probe Windows-safe"
    )
    assert 'users/${OperatorUserObjectId}?`$select=' in text, (
        "Azure permission validation must delimit the operator user variable safely in the Graph user probe"
    )


def test_permission_validator_supports_release_scenario() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "validate" / "validate_azure_permissions.ps1"
    text = script.read_text(encoding="utf-8")

    assert '[ValidateSet("Standard", "Release")][string]$Scenario = "Standard"' in text, (
        "Azure permission validation must expose a release-specific scenario switch"
    )
    assert "-Scenario <Standard|Release>" in text, (
        "Azure permission validation usage should document the release scenario"
    )
    assert '$Scenario -eq "Release"' in text, (
        "Azure permission validation must branch into a release-specific validation path"
    )
    assert 'Add-Result -Name "Resource group exists"' in text, (
        "Release validation must verify resource-group visibility"
    )
    assert 'Add-Result -Name "Deploy SP has AcrPush"' in text, (
        "Release validation must verify ACR push access for the release service principal"
    )


def test_permission_validator_checks_api_log_analytics_reader_access() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "validate" / "validate_azure_permissions.ps1"
    text = script.read_text(encoding="utf-8")

    assert '[string]$LogAnalyticsWorkspaceName = ""' in text, (
        "Azure permission validation must accept the Log Analytics workspace name"
    )
    assert '"LOG_ANALYTICS_WORKSPACE_NAME"' in text, (
        "Azure permission validation must resolve the workspace name from env config"
    )
    assert "az monitor log-analytics workspace show" in text, (
        "Azure permission validation must resolve the workspace resource id"
    )
    assert 'Add-Result -Name "API runtime identity has Log Analytics Reader"' in text, (
        "Azure permission validation must fail when API runtime log-read access is missing"
    )
    assert '"Log Analytics Reader", "Log Analytics Contributor", "Contributor", "Owner"' in text, (
        "Azure permission validation must recognize built-in log-reader access and stronger explicit roles"
    )


def test_shared_provisioner_uses_workspace_safe_log_analytics_retention() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure.ps1"
    text = script.read_text(encoding="utf-8")

    assert "[int]$LogAnalyticsRetentionInDays = 30" in text, (
        "Shared Azure provisioning must default Log Analytics retention to a valid value for the workspace SKU"
    )
    assert "Resolve-LogAnalyticsRetentionTarget" in text, (
        "Shared Azure provisioning must compute an effective Log Analytics retention target"
    )
    assert "Configuring Log Analytics retention: requested=" in text, (
        "Shared Azure provisioning must log the requested and effective Log Analytics retention"
    )


def test_shared_provisioner_supports_parallel_private_runtime_and_network_smoke() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure.ps1"
    text = script.read_text(encoding="utf-8")

    assert '[string]$VnetContainerAppsEnvironmentName = "asset-allocation-env-vnet"' in text, (
        "Shared Azure provisioning must expose the parallel VNet Container Apps environment name"
    )
    assert '[string]$VnetName = "asset-allocation-vnet-prod"' in text, (
        "Shared Azure provisioning must expose the standalone production VNet name"
    )
    assert '[switch]$DisablePublicDataPlaneAccess' in text, (
        "Shared Azure provisioning must allow the private data plane cutover to disable public access explicitly"
    )
    assert "function Ensure-ParallelPrivateRuntime" in text, (
        "Shared Azure provisioning must centralize the workload-profiles VNet runtime creation flow"
    )
    assert "az network nat gateway create" in text, (
        "Shared Azure provisioning must provision a NAT Gateway for explicit ACA egress ownership"
    )
    assert "--infrastructure-subnet-resource-id $acaSubnetId" in text, (
        "Shared Azure provisioning must create the VNet-backed Container Apps environment on the delegated subnet"
    )
    assert "privatelink.blob.core.windows.net" in text, (
        "Shared Azure provisioning must create the blob private DNS zone"
    )
    assert "privatelink.dfs.core.windows.net" in text, (
        "Shared Azure provisioning must create the dfs private DNS zone"
    )
    assert "privatelink.postgres.database.azure.com" in text, (
        "Shared Azure provisioning must create the Postgres private DNS zone"
    )
    assert "privatelink.azurecr.io" in text, (
        "Shared Azure provisioning must create the ACR private DNS zone"
    )
    assert "-GroupId \"registry\"" in text, (
        "Shared Azure provisioning must create the ACR private endpoint with the registry group"
    )
    assert "--sku Premium" in text, (
        "Shared Azure provisioning must support ACR Premium for private endpoint support"
    )
    assert "az network private-endpoint create" in text, (
        "Shared Azure provisioning must create private endpoints for the private data plane"
    )
    assert '"network", "private-link-resource", "list"' in text, (
        "Shared Azure provisioning must fail fast when the current Postgres server cannot expose the required private link group"
    )
    assert "Replace or reconfigure the server in this phase instead of weakening the target topology." in text, (
        "Shared Azure provisioning must explain the fallback when Postgres private link is unavailable"
    )
    assert '"containerapp", "job", "create"' in text, (
        "Shared Azure provisioning must create the in-environment smoke job"
    )
    assert "busybox:1.36" in text, (
        "Shared Azure provisioning must use a lightweight diagnostic image for the network smoke job"
    )
    assert "Ensure parallel VNet Container Apps environment exists: ${VnetContainerAppsEnvironmentName}?" in text, (
        "Shared Azure provisioning must prompt for the parallel VNet runtime separately from the legacy public environment"
    )
    assert "[switch]$SkipParallelPrivateRuntime" in text, (
        "Shared Azure provisioning must allow recovery workflows to defer private-runtime provisioning"
    )
    assert "Skipping parallel VNet Container Apps environment because -SkipParallelPrivateRuntime was supplied." in text, (
        "Shared Azure provisioning must make private-runtime skips explicit in logs"
    )
    assert "containerAppsEnvironmentVnetName" in text, (
        "Shared Azure provisioning outputs must expose the parallel VNet environment name"
    )
    assert "networkSmokeJobName" in text, (
        "Shared Azure provisioning outputs must expose the smoke job resource name"
    )
    assert "apiRuntimeIdentityName" in text, (
        "Shared Azure provisioning outputs must expose the API runtime identity"
    )
    assert "Asset Allocation ACA Operator" in text, (
        "Shared Azure provisioning must replace RG Contributor job-start behavior with a narrow custom role"
    )
    assert '"Microsoft.OperationalInsights/workspaces/query/read"' not in text, (
        "ACA operator role must not rely on misleading generic Log Analytics query permissions"
    )
    assert '--query "{customerId:customerId,id:id}"' in text, (
        "Shared Azure provisioning must resolve the Log Analytics workspace resource id"
    )
    assert '-RoleName "Log Analytics Reader"' in text, (
        "API runtime identity must receive built-in Log Analytics Reader for console log queries"
    )
    assert "-Scope $lawWorkspaceResourceId" in text, (
        "Log Analytics Reader must be scoped to the workspace, not only the resource group"
    )
    assert "Storage Blob Data Contributor granted to $AcrPullIdentityName" not in text, (
        "ACR pull identity must not receive storage data-plane permissions"
    )


def test_infra_shared_workflow_passes_parallel_private_runtime_inputs() -> None:
    repo_root = _repo_root()
    workflow = repo_root / ".github" / "workflows" / "infra-shared-prod.yml"
    text = workflow.read_text(encoding="utf-8")

    assert "disable_public_data_plane_access:" in text, (
        "Shared infra workflow must expose the public data-plane cutover as an explicit operator input"
    )
    assert "provision_private_runtime:" in text, (
        "Shared infra workflow must keep private-runtime provisioning as an explicit operator input"
    )
    assert "CONTAINER_APPS_ENVIRONMENT_VNET_NAME=" in text, (
        "Shared infra workflow must write the VNet-backed Container Apps environment name into the env file"
    )
    assert "ACA_VNET_NAME=" in text, (
        "Shared infra workflow must write the standalone VNet name into the env file"
    )
    assert "NAT_GATEWAY_NAME=" in text, (
        "Shared infra workflow must write the NAT Gateway resource name into the env file"
    )
    assert "ACA_NETWORK_SMOKE_JOB_NAME=" in text, (
        "Shared infra workflow must write the smoke job resource name into the env file"
    )
    assert "UI_PUBLIC_HOSTNAME=" in text, (
        "Shared infra workflow must surface the stable UI hostname for custom-domain cutover"
    )
    assert "$provisionArgs = @{" in text, (
        "Shared infra workflow must use hash-table splatting so PowerShell binds provisioner arguments by name"
    )
    assert "$args = @(" not in text, (
        "Shared infra workflow must not use array splatting for named provisioner parameters"
    )
    assert "& .\\scripts\\ops\\provision\\provision_azure.ps1 @provisionArgs" in text, (
        "Shared infra workflow must splat the named provisioner argument hash"
    )
    assert "NonInteractive = $true" in text, (
        "Shared infra workflow must keep provisioning non-interactive"
    )
    assert "PromptForResources = $false" in text, (
        "Shared infra workflow must disable resource prompts through named binding"
    )
    assert "$provisionArgs.SkipParallelPrivateRuntime = $true" in text, (
        "Shared infra workflow must skip private-runtime provisioning unless explicitly requested"
    )
    assert "VnetContainerAppsEnvironmentName =" in text, (
        "Shared infra workflow must pass the VNet-backed Container Apps environment name to the provisioner"
    )
    assert "NatGatewayName =" in text, (
        "Shared infra workflow must pass the NAT Gateway name to the provisioner"
    )
    assert "NetworkSmokeJobName =" in text, (
        "Shared infra workflow must pass the network smoke job name to the provisioner"
    )
    assert "UiPublicHostname =" in text, (
        "Shared infra workflow must pass the stable UI hostname to the provisioner"
    )
    assert "$provisionArgs.DisablePublicDataPlaneAccess = $true" in text, (
        "Shared infra workflow must only disable the public data plane when the explicit cutover input is enabled"
    )
    assert "private_runtime_verified=true is required before disabling public Storage/Postgres data-plane access." in text, (
        "Shared infra workflow must require a private-runtime smoke confirmation before data-plane cutover"
    )
    assert "ApiRuntimeIdentityName =" in text, (
        "Shared infra workflow must pass the API runtime identity name to the provisioner"
    )
    assert "Reapply Container Apps/job operator RBAC and API Log Analytics read access" in text, (
        "Shared infra workflow must describe the RBAC reconcile input as covering log-read access"
    )
    assert "Job operation and log-read RBAC reconciled" in text, (
        "Shared infra workflow summary must report the combined job/log RBAC reconcile state"
    )
    assert "$provisionArgs.EnableAcrPrivateLink = $true" in text, (
        "Shared infra workflow must pass the ACR private link input to the provisioner"
    )


def test_postgres_provisioner_persists_env_outputs() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure_postgres.ps1"
    text = script.read_text(encoding="utf-8")

    assert "function Set-EnvValues" in text, (
        "Postgres provisioner must persist resolved database settings back into the active env file"
    )
    assert 'POSTGRES_SERVER_NAME   = $ServerName' in text, (
        "Postgres provisioner must write the resolved server name back into the env file"
    )
    assert 'POSTGRES_DATABASE_NAME = $DatabaseName' in text, (
        "Postgres provisioner must write the resolved database name back into the env file"
    )
    assert 'POSTGRES_ADMIN_USER    = $AdminUser' in text, (
        "Postgres provisioner must write the admin username back into the env file"
    )
    assert 'Write-Host "POSTGRES_DSN source: $persistedDsnSource"' in text, (
        "Postgres provisioner should report which credential was written into POSTGRES_DSN"
    )


def test_shared_provisioner_passes_env_file_to_postgres_and_syncs_github() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "provision" / "provision_azure.ps1"
    text = script.read_text(encoding="utf-8")

    assert "function Sync-EnvWebToGitHub" in text, (
        "Shared Azure provisioner must centralize .env.web GitHub synchronization"
    )
    assert "EnvFile              = $envPath" in text, (
        "Shared Azure provisioner must pass the active env file through to the Postgres provisioner"
    )
    assert 'Join-Path $repoRoot "scripts\\sync-all-to-github.ps1"' in text, (
        "Shared Azure provisioner must sync GitHub vars/secrets through the repo-local helper"
    )
    assert "Sync-EnvWebToGitHub -EnvPath $envPath" in text, (
        "Shared Azure provisioner must sync GitHub after the Postgres step updates .env.web"
    )
    assert "Configure-AiRelayBootstrap" in text, (
        "Shared Azure provisioner must expose the AI relay bootstrap helper"
    )
    assert "ai-relay-api-key" in text, (
        "Shared Azure provisioner must set the AI relay secret on the API container app"
    )
