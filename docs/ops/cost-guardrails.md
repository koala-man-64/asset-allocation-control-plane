# Cost Guardrails

This repo now includes Azure-side guardrail automation for the cost controls chosen during the audit:

- monthly subscription-scope budgets filtered to `AssetAllocationRG` and split by meter category
- a daily subscription-scoped cost anomaly alert
- default notification target `rdprokes@gmail.com`
- lightweight tagging defaults for Container Apps Jobs via the deploy manifests

## What gets deployed

[`deploy/cost_guardrails.bicep`](../../deploy/cost_guardrails.bicep) creates:

- one `Microsoft.Consumption/budgets` resource per configured meter-category group
- one `Microsoft.CostManagement/scheduledActions` resource of kind `InsightAlert`

[`scripts/ops/provision/configure_cost_guardrails.ps1`](../../scripts/ops/provision/configure_cost_guardrails.ps1) wraps the subscription deployment and supplies default values for this project.

## Defaults

The wrapper script assumes these starting monthly budgets:

- `Container Apps`: `150`
- `Azure Monitor`: `30`
- `Storage`: `40`
- `Container Registry`: `20`
- `Azure Database for PostgreSQL`: `50`

These are starting guardrails, not usage-derived targets. Tune them after confirming the exact meter-category names and the first month of actual spend.

## Run

Preview the deployment first:

```powershell
pwsh ./scripts/ops/provision/configure_cost_guardrails.ps1 -WhatIf
```

Apply it:

```powershell
pwsh ./scripts/ops/provision/configure_cost_guardrails.ps1
```

## Important limits

- Azure Cost Management budgets can be filtered by meter category and resource group, so those alerts are split the way requested.
- Azure cost anomaly alerts are subscription-scoped. The repo deploys a single anomaly alert for the subscription because Azure does not expose meter-category-scoped anomaly alerts.
- Native Cost Management budgets and anomaly alerts do not add Azure monitoring charges by themselves. Separate Azure Monitor action groups or downstream automation can add cost.

## Verification

After deployment, verify:

```powershell
az consumption budget list --query "[].name"
az resource list --namespace Microsoft.CostManagement --resource-type scheduledActions --query "[].name"
```

In the Azure portal, confirm:

- budgets appear under Cost Management for the subscription
- each budget filters to `AssetAllocationRG` plus the intended meter category
- the anomaly alert points to `rdprokes@gmail.com`
