# Deployment Setup

## Recommendation

Yes. This repo should have its own deployment to the shared Azure resource group.

Target shape:

- one Azure Container App for the control-plane
- same resource group: `AssetAllocationRG`
- same Container Apps environment: `asset-allocation-env`
- same ACR: `assetallocationacr`

Do not let this repo deploy the jobs or the standalone UI once the split is complete.

## Current State

The workflow split is already in place.

- `.github/workflows/deploy-prod.yml` deploys only `asset-allocation-api`.
- `.github/workflows/infra-shared-prod.yml` is the only workflow that mutates shared Azure substrate.
- `deploy/app_api.yaml` and `deploy/app_api_public.yaml` render API-only Container App manifests.

## Deploy

Use only these workflow entry points:

1. `.github/workflows/ci.yml`
2. `.github/workflows/security.yml`
3. `.github/workflows/release.yml`
4. `.github/workflows/deploy-prod.yml`
5. `.github/workflows/infra-shared-prod.yml`

`deploy-prod.yml` deploys only `asset-allocation-api`.

`infra-shared-prod.yml` is the only workflow that may mutate shared Azure substrate in `AssetAllocationRG`.

## Operate

- Run `contracts-compat.yml` when `contracts_released` is dispatched or when validating a candidate contracts ref manually.
- Use `release.yml` to build one immutable API image digest and export `api/contracts/control-plane.openapi.json` plus `api/contracts/ui-runtime-config.schema.json`.
- Use `deploy-prod.yml` with a full image digest and verify `/healthz`, `/readyz`, `/config.js`, and `/api/v1/openapi.json`.

## Shared Azure Foundation To Provision Once

Until infrastructure is moved into its own repo, use the local bootstrap scripts in this repository:

1. `powershell -ExecutionPolicy Bypass -File .\\scripts\\ops\\provision\\provision_azure.ps1 -ProvisionPostgres`
2. `powershell -ExecutionPolicy Bypass -File .\\scripts\\ops\\provision\\provision_entra_oidc.ps1`
3. `powershell -ExecutionPolicy Bypass -File .\\scripts\\ops\\validate\\validate_azure_permissions.ps1`

Those scripts currently provision or expect:

- resource group `AssetAllocationRG`
- storage account `assetallocstorage001`
- ACR `assetallocationacr`
- ACR pull identity `asset-allocation-acr-pull-mi`
- Log Analytics workspace `asset-allocation-law`
- Container Apps environment `asset-allocation-env`
- service account `asset-allocation-sa`
- Postgres Flexible Server `pg-asset-allocation`
- database `asset_allocation`

## Repo-Specific Inputs

This repo needs the shared runtime infra plus the API and UI auth configuration.

GitHub secrets:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_STORAGE_CONNECTION_STRING`
- `ALPHA_VANTAGE_API_KEY`
- `NASDAQ_API_KEY`
- `POSTGRES_DSN`
- `API_OIDC_ISSUER`
- `API_OIDC_AUDIENCE`
- `UI_OIDC_CLIENT_ID`
- `UI_OIDC_AUTHORITY`
- `UI_OIDC_SCOPES`
- `UI_OIDC_REDIRECT_URI`
- `ASSET_ALLOCATION_API_SCOPE`

GitHub variables:

- `RESOURCE_GROUP=AssetAllocationRG`
- `ACR_NAME=assetallocationacr`
- `ACR_PULL_IDENTITY_NAME=asset-allocation-acr-pull-mi`
- `SERVICE_ACCOUNT_NAME=asset-allocation-sa`

## Deployment Steps

1. Publish the contracts repo first and pin the version consumed here.
2. Export contract artifacts:
   - `python scripts/automation/export_contract_artifacts.py`
3. Run the repo test gates:
   - `python -m pytest tests/api/test_config_js_contract.py tests/api/test_internal_endpoints.py -q`
4. Build the API image from `Dockerfile.asset_allocation_api`.
5. Deploy a single control-plane Container App:
   - use `deploy/app_api_public.yaml` for public ingress
   - use `deploy/app_api.yaml` for private ingress
6. Verify:
   - `/healthz`
   - `/readyz`
   - `/config.js`
   - `/api/v1/openapi.json`

## Rollback

- Capture the previous `asset-allocation-api` image digest before every deployment.
- Roll back by rerunning `.github/workflows/deploy-prod.yml` with that previous digest.
- If shared substrate changes caused the regression, rerun `.github/workflows/infra-shared-prod.yml` with the last known-good configuration inputs.

## Troubleshoot

- If `ci.yml` fails on artifact drift, regenerate `api/contracts/*` with `python scripts/automation/export_contract_artifacts.py` and commit the changes.
- If `release.yml` fails to build the image, verify the runner checked out the sibling contracts repo and that Docker is building from the shared workspace root.
- If `deploy-prod.yml` fails before apply, verify `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `RESOURCE_GROUP`, `ACR_NAME`, `CONTAINER_APPS_ENVIRONMENT_NAME`, and `ACR_PULL_IDENTITY_NAME`.
- If `deploy-prod.yml` fails verification, inspect the deployed FQDN, `/healthz`, `/readyz`, `/config.js`, and `/api/v1/openapi.json` before retrying.
- If `infra-shared-prod.yml` fails, verify the generated env file contains the expected shared resource names and that the `prod` environment has the required secrets and variables.

## Dependencies

- Sibling contracts repo for CI, compatibility checks, and release builds
- Azure OIDC credentials in GitHub variables
- `prod` GitHub environment for deploy and infra workflows
- Shared runtime resources in `AssetAllocationRG`

## Notes

- `ASSET_ALLOCATION_API_BASE_URL` is a downstream consumer concern. Keep it in the jobs and UI repos, not in this repo's deploy contract.
- This repo is the source of truth for `/config.js` and `/openapi.json`.

## Evidence

- `.github/workflows/deploy-prod.yml`
- `.github/workflows/infra-shared-prod.yml`
- `deploy/app_api.yaml`
- `deploy/app_api_public.yaml`
- `api/service/app.py`
- `scripts/automation/export_contract_artifacts.py`
- `tests/api/test_config_js_contract.py`
- `tests/api/test_internal_endpoints.py`
- `.\\scripts\\ops\\provision\\provision_azure.ps1`
- `.\\scripts\\ops\\provision\\provision_azure_postgres.ps1`
- `.\\scripts\\ops\\provision\\provision_entra_oidc.ps1`
- `.\\scripts\\automation\\validate_deploy_inputs.py`
