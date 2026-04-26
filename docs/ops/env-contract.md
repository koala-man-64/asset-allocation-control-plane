# Control-Plane Env Contract

This repo treats `.env.web` as the sync surface for GitHub variables and secrets.

Flow:

1. Review `docs/ops/env-contract.csv`.
2. Run `powershell -ExecutionPolicy Bypass -File scripts/setup-env.ps1`.
3. Inspect the preview or generated `.env.web`.
4. Run `powershell -ExecutionPolicy Bypass -File scripts/sync-all-to-github.ps1`.

Rules:

- `scripts/setup-env.ps1` only walks keys documented in `env-contract.csv`.
- Azure-backed values are auto-discovered when `az` is installed and logged in.
- Git and GitHub metadata are used for repo slug defaults where possible.
- Secrets are never fetched from Azure. Existing `.env.web` secrets are reused, and matching GitHub secret names suppress prompts so already-populated repo secrets can be preserved without re-entry.
- Protected deploy smoke tokens are minted during `deploy-prod.yml` from the Azure login identity using `API_OIDC_AUDIENCE`; rerun `scripts/ops/provision/provision_entra_oidc.ps1` after setting `AZURE_CLIENT_ID` so the deploy principal receives `AssetAllocation.Access`.
- `API_AUTH_SESSION_MODE=cookie` enables browser session cookies. `API_AUTH_SESSION_SECRET_KEYS` must be populated with at least one high-entropy secret before rollout; comma-separated older keys may remain temporarily for rotation.
- `API_DEPLOY_MANIFEST` defaults to the internal-only VNet manifest (`deploy/app_api.yaml`). `deploy/app_api_public.yaml` is break-glass only and `deploy-prod.yml` refuses it unless `ALLOW_PUBLIC_API_INGRESS=true`.
- `API_RUNTIME_IDENTITY_NAME` is the managed identity rendered into runtime `AZURE_CLIENT_ID`; `ACR_PULL_IDENTITY_NAME` must remain limited to image pull, and infra reconcile grants the deploy principal Managed Identity Operator on both identities.
- `ENABLE_ACR_PRIVATE_LINK=true` reconciles ACR Premium private endpoint support through `privatelink.azurecr.io`. Keep ACR public access enabled until the in-environment image-pull smoke has passed.
- `SYSTEM_*_REQUIRED_ROLES`, `RUNTIME_CONFIG_WRITE_REQUIRED_ROLES`, `JOB_OPERATE_REQUIRED_ROLES`, and `PURGE_WRITE_REQUIRED_ROLES` gate operator-only control-plane routes beyond baseline API auth.
- `RUN_LIVE_MASSIVE_TESTS` is an optional local opt-in flag in the managed env surface. Leave it `false` unless you intentionally want live Massive integration tests to run.
- `AI_RELAY_API_KEY` is always manual input. The setup script never discovers or backfills it automatically.
- E*TRADE broker credentials and Schwab OAuth client credentials are operator-provided GitHub secrets. The setup script never discovers or backfills broker API keys or OAuth client secrets.
- Schwab access and refresh tokens are never setup or sync inputs. The control-plane obtains them through the Schwab OAuth connect flow, keeps them in process memory, and refreshes access tokens from the in-memory refresh token when available.
- Broker write routes are separately gated. Set `ETRADE_TRADING_ENABLED=true` or `SCHWAB_TRADING_ENABLED=true` only after the corresponding read integration, credentials, and broker trade roles are configured.
- `API_CORS_ALLOW_ORIGINS` should not stay blank in deployed environments. `scripts/setup-env.ps1` derives it from `UI_PUBLIC_HOSTNAME` when present, otherwise from the effective UI redirect origin and finally the live UI Container App ingress FQDN.
- The parallel private runtime is controlled through the `ACA_*`, `PRIVATE_ENDPOINT_*`, `NAT_*`, `*_VNET_NAME`, and `UI_PUBLIC_HOSTNAME` vars in the env contract so the VNet-backed substrate and stable UI hostname stay explicit in source control.
- `scripts/sync-all-to-github.ps1` reads only `.env.web` and this repo-local env contract, and fails fast if AI relay is enabled without the required AI key or role surface.

Operational ownership:

- This repo owns shared Azure provisioning scripts.
- This repo owns `infra-shared-prod.yml`.
- The jobs, UI, and contracts repos must not carry shared Azure bootstrap scripts.
