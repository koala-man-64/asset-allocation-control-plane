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
- Secrets are never fetched from Azure. Existing `.env.web` secrets are reused; otherwise the script prompts securely.
- `DEPLOY_SMOKE_BEARER_TOKEN` is a required manual secret for protected deploy smoke checks and should carry the minimum `AssetAllocation.Access` role only.
- `API_DEPLOY_MANIFEST` controls whether `deploy-prod.yml` rolls out the transitional public API edge (`deploy/app_api_public.yaml`) or the internal-only VNet app (`deploy/app_api.yaml`). Internal-only deploys switch verification to the in-environment smoke job instead of public HTTP probes.
- `RUN_LIVE_MASSIVE_TESTS` is an optional local opt-in flag in the managed env surface. Leave it `false` unless you intentionally want live Massive integration tests to run.
- `AI_RELAY_API_KEY` is always manual input. The setup script never discovers or backfills it automatically.
- The parallel private runtime is controlled through the `ACA_*`, `PRIVATE_ENDPOINT_*`, `NAT_*`, `*_VNET_NAME`, and `UI_PUBLIC_HOSTNAME` vars in the env contract so the VNet-backed substrate and stable UI hostname stay explicit in source control.
- `scripts/sync-all-to-github.ps1` reads only `.env.web` and this repo-local env contract, and fails fast if AI relay is enabled without the required AI key or role surface.

Operational ownership:

- This repo owns shared Azure provisioning scripts.
- This repo owns `infra-shared-prod.yml`.
- The jobs, UI, and contracts repos must not carry shared Azure bootstrap scripts.
