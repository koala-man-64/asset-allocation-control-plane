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
- `scripts/sync-all-to-github.ps1` reads only `.env.web` and this repo-local env contract.

Operational ownership:

- This repo owns shared Azure provisioning scripts.
- This repo owns `infra-shared-prod.yml`.
- The jobs, UI, and contracts repos must not carry shared Azure bootstrap scripts.
