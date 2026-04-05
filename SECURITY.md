# Security Policy

## Reporting a Vulnerability

If this repository is hosted on GitHub, use the repository Security tab to report a vulnerability privately.

If GitHub Security Advisories are not available for this repo, report the issue to the repository owner or maintainer through your internal security process. Do not open a public issue with exploit details.

## Authentication and Authorization

- Production deploys must configure `API_OIDC_ISSUER`, `API_OIDC_AUDIENCE`, `UI_OIDC_CLIENT_ID`, `UI_OIDC_AUTHORITY`, `UI_OIDC_SCOPES`, `UI_OIDC_REDIRECT_URI`, and `ASSET_ALLOCATION_API_SCOPE`.
- OIDC auth validates issuer and audience and can require scopes and roles. The service discovers JWKS from the issuer unless `API_OIDC_JWKS_URL` is set explicitly.
- The UI receives its runtime auth and API base URL settings from `/config.js`.
- Browser OIDC requires an explicit absolute `UI_OIDC_REDIRECT_URI`; deployed environments should use `https://.../auth/callback`.
- Local development can fall back to anonymous access only when no auth providers are configured and the runtime is local. Deployed environments do not allow anonymous auth.

## Secrets and Identities

- Do not commit secrets. `.gitignore` excludes `.env` and `.env.*`, while `.env.template` is the checked-in contract.
- Public ACA deploy manifests use Entra OIDC for browser and bronze-job auth.
- Azure deployment uses a user-assigned managed identity for registry pulls and platform access.

## Response Hardening and Input Validation

- API middleware sets `X-Content-Type-Options: nosniff` and `X-Frame-Options: DENY`.
- `API_CSP` controls the Content Security Policy header when set.
- CORS origins are parsed from `API_CORS_ALLOW_ORIGINS`; wildcard `*` is removed when credentials are enabled.
- `api/service/security.py` validates run IDs, artifact names, local paths, and ADLS container/path inputs for filesystem- and artifact-related operations.

## Dependency Hygiene

- Runtime dependencies are pinned in `pyproject.toml`, `requirements.txt`, and `requirements.lock.txt`.
- CI and supply-chain workflows consume `requirements.lock.txt` and `requirements-dev.lock.txt`.
- Run `python3 scripts/repo/dependency_governance.py check --report artifacts/dependency_governance_report.json` before merging dependency changes.

## Evidence

- `.gitignore`
- `.env.template`
- `api/service/app.py`
- `api/service/settings.py`
- `api/service/auth.py`
- `api/service/security.py`
- `deploy/app_api.yaml`
- `.github/workflows/ci.yml`
- `.github/workflows/security.yml`
- `scripts/repo/dependency_governance.py`
