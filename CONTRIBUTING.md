# Contributing

## Development Setup

### Python

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e .
python3 -m pip install -r requirements-dev.txt
```

Use Python 3.14 for local backend work so local installs match CI and container images.

### UI

```bash
cd ui
pnpm install
```

## Day-to-Day Checks

### Backend

```bash
python3 -m ruff check .
python3 -m pytest -q
```

### UI

```bash
cd ui
pnpm lint
pnpm exec vitest run --coverage
pnpm build
```

GitHub Actions currently runs workflow lint, backend validation, and dependency governance on Python 3.14. Dependency audits and SARIF publication run separately in the scheduled or manual security workflow.

## Dependency Governance

- Runtime dependency source of truth is `pyproject.toml` under `[project].dependencies`.
- Regenerate `requirements.txt` and `requirements.lock.txt` from `pyproject.toml` with:

```bash
python3 scripts/repo/dependency_governance.py sync
```

- Validate runtime and dev dependency alignment with:

```bash
python3 scripts/repo/dependency_governance.py check --report artifacts/dependency_governance_report.json
```

- If you change `requirements-dev.txt`, keep `requirements-dev.lock.txt` aligned in the same change. CI installs the dev lockfile when it is present.

## Docs and Config Changes

- Update `.env.template` when you add, rename, or remove environment variables.
- Update the root docs and targeted runbooks when you change application behavior, deployment knobs, or operator workflows.
- Treat `/api/docs` and `/api/openapi.json` as the source of truth for live API routes.
- If you add, remove, or rename repo-local agents under `.codex/skills`, update `AGENTS.md` in the same change.

## Pull Requests

- Keep changes scoped.
- Add or update tests for behavior changes.
- Call out secret, auth, data migration, or deployment impacts when you touch `deploy/`, runtime config, auth, or provider integration code.

## Evidence

- `pyproject.toml`
- `requirements.txt`
- `requirements.lock.txt`
- `requirements-dev.txt`
- `requirements-dev.lock.txt`
- `scripts/repo/dependency_governance.py`
- `.github/workflows/ci.yml`
- `.github/workflows/security.yml`
- `pytest.ini`
- `.env.template`
- `api/service/app.py`
- `ui/package.json`
