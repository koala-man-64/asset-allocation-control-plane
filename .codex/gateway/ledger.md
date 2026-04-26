# Gateway Ledger (Local)

This ledger tracks tool usage and delivery orchestration for work items executed by Codex.

## Policy
- MCP-first: attempt to discover MCP tools/resources; if unavailable, fallback to direct local tools with justification.
- Log: intent → tool → outcome → next decision.

## Session Log

### 2026-04-26
- **Work Item:** `bronze-layer-remediation-20260426` control-plane remediation.
  - **Branch:** `agent/codex/bronze-layer-remediation-20260426/asset-allocation-control-plane` from `origin/main` at `779a3b8ed2f09f38e2fb30782f72964370110d2b`.
  - **Scope:** provider endpoint classification, shallow health/readiness preservation, provider telemetry, and log/query redaction.
  - **Contract routing:** local-only; preserve public response shapes and avoid provider-readiness schema changes.
  - **Coordination:** depends on runtime-common package hardening where shared gateway clients are consumed.
  - **Progress:** implemented shallow health/readiness regression tests, provider status classification updates, middleware query metadata logging, per-provider telemetry fields, Quiver disabled logging, and recursive log redaction.
  - **Validation:** `python -m ruff check .` passed; `python -m pytest tests/api/test_health_readiness.py tests/core/test_log_redaction.py tests/api/test_alpha_vantage_endpoints.py tests/api/test_massive_endpoints.py tests/api/test_quiver_endpoints.py` passed (`64 passed`).
- **Coordinator follow-up:** full-suite validation exposed a log-formatting interaction after redaction installed globally; patched `core/log_redaction.py` to format log messages before redacting and clear stale args.
- **Coordinator validation:** added regression coverage in `tests/core/test_log_redaction.py` for sensitive placeholder names with `%s` args.
- **Coordinator validation:** reran focused redaction/auth tests (`9 passed`), full `python -m pytest -q --tb=short` (passed), full `python -m ruff check .` (passed), and `git diff --check` (passed with line-ending warnings only).

### 2026-02-03
- **MCP discovery:** `functions.list_mcp_resources` / `functions.list_mcp_resource_templates` returned empty; no MCP tools available → fallback to local tools permitted.
- **Fallback tooling:** Using `functions.shell_command` and `functions.apply_patch` with explicit intent logging in Orchestrator Updates.
- **Work Item:** `WI-CONFIGJS-001` standardize `/config.js` at domain root (docs + tests + dev proxy toggle).
  - **Code changes:** added `VITE_PROXY_CONFIG_JS` toggle in `ui/vite.config.ts`; documented contract in `docs/config_js_contract.md`; added backend contract tests in `tests/api/test_config_js_contract.py`; updated `.env.template`.
  - **Verification:** `python3 -m pytest -q tests/api/test_config_js_contract.py tests/monitoring/test_system_health.py` → `13 passed`.
