# Gateway Ledger (Local)

This ledger tracks tool usage and delivery orchestration for work items executed by Codex.

## Policy
- MCP-first: attempt to discover MCP tools/resources; if unavailable, fallback to direct local tools with justification.
- Log: intent → tool → outcome → next decision.

## Session Log

### 2026-02-03
- **MCP discovery:** `functions.list_mcp_resources` / `functions.list_mcp_resource_templates` returned empty; no MCP tools available → fallback to local tools permitted.
- **Fallback tooling:** Using `functions.shell_command` and `functions.apply_patch` with explicit intent logging in Orchestrator Updates.
- **Work Item:** `WI-CONFIGJS-001` standardize `/config.js` at domain root (docs + tests + dev proxy toggle).
  - **Code changes:** added `VITE_PROXY_CONFIG_JS` toggle in `ui/vite.config.ts`; documented contract in `docs/config_js_contract.md`; added backend contract tests in `tests/api/test_config_js_contract.py`; updated `.env.template`.
  - **Verification:** `python3 -m pytest -q tests/api/test_config_js_contract.py tests/monitoring/test_system_health.py` → `13 passed`.
