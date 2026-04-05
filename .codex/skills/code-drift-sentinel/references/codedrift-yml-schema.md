# `.codedrift.yml` Contract

Use `.codedrift.yml` at repo root as the source of truth.

## Required Sections

```yaml
baseline:
  branch: string            # optional
  tag: string               # optional
  commit: string            # optional

standards:
  format: [string]
  lint: [string]
  typecheck: [string]
  test_fast: [string]
  test_full: [string]
  security: [string]        # optional
  benchmark: [string]       # optional
  format_fix: [string]      # optional (auto-remediate)
  lint_fix: [string]        # optional (auto-remediate)

architecture:
  layers:
    - name: string
      path_glob: string
      forbid_imports_from: [string]  # optional
      allow_imports_from: [string]   # optional
  module_boundaries: []      # optional
  blessed_patterns: [string] # optional

dependencies:
  allowlist: [string]
  denylist: [string]
  lockfile_required: boolean
  license_policy: {}         # optional

api:
  public_globs: [string]
  breaking_change_policy: "warn" | "strict"

thresholds:
  drift_score_fail: number
  category_weights: {}       # optional category overrides
  category_severity_fail: {} # optional severity gates

auto_remediate:
  enabled: boolean
  max_files_changed: number
  safe_directories: [string]
  commands: [string]

risk_controls:
  protected_globs: [string]
  require_tests_passing_before_automerge: boolean

reporting:
  markdown_path: string
  json_path: string
  patch_path: string

detection:
  lookback_days: number
  speculative_safeguards:   # optional
    enabled: boolean
    patterns: [string]      # regexes matched against added code lines
```

## Baseline Priority

1. `baseline.commit` / `baseline.tag` / `baseline.branch`
2. `main` then `master`
3. latest tag

## Notes

- Configure `standards.*` commands as deterministic checks.
- Use `auto_remediate.commands` (or `format_fix`/`lint_fix`) for mutating commands.
- Keep `protected_globs` strict for auth/migrations/infra paths.
- Use `detection.speculative_safeguards` to flag precautionary guard branches and placeholder fallbacks such as `: null`, `|| null`, `?? null`, or `: "N/A"` when those behaviors are not explicitly desired.
