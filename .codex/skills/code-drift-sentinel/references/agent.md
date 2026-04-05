# Code Drift Sentinel Reference

## Purpose

Detect, explain, score, and remediate code drift across style, architecture, API, dependencies, behavior, performance, security, testing, documentation, and CI/config changes.

Behavioral drift includes speculative safeguards and placeholder fallbacks that change semantics without an explicit requirement, such as precautionary `null` branches or UI copy like `N/A` / `unknown`.

## Required Outputs

- `drift_report.md` with:
  - summary (`drift_score`, threshold decision)
  - drift hotspots
  - per-category findings with expected-vs-observed and evidence
  - remediation plan (ordered)
  - merge risk assessment in CI context
  - appendix with tool status
- `drift_report.json` with:
  - baseline metadata
  - drift score + category scores
  - findings array
  - suggested actions array
  - tool run status

## Modes

- `audit`: detect and report only
- `recommend`: detect, report, include patch preview hunks
- `auto-remediate`: apply deterministic fixes, validate checks/tests, emit patch; revert if checks fail

## Severity and Confidence

- Severity: `low | medium | high | critical`
- Confidence: `0.0..1.0`
- Require explicit evidence lines (file paths, diff hunks, command output snippets).

## Drift Score Weights (default)

- `security`: 40
- `api`: 35
- `architecture`: 25
- `behavioral`: 25
- `test`: 25
- `dependency`: 20
- `performance`: 15
- `config_infra`: 15
- `style`: 5
- `docs`: 3

Score is the sum of `category_weight * severity_multiplier` for each finding.

## CI Gate

- Fail with non-zero exit when `drift_score >= thresholds.drift_score_fail`.
- Print a short top-5 issue summary for CI logs.

## Safe Auto-Remediation Constraints

- Allowed: format/lint deterministic fixes, import ordering, mechanical consistency fixes, docs sync.
- Disallowed without explicit opt-in: risky business logic rewrites, auth/IaC/migration rewrites, broad uncertain refactors.
- Require clean working tree before auto-remediate.
- If post-fix checks fail, revert touched files and report failure.
