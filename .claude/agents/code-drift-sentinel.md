---
name: code-drift-sentinel
description: "Detect, score, attribute, and remediate code drift caused by multiple AI agents and humans across style, architecture, APIs, dependencies, behavior, performance, security, tests, docs, and CI/config. Use when running drift audits locally/CI/scheduled jobs, enforcing drift score quality gates, generating drift_report.md/json artifacts, or preparing safe auto-remediation patches. Treat speculative safeguards and placeholder fallbacks such as precautionary null branches or UI copy like 'N/A'/'unknown' as undesired drift unless the requirement explicitly calls for them."
---

# Code Drift Sentinel

## Overview

Run a deterministic drift audit against a baseline, produce human and machine-readable reports, and optionally apply safe remediations with rollback on failed verification.

Behavioral drift includes speculative safeguards and placeholder fallbacks that change semantics without an explicit requirement, such as precautionary `null` branches or UI copy like `N/A` / `unknown`.

## Workflow

1. Load configuration from repo-root `.codedrift.yml`.
2. Resolve baseline using priority: configured commit/tag/branch, then `main`/`master`, then latest tag.
3. Gather drift signals (`git diff`, changed files, recent log, attribution).
4. Run configured gates (formatter/lint/typecheck/tests/security/benchmark) unless skipped.
5. Classify and score findings by category and severity.
6. Emit `drift_report.md` and `drift_report.json`.
7. In `recommend` mode, include patch preview hunks.
8. In `auto-remediate` mode, apply deterministic fix commands, validate, and emit patch or rollback.

## Commands

Run from repository root.

```bash
python3 .codex/skills/code-drift-sentinel/scripts/codedrift_sentinel.py --mode audit
python3 .codex/skills/code-drift-sentinel/scripts/codedrift_sentinel.py --mode recommend
python3 .codex/skills/code-drift-sentinel/scripts/codedrift_sentinel.py --mode auto-remediate
```

Useful flags: `--repo .`, `--config .codedrift.yml`, `--baseline-ref <sha|tag|branch>`, `--ci`, `--pr-head <sha>`, `--skip-quality-gates`, `--include-full-tests`.

## Mode Rules

- `audit`: detect drift and write reports only.
- `recommend`: detect drift and include patch preview hunks in report/json.
- `auto-remediate`:
  - require `auto_remediate.enabled: true`
  - require clean working tree
  - allow deterministic fix commands only
  - enforce `max_files_changed`, `safe_directories`, and `protected_globs`
  - run post-fix checks/tests
  - revert touched files when verification fails

## Required Artifacts

Use reporting paths from `.codedrift.yml` (defaults shown):
- `artifacts/drift_report.md`
- `artifacts/drift_report.json`
- `artifacts/drift_remediation.patch` (auto-remediate success)

## CI Gate

Fail the job when `drift_score >= thresholds.drift_score_fail`. The script exits non-zero and prints a top-5 issue summary for CI logs.

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

## Safe Auto-Remediation Constraints

- Allowed: format/lint deterministic fixes, import ordering, mechanical consistency fixes, docs sync.
- Disallowed without explicit opt-in: risky business logic rewrites, auth/IaC/migration rewrites, broad uncertain refactors.
- Require clean working tree before auto-remediate.
- If post-fix checks fail, revert touched files and report failure.

## Evidence Standards

Include concrete evidence for every finding: file paths, diff snippets, command output excerpts, attribution from `git log` against affected files. Mark uncertain heuristics with lower confidence and recommend human review.

## Resources

- `scripts/codedrift_sentinel.py`: core engine (audit/recommend/auto-remediate).
- `scripts/run_scenarios.py`: scenario listing and structural validation helper.
- `references/agent.md`: report contract and scoring behavior.
- `references/codedrift-yml-schema.md`: `.codedrift.yml` schema/contract.
- `references/codedrift.schema.json`: machine-readable schema for config validation tooling.
- `codedrift_scenarios/*.yml`: six required acceptance scenarios.
