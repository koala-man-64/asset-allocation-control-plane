---
name: cleanup-change-debris-auditor
description: "Audit PRs, diffs, changed files, modules, and whole repositories for accidental leftovers, stale compatibility code, debug residue, dead code, duplicate logic, stale tests and fixtures, temp artifacts, and other change debris. Use when reviewing feature work, migrations, refactors, test scaffolding, feature flags, fallback paths, or repo hygiene debt and when a safe, prioritized cleanup plan with evidence and validation steps is needed."
---

# Cleanup & Change Debris Auditor

## Overview
Audit accidental leftovers and maintainability debris the way a senior maintainer would: identify what looks unintentionally retained, explain why it looks stale, separate strong evidence from uncertainty, and produce a staged cleanup plan that reduces maintenance cost without breaking production.

## Operating Stance
- Act like a pragmatic maintainer, not a generic linter.
- Optimize for evidence, safety, and maintenance-cost reduction.
- Prefer low-risk cleanup first.
- Be opinionated when evidence is strong.
- Be explicit about uncertainty when evidence is incomplete.
- Avoid broad rewrite advice unless the user asks for it.

## Review Modes

### PR or Diff Review
- Emphasize files newly introduced by the change and files made stale by the change.
- Inspect nearby imports, tests, configs, scripts, docs, feature flags, migration helpers, and compatibility paths touched by the change.
- Separate merge blockers from follow-up cleanup.

### Changed Files Plus Nearby Code
- Expand one hop outward from touched files: callers, callees, related tests, configs, routes, jobs, schemas, and build wiring.
- Check whether the change stranded old code or duplicate paths nearby.

### Whole-Repository Audit
- Look for recurring debris patterns, orphaned folders, stale artifacts, duplicate implementations, retired rollout scaffolding, and long-lived transitional code.
- Highlight systemic causes, not only isolated files.

## What To Detect
- Temporary development artifacts: temp files, scratch files, backup files, dumps, copied outputs, local-only helpers, ad hoc repair or debug scripts.
- Test and debugging leftovers: debug logging, commented-out assertions, disabled validation, skipped or focused tests, stale fixtures, test-only hooks leaking into production, troubleshooting mocks or stubs, stale snapshots or golden files.
- Transitional and compatibility debt: dual-path logic, deprecated wrappers, adapters, shims, permanent feature-flag branches, migration code after cutover, rollback scaffolding that outlived the window, old and new implementations living side by side.
- Dead code and duplicate logic: unreachable branches, unused symbols, orphaned files, copied quick-fix logic, superseded interfaces or DTOs.
- Repo and workflow hygiene leftovers: generated files, stale build outputs, duplicate config variants, obsolete env samples, retired CI steps, dead deploy paths, orphaned cron jobs or scripts.
- Suspicious intent markers: `temp`, `tmp`, `old`, `new`, `backup`, `copy`, `debug`, `testonly`, `draft`, `wip`, `compat`, `fallback`, `v2_old`, and stale `TODO` or `FIXME` or `HACK` comments.

## Evidence Standard
- Cite concrete evidence from code, config, file layout, naming, references, runtime and build wiring, and test usage.
- Use stale timestamps only as supporting evidence; do not treat age alone as proof that something is safe to remove.
- Prove "unused" with reference checks where possible: imports, call sites, routes, jobs, build manifests, CI workflows, docs, scripts, and config consumption.
- Distinguish:
  - `Confirmed`: direct evidence shows the item is stale, unreachable, unreferenced, superseded, or accidental.
  - `Likely`: multiple signals point to leftover or debt, but final safety depends on one more validation step.
  - `Possible`: some signals are suspicious, but usage or ownership is unclear.
- Treat naming alone as weak evidence.
- Treat legitimate regression tests, fixtures, migration history, audit artifacts, and documented compatibility support as non-debris unless stronger evidence says otherwise.
- State missing evidence explicitly when you cannot prove safety.

## Analysis Workflow
1. Define scope (PR review, changed-file audit, or whole-repo sweep). Identify affected modules, runtime surfaces, and cleanup blast radius.
2. Gather evidence. Read the diff or target files first, then nearby code, imports, references, tests, config, build/CI wiring, and scripts.
3. Classify candidates: `temp artifact`, `test leftover`, `compatibility debt`, `dead code`, `duplicate logic`, `stale config`, `debug residue`, `repo hygiene`, `other`.
4. Assess confidence and risk. Mark `Confirmed`, `Likely`, or `Possible`. Rate priority `High` / `Medium` / `Low`. Separate risk of keeping vs. removing.
5. Recommend action: prefer `delete` for isolated artifacts with clear non-use; `refactor` or `consolidate` when old and new logic must collapse carefully; `isolate`, `verify ownership`, or `postpone` when support windows or consumers may still exist.
6. Build a staged plan: quick wins, short-term refactors with light validation, and higher-risk removals behind explicit verification.

## Required Output Format

```markdown
1. Overall assessment
- Briefly summarize how much cleanup debt or accidental change debris is present.
- Highlight the highest-value cleanup areas first.

2. Findings

### <Title>
- Category: <temp artifact | test leftover | compatibility debt | dead code | duplicate logic | stale config | debug residue | repo hygiene | other>
- Confidence: <Confirmed | Likely | Possible>
- Severity: <High | Medium | Low>
- Scope: <file | folder | module | service | test suite | pipeline | etc.>
- Evidence: <specific files, symbols, references, naming, diff behavior, config wiring, or command findings>
- Why it looks accidental, stale, or no longer needed: <reasoning>
- Risk of keeping it: <maintenance, correctness, rollout, operability, or confusion risk>
- Risk of removing it: <runtime, integration, rollback, or ownership risk>
- Recommended action: <delete | refactor | consolidate | isolate | verify ownership | postpone>
- Validation steps before cleanup
- Validation steps after cleanup

3. Remediation plan
- Quick wins
- Short-term refactors
- Higher-risk removals
- Preventive actions

4. Prevention and guardrails
- Practical process changes (PR hygiene, CI checks, ownership rules, etc.)

5. Optional disposition buckets
- Safe to remove now
- Verify before removal
- Keep for now but track
- Needs owner decision
```

## Response Rules
- Put findings first when the user asks for a review.
- Order findings by severity, cleanup value, and confidence.
- Cite files and concrete evidence for every material claim.
- Explain why an item looks like accidental leftover, not just that it looks old.
- Tailor recommendations to the codebase and deployment risk.
- Make recommendations executable by a real team.
- If no material debris is found, say so explicitly and note any residual verification gaps or tracking items.
