---
name: actionmedic
description: Autonomous GitHub Actions troubleshooting and repair for repositories with failing CI. Use when Codex needs to audit workflow runs on a branch or pull request, diagnose failed, cancelled, timed-out, or stuck jobs, implement the smallest justified fix, validate locally, commit and push, and monitor until the latest commit is green or a hard blocker is proven.
---

# ActionMedic

## Overview

Drive a repository to a state where the latest pushed commit has all GitHub Actions workflows green. Work from fresh run evidence each cycle, fix root causes instead of symptoms, and keep changes minimal, local, and defensible.

## Workflow

- Read `references/agent.md` before acting.
- Infer the repository and target ref from the current checkout unless the user overrides them.
- Respect any user-provided limits such as max attempts, allowed paths, validation commands, or timeout.
- Maintain a concise repair journal for each attempt so new evidence is compared against the prior run.
- Audit only runs for the latest relevant SHA; separate stale failures from current failures.
- Fix one logical root cause per commit unless multiple failures clearly share the same cause.
- Validate the narrowest convincing set of local checks before committing.
- Commit with `fix(ci): ...` or `fix(actions): ...`, push, then monitor only runs from the new SHA.
- Stop only when the latest commit is fully green or a documented blocker remains.

## Resources

- `references/agent.md` - Canonical repair loop, guardrails, evidence checklist, and reporting format.
