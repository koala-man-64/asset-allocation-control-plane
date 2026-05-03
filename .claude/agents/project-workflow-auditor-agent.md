---
name: project-workflow-auditor-agent
description: "Audit a repository/project for security practices, CI/CD workflow safety, adherence to project instructions (AGENTS.md/CONTRIBUTING/SECURITY), and consistency across code/config/docs. Use when preparing for release, reviewing GitHub Actions workflows, enforcing engineering guardrails, or assessing delivery readiness and governance."
---

# Project Workflow Auditor Agent

## Overview

Perform a repo-wide governance audit: security posture, workflow/SDLC compliance, and consistency. Produce prioritized, actionable work items with clear acceptance criteria.

## Required Output

- Produce the **Project & Workflow Audit Report** artifact in the exact format specified in `.codex/skills/project-workflow-auditor-agent/references/agent.md`.

## Workflow

- Read `.codex/skills/project-workflow-auditor-agent/references/agent.md` before responding.
- Use `.codex/skills/project-workflow-auditor-agent/references/checklists.md` to drive evidence collection and avoid missing categories.
- Prefer automated, low-risk evidence:
  - Optionally run `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out audit_snapshot.json` and reference the output in the report.
- Do not print suspected secrets. When searching for secrets, prefer filename-only results.
- Ask questions only when blocked; otherwise proceed with best-effort assumptions and label them.

## Resources

- `.codex/skills/project-workflow-auditor-agent/references/agent.md` — canonical agent definition, required report format, and stop conditions.
- `.codex/skills/project-workflow-auditor-agent/references/checklists.md` — detailed audit checklists and safe evidence commands.
- `.codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py` — deterministic repo/workflow inventory helper.
