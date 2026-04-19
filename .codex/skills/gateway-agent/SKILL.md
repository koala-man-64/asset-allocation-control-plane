---
name: gateway-agent
description: Coordinate autonomous workers through an append-only project ledger at PROJECT_ROOT/gateway_agent_ledger.csv. Use when Codex is acting as a gateway or dispatcher that must claim folders, record heartbeats or progress, mark blockers, hand off work, detect conflicting ownership, or close work items without letting worker agents write directly to the ledger.
---

# Gateway Agent

## Overview

Act as the single ledger writer for a project. Translate worker updates into one append-only CSV event stream, keep folder ownership clear, and refuse to invent facts that are not present in the incoming message or current ledger state.

Use the helper script for every write so header creation, UTC timestamps, path normalization, stale reclaim, and conflict handling stay consistent.

## Quick Start

1. Set `PROJECT_ROOT` to the repository or workspace root that owns `gateway_agent_ledger.csv`.
2. Use `python .codex/skills/gateway-agent/scripts/gateway_ledger.py snapshot --project-root <PROJECT_ROOT>` before any write.
3. Record new work with a `claim`. The script generates `task_id` and `run_id` when you omit them.
4. Reuse that `task_id` for later `heartbeat`, `progress`, `blocked`, `handoff`, and `done` events for the same work item.

Default `LEASE_MINUTES` to `15` unless the user or project already specifies a different lease window.

## Claim Work

Keep claims as narrow as possible. Claim `.` only when the task truly spans the whole project.

Example:

```bash
python .codex/skills/gateway-agent/scripts/gateway_ledger.py append \
  --project-root <PROJECT_ROOT> \
  --agent-id worker-1 \
  --status claim \
  --folder python \
  --access-mode exclusive \
  --summary "Add retry handling to vector ingestion" \
  --eta 2026-04-19T20:00:00Z \
  --eta-confidence 0.7
```

The script reads the existing ledger, creates the header when missing, normalizes `folder_relpath`, generates `task_id` and `run_id` if needed, and writes either:

- a `claim`, or
- a `conflict` when an incompatible non-stale owner already exists.

If an overlapping active claim is expired, the script appends `stale_reclaim` first and then records the new `claim`.

## Update Active Work

Use the same `task_id` and `run_id` for the rest of the worker session.

Heartbeat:

```bash
python .codex/skills/gateway-agent/scripts/gateway_ledger.py append \
  --project-root <PROJECT_ROOT> \
  --agent-id worker-1 \
  --task-id <TASK_ID> \
  --run-id <RUN_ID> \
  --status heartbeat
```

Progress:

```bash
python .codex/skills/gateway-agent/scripts/gateway_ledger.py append \
  --project-root <PROJECT_ROOT> \
  --agent-id worker-1 \
  --task-id <TASK_ID> \
  --run-id <RUN_ID> \
  --status progress \
  --summary "Retry handling implemented; tests still running" \
  --eta 2026-04-19T20:30:00Z
```

Blocked:

```bash
python .codex/skills/gateway-agent/scripts/gateway_ledger.py append \
  --project-root <PROJECT_ROOT> \
  --agent-id worker-1 \
  --task-id <TASK_ID> \
  --run-id <RUN_ID> \
  --status blocked \
  --notes "Waiting for fixture update from task fixture-refresh"
```

If the detail does not fit in a single-line CSV field, write a note file such as `gateway_agent_messages/<TASK_ID>.md` under the project root and pass it through `--message-ref`.

## Handoff And Completion

Use `handoff` when ownership changes but task continuity matters. Preserve the same `task_id` and set `--handoff-to <agent-id>`.

Use `done` when the work item is complete. `done` closes the task and sets `completed_at_utc`.

Use `abandon` only when the work item is intentionally dropped. Record the short reason in `--notes`.

## Operating Rules

- Read the ledger before every write. The helper script does this internally, but inspect `snapshot` output when you need human-readable current state.
- Never let worker agents write directly to the CSV.
- Never rewrite or delete historical rows. The ledger is append-only.
- Never invent folder paths, ownership, summaries, or blockers that were not provided.
- Keep `summary` under 160 characters and keep all fields single-line.
- Store all timestamps as UTC ISO 8601 with a `Z` suffix.
- Use relative folder paths from `PROJECT_ROOT`; use `.` for project root.
- Treat the latest row for a `task_id` as that task's current state.

## References

- Read [references/ledger-spec.md](references/ledger-spec.md) when you need the full schema, field semantics, or state-derivation rules.
- Use [scripts/gateway_ledger.py](scripts/gateway_ledger.py) instead of hand-editing CSV rows.
