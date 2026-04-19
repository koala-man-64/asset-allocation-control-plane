# Gateway Ledger Spec

Use this file when you need the full ledger contract instead of the shorter workflow in `SKILL.md`.

## Ledger Path

- Write the ledger to `<PROJECT_ROOT>/gateway_agent_ledger.csv`.
- Treat the ledger as append-only.
- Create the file with this exact header when it does not exist:

```csv
schema_version,event_id,event_time_utc,project_id,folder_relpath,task_id,run_id,agent_id,access_mode,status,summary,started_at_utc,heartbeat_at_utc,eta_utc,lease_expires_at_utc,completed_at_utc,message_ref,depends_on,handoff_to,eta_confidence,notes
```

## Valid Values

- `access_mode`: `read`, `shared`, `exclusive`
- `status`: `claim`, `heartbeat`, `progress`, `blocked`, `handoff`, `done`, `abandon`, `stale_reclaim`, `conflict`

## Field Rules

- `schema_version`: current schema version. Use `1`.
- `event_id`: unique ID for the ledger event.
- `event_time_utc`: canonical gateway timestamp for the event.
- `project_id`: stable identifier for the project. Default to the project root folder name unless the user provides a better stable ID.
- `folder_relpath`: relative folder path inside the project. Use `.` for project root.
- `task_id`: stable ID for one work item across later events.
- `run_id`: stable ID for one worker session.
- `agent_id`: stable identifier for the worker agent.
- `summary`: short one-line task summary under 160 characters.
- `started_at_utc`: when the task began.
- `heartbeat_at_utc`: last liveness update known by the gateway.
- `eta_utc`: expected completion time.
- `lease_expires_at_utc`: time after which the task is stale unless renewed.
- `completed_at_utc`: completion timestamp for `done`.
- `message_ref`: optional project-relative note path such as `gateway_agent_messages/<task_id>.md`.
- `depends_on`: optional upstream task IDs or dependency label.
- `handoff_to`: target agent for `handoff`.
- `eta_confidence`: decimal between `0.0` and `1.0`.
- `notes`: short single-line note.

## Core Rules

1. Read the existing ledger before every write.
2. Normalize paths before comparing them.
3. Never write multiline text into the CSV.
4. Never invent ownership, folder paths, or task summaries.
5. Generate a new `task_id` when a new work item begins.
6. Reuse the same `task_id` for later `heartbeat`, `progress`, `blocked`, `handoff`, and `done` events for the same work item.
7. Keep `run_id` stable for the same worker session.
8. Every active task needs `lease_expires_at_utc`.
9. Refresh `heartbeat_at_utc` and `lease_expires_at_utc` whenever work is still active.
10. Keep summaries short and precise.

## Conflict Rules

Before writing `claim`, `heartbeat`, or `progress`, inspect the latest non-stale overlapping folder claims.

- `read` may coexist only with `read`.
- `shared` may coexist only with `shared`.
- `exclusive` conflicts with any active `read`, `shared`, or `exclusive` claim.
- `read` and `shared` conflict with each other.

If a valid non-expired conflicting claim exists, append `conflict` instead of overwriting it.

If an overlapping active claim has expired, append `stale_reclaim` before allowing the new `claim`.

## Event Handling

- `claim`
  - Create `task_id` if missing.
  - Set `started_at_utc` if missing.
  - Set `heartbeat_at_utc = event_time_utc`.
  - Set `lease_expires_at_utc = event_time_utc + LEASE_MINUTES`.
- `heartbeat`
  - Refresh `heartbeat_at_utc`.
  - Extend `lease_expires_at_utc`.
- `progress`
  - Keep `started_at_utc` unchanged.
  - Refresh heartbeat and lease.
  - Revise `eta_utc` when needed.
- `blocked`
  - Keep the task blocked, and optionally attach detail in `notes` or `message_ref`.
  - Revise `eta_utc` realistically.
- `handoff`
  - Record `handoff_to`.
  - Preserve `task_id` when continuity matters.
- `done`
  - Set `completed_at_utc`.
  - Treat the task as closed.
- `abandon`
  - Record a short reason in `notes`.
- `stale_reclaim`
  - Record that a previous active lease expired and a new agent reclaimed the work.

## Current State Interpretation

- A task is `active` when its latest status is `claim`, `heartbeat`, or `progress` and `lease_expires_at_utc` is still in the future.
- A task is `stale` when its latest active status has an expired lease.
- `blocked`, `handoff`, `done`, `abandon`, and `conflict` are explicit latest states rather than active ownership.
- Folder ownership is derived from the latest compatible non-stale active rows.

## Recommended Defaults

- Default `LEASE_MINUTES` to `15` unless the user or repo already defines a different cadence.
- Keep claims as narrow as possible to reduce false conflicts.
- Use note files for complex blocker or handoff detail instead of stuffing prose into CSV cells.
