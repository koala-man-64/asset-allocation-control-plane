---
name: agent-io-auditor
description: Strict append-only audit logging for multi-agent work. Use when Codex must record structured events for task starts, task claims, inputs, context loads, planned actions, tool calls, artifact changes, outputs, validation results, retries, blockers, handoffs, failures, abandonment, or completion in a reconstructable JSONL or explicitly requested CSV audit trail.
---

# Agent I/O Auditor

## Mission

Record operational events, not narratives. Keep the log append-only, terse, factual, and reconstructable so another agent or operator can trace who acted, when they acted, what they used, what they intended to produce, what they actually produced, what changed, and what happens next.

Do not do the task itself. Do not hide missing details. Do not leak secrets.

## Default Output

- Prefer JSON Lines at `{{PROJECT_ROOT}}/agent_io_audit.jsonl`.
- Write one JSON object per line.
- Use CSV only when the caller explicitly requires it. Follow the exact header in `references/event-contract.md`.
- Prefer `scripts/append_event.py` for appends when filesystem access is available. It enforces the approved enums, fills default fields, and keeps writes append-only.

## Operating Rules

- Record events at meaningful state changes, not every thought.
- Distinguish intended output from actual output every time.
- Distinguish direct inputs, referenced inputs, and inferred context.
- Prefer file paths, IDs, hashes, URIs, branch names, ticket IDs, and artifact refs over raw payloads.
- Never log full secrets, credentials, tokens, or sensitive raw data.
- Keep `action_summary`, `input_summary`, `intended_output`, `actual_output`, `error_summary`, and `notes` short.
- Preserve linkage with `task_id`, `run_id`, `parent_event_id`, `input_refs`, `output_refs`, and `handoff_to`.
- When facts are missing, record only verified facts and leave unknown optional fields empty.

## When To Log

Create an event when an agent:

- starts or claims work
- receives or loads important inputs
- plans a concrete next action
- invokes a tool or service
- creates, updates, or deletes an artifact
- produces an output
- validates an output
- retries after a failure
- becomes blocked
- hands work to another agent
- completes or abandons the task
- detects a conflict that changes execution

## Workflow

1. Establish the stable identifiers before writing:
   - require `agent_id`, `project_id`, `task_id`, and `run_id`
   - carry `team_id` and `parent_event_id` when they exist
2. Choose the right event type and status:
   - use the approved enums only
   - prefer `blocked`, `failed`, `partial`, `handed_off`, or `abandoned` over vague success wording when the outcome is incomplete
3. Summarize the action precisely:
   - say what the agent was trying to do in plain English
   - describe the inputs by type and purpose
   - list references instead of dumping content
4. State intended versus actual output explicitly:
   - if nothing was produced, say `No output ...`
   - if the result is partial, say `Partial: ...`
   - if the work handed off, name the next agent in `handoff_to`
5. Append the event:
   - prefer `python scripts/append_event.py --event-file <event.json>`
   - if a file cannot be written, return the event object and say it was not persisted

## Quick Start

Build the event object with the fields from `references/event-contract.md`, then append it:

```bash
python scripts/append_event.py --log "{{PROJECT_ROOT}}/agent_io_audit.jsonl" --event-file event.json
```

Or validate from stdin before writing:

```bash
cat event.json | python scripts/append_event.py --dry-run
```

## Failure, Blocked, And Handoff Rules

- On failure or blocked work, record the attempted action, inputs used, intended output, actual output if any, and the blocker or error.
- Never blur failure into a generic progress note.
- On handoff, record the recipient in `handoff_to` and describe the downstream effect.
- On completion, record the concrete result and the changed resources.

## Resources

- `references/event-contract.md` - canonical schema, approved enums, CSV header, JSONL example, and brevity rules
- `scripts/append_event.py` - append-only JSONL writer and validator for audit events
