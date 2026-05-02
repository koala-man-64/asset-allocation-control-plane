# Agent I/O Audit Contract

## Core Purpose

Maintain a clean, minimal, append-only audit trail for a multi-agent system. The log must support accountability, debugging, handoffs, incident review, dependency tracing, replay, diagnosis, and team coordination.

## Event Shape

Capture these fields on every event. When a detail is unknown, keep the field empty instead of inventing a value.

- `schema_version` - use `1`
- `event_id` - unique ID for this audit event
- `event_time_utc` - UTC timestamp in ISO 8601 format
- `agent_id` - stable agent identifier
- `team_id` - optional team or swarm identifier
- `project_id` - project identifier
- `task_id` - stable work item ID
- `run_id` - execution or session ID
- `parent_event_id` - optional prior event link
- `event_type` - approved event type
- `status` - `planned`, `in_progress`, `succeeded`, `failed`, `blocked`, `partial`, `handed_off`, or `abandoned`
- `action_summary` - short plain-English description of the action
- `input_summary` - short summary of the inputs used
- `input_refs` - file paths, IDs, URIs, message IDs, artifact refs, or hashes used as inputs
- `intended_output` - what the agent aimed to produce
- `actual_output` - what the agent actually produced
- `output_refs` - file paths, IDs, URIs, message IDs, artifact refs, or hashes created or modified
- `changed_resources` - files, records, tickets, services, folders, or branches changed
- `downstream_effect` - expected effect on the next step
- `handoff_to` - target agent when handing work off
- `error_summary` - concise blocker or error description
- `notes` - optional short operational note

## Approved Event Types

Use one of these values:

- `task_started`
- `task_claimed`
- `input_received`
- `context_loaded`
- `action_planned`
- `tool_invoked`
- `artifact_created`
- `artifact_updated`
- `artifact_deleted`
- `output_produced`
- `validation_passed`
- `validation_failed`
- `retry_scheduled`
- `blocked`
- `handoff`
- `task_completed`
- `task_abandoned`
- `conflict_detected`

## Operational Rules

- Record events, not narratives.
- Be concise, specific, and factual.
- Never invent missing details.
- Separate intended output, actual output, no output, and partial output.
- Separate direct inputs, referenced inputs, and inferred context.
- Never log full secrets, credentials, tokens, or sensitive raw payloads.
- Prefer references, hashes, IDs, and short summaries over full content.
- Keep entries append-only. Never rewrite history.
- Keep identifiers consistent so events can be linked across agents and runs.
- Log enough detail for reconstruction without turning the log into noise.

## Input Rules

- Summarize the type and purpose of the inputs.
- Use `input_refs` for files, messages, tickets, artifact IDs, hashes, or URIs.
- Do not dump whole documents into the log.
- Do not include secrets or unnecessary raw content.

Examples:

- `input_summary`: `Used user story, API spec, and latest auth service diff`
- `input_summary`: `Used project ledger, failing CI logs, and test report from run 1842`
- `input_summary`: `Used prompt template v3 and dataset shard 07`
- `input_refs`: `docs/api/openapi.yaml`
- `input_refs`: `logs/ci/run_1842.txt`
- `input_refs`: `msg:planner-0281`
- `input_refs`: `artifact:test-plan-004`
- `input_refs`: `hash:9fd3c1...`

## Output Rules

Always separate intended output from actual output.

Examples:

- `intended_output`: `Produce regression test plan for payment retries`
- `actual_output`: `Created test_plan_payment_retries.md with 18 test cases`
- `intended_output`: `Patch auth middleware null-session crash`
- `actual_output`: `Partial: identified root cause and drafted patch, but no code committed`
- `intended_output`: `Generate release notes`
- `actual_output`: `No output due to missing changelog input`

When an output is an artifact, prefer `output_refs`:

- `reports/test_plan_payment_retries.md`
- `branch:fix/null-session-guard`
- `ticket:OPS-431`
- `msg:reviewer-009`

## Brevity Standard

Keep these fields brief:

- `action_summary` - ideally under 120 characters
- `input_summary` - ideally under 160 characters
- `intended_output` - ideally under 160 characters
- `actual_output` - ideally under 160 characters
- `error_summary` - ideally under 160 characters
- `notes` - ideally under 160 characters

## Failure And Blocked Events

When an agent fails or is blocked, log:

- what it attempted
- what inputs it used
- what output was intended
- what output, if any, was produced
- the blocker or error
- whether retry or handoff is needed

Never hide failure behind vague wording.

## Multi-Agent Traceability

Keep these links intact whenever possible:

- `task_id`
- `run_id`
- `parent_event_id`
- `handoff_to`
- `input_refs`
- `output_refs`

A reviewer should be able to answer:

- which agent touched the task
- which inputs it used
- what it intended to create
- what it actually created
- what changed
- who took over next
- where the failure started
- what should be inspected first

## File Format

Prefer JSON Lines because it is append-friendly and preserves structure better than CSV.

Default log path:

`{{PROJECT_ROOT}}/agent_io_audit.jsonl`

If CSV is explicitly required, use this header exactly:

```text
schema_version,event_id,event_time_utc,agent_id,team_id,project_id,task_id,run_id,parent_event_id,event_type,status,action_summary,input_summary,input_refs,intended_output,actual_output,output_refs,changed_resources,downstream_effect,handoff_to,error_summary,notes
```

CSV rules:

- one event per row
- no multiline values
- escape commas and quotes correctly
- store lists as semicolon-separated values if needed
- use references instead of large payloads

## JSONL Example

```json
{
  "schema_version": 1,
  "event_id": "evt-2026-04-19-00142",
  "event_time_utc": "2026-04-19T21:42:11Z",
  "agent_id": "tester-agent-2",
  "team_id": "release-swarm-a",
  "project_id": "checkout-service",
  "task_id": "task-qa-118",
  "run_id": "run-2026-04-19-07",
  "parent_event_id": "evt-2026-04-19-00141",
  "event_type": "output_produced",
  "status": "succeeded",
  "action_summary": "Generated negative test cases for coupon validation",
  "input_summary": "Used pricing rules doc, API schema, and recent validation bug report",
  "input_refs": [
    "docs/pricing_rules.md",
    "spec/openapi.yaml",
    "ticket:BUG-882"
  ],
  "intended_output": "Create negative test suite for invalid coupon combinations",
  "actual_output": "Created 14 negative API test cases and 4 UI validation checks",
  "output_refs": [
    "tests/coupons/negative_cases.md"
  ],
  "changed_resources": [
    "tests/coupons/negative_cases.md"
  ],
  "downstream_effect": "Reviewer can validate missing server-side checks",
  "handoff_to": "review-agent-1",
  "error_summary": "",
  "notes": "No execution performed; planning artifact only"
}
```

## Success Condition

A reviewer reading the audit log should be able to reconstruct the sequence of work across a multi-agent team without reading every internal message or every artifact in full.
