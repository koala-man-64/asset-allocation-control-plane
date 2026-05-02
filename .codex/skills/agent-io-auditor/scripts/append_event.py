#!/usr/bin/env python3
"""Validate and append an Agent I/O audit event to a JSONL log."""

from __future__ import annotations

import argparse
import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

APPROVED_EVENT_TYPES = {
    "task_started",
    "task_claimed",
    "input_received",
    "context_loaded",
    "action_planned",
    "tool_invoked",
    "artifact_created",
    "artifact_updated",
    "artifact_deleted",
    "output_produced",
    "validation_passed",
    "validation_failed",
    "retry_scheduled",
    "blocked",
    "handoff",
    "task_completed",
    "task_abandoned",
    "conflict_detected",
}

APPROVED_STATUSES = {
    "planned",
    "in_progress",
    "succeeded",
    "failed",
    "blocked",
    "partial",
    "handed_off",
    "abandoned",
}

LIST_FIELDS = ("input_refs", "output_refs", "changed_resources")
STRING_FIELDS = (
    "event_id",
    "event_time_utc",
    "agent_id",
    "team_id",
    "project_id",
    "task_id",
    "run_id",
    "parent_event_id",
    "event_type",
    "status",
    "action_summary",
    "input_summary",
    "intended_output",
    "actual_output",
    "downstream_effect",
    "handoff_to",
    "error_summary",
    "notes",
)

REQUIRED_NON_EMPTY_FIELDS = (
    "agent_id",
    "project_id",
    "task_id",
    "run_id",
    "event_type",
    "status",
    "action_summary",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event_id(now: datetime) -> str:
    return f"evt-{now.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}"


def load_payload(args: argparse.Namespace) -> Dict[str, Any]:
    if args.event_file:
        return json.loads(Path(args.event_file).read_text(encoding="utf-8-sig"))
    if args.event_json:
        return json.loads(args.event_json)

    raw = sys.stdin.read().strip()
    if not raw:
        raise ValueError("No event payload supplied. Use --event-file, --event-json, or stdin.")
    return json.loads(raw)


def normalize_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        raise TypeError(f"Expected a scalar string value, got {type(value).__name__}.")
    return str(value).strip()


def normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, list):
        raise TypeError(f"Expected a list or string, got {type(value).__name__}.")

    normalized: List[str] = []
    for item in value:
        text = normalize_string(item)
        if text:
            normalized.append(text)
    return normalized


def normalize_event_time(value: str, default_now: datetime) -> str:
    if not value:
        return format_utc(default_now)

    candidate = value
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        raise ValueError("event_time_utc must include timezone information.")
    return format_utc(dt)


def normalize_payload(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw_payload, dict):
        raise TypeError("Event payload must be a JSON object.")

    now = utc_now()
    event: Dict[str, Any] = {"schema_version": 1}

    for field in STRING_FIELDS:
        event[field] = normalize_string(raw_payload.get(field, ""))

    for field in LIST_FIELDS:
        event[field] = normalize_list(raw_payload.get(field, []))

    event["event_time_utc"] = normalize_event_time(event["event_time_utc"], now)
    if not event["event_id"]:
        event["event_id"] = make_event_id(now)

    if "schema_version" in raw_payload:
        event["schema_version"] = int(raw_payload["schema_version"])

    if event["event_type"] not in APPROVED_EVENT_TYPES:
        raise ValueError(
            f"event_type must be one of: {', '.join(sorted(APPROVED_EVENT_TYPES))}."
        )
    if event["status"] not in APPROVED_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(APPROVED_STATUSES))}.")
    if event["schema_version"] != 1:
        raise ValueError("schema_version must be 1.")

    for field in REQUIRED_NON_EMPTY_FIELDS:
        if not event[field]:
            raise ValueError(f"{field} is required.")

    return event


def append_event(log_path: Path, event: Dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(event, separators=(",", ":"), ensure_ascii=True))
        handle.write("\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and append an Agent I/O audit event to a JSONL log."
    )
    parser.add_argument(
        "--log",
        default="agent_io_audit.jsonl",
        help="Path to the JSONL log file. Defaults to ./agent_io_audit.jsonl",
    )
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument("--event-file", help="Path to a JSON file containing one event object.")
    input_group.add_argument("--event-json", help="Inline JSON object containing one event.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the normalized event without appending it.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        raw_payload = load_payload(args)
        event = normalize_payload(raw_payload)
        if args.dry_run:
            print(json.dumps(event, indent=2, ensure_ascii=True))
        else:
            append_event(Path(args.log), event)
            print(f"Appended {event['event_id']} to {Path(args.log).resolve()}")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
