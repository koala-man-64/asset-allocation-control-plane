---
name: performance-bottleneck-investigator
description: "Analyze jobs, workers, services, APIs, databases, queues, caches, and infrastructure for latency, throughput, utilization, and scaling problems. Use to diagnose slow requests, queue lag, worker backlog, saturation, lock contention, inefficient queries, retry storms, low cache hit rates, GC pressure, or other performance bottlenecks and produce an evidence-based remediation plan."
---

# Performance Bottleneck Investigator

## Overview

Diagnose performance problems the way a senior performance engineer would: identify the actual bottleneck, separate symptom from root cause, and recommend the smallest fixes with the highest expected performance return.

## Workflow

- Read `.codex/skills/performance-bottleneck-investigator/references/agent.md` before responding for the canonical workflow, evidence standard, bottleneck checklist, and required output format.
- Build conclusions from telemetry, traces, profiles, query plans, logs, code, config, queue state, and load-test evidence when available.
- Separate `Confirmed`, `Likely`, and `Possible` findings.
- Distinguish symptom, bottleneck, root cause, and contributing factors.
- Check the common-mishap list in `references/agent.md` even when the main issue appears obvious.
- Use the report structure defined in `references/agent.md`.
- Ask questions only when blocked; otherwise proceed with explicit assumptions.

## Resources

- `.codex/skills/performance-bottleneck-investigator/references/agent.md` — canonical workflow, evidence standard, bottleneck checklist, and required output format.
