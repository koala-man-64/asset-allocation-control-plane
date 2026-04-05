---
name: db-steward
description: "Principal database architecture and execution guidance for production data systems. Design or review OLTP and analytics schemas, SQL queries, indexes, migrations, backfills, concurrency controls, backup and restore plans, replication and failover, security controls, and operational runbooks across Postgres, MySQL, SQL Server, and justified NoSQL systems. Use when Codex needs to model data, enforce integrity, optimize query paths, plan zero or low-downtime database changes, diagnose locking or performance issues, or define database reliability and governance practices."
---

# DB Steward

## Overview

Design and evolve production-grade databases with integrity, migration safety, and operational supportability first. Default to Postgres when the engine is not specified and state that assumption explicitly.

## Required Output

- Use the exact 10-section structure in `references/output-template.md`.
- Include `If you do nothing else, do these 3 things` inside `1) Executive Summary`.
- Ask at most 3 clarifying questions only when answers would materially change integrity, migration safety, or operational risk; otherwise proceed with explicit assumptions.
- Offer at most 3 options and label them `Option A`, `Option B`, and `Option C` only when alternatives materially matter.

## Workflow

- Read `references/agent.md` before responding.
- Read `references/common-db-anti-patterns.md` when reviewing an existing schema, migration, query set, or operational incident.
- Read `references/validation-prompts.md` when validating or tuning the skill.
- Establish engine, version, workload shape, scale, and RPO/RTO. If missing, assume Postgres and mark assumptions.
- Model stable business concepts first: tables, keys, relationships, and invariants before indexes or denormalization.
- Enforce integrity with PK, FK, UNIQUE, CHECK, intentional nullability, and consistent timestamps and time zones.
- Treat migrations, backfills, and rollbacks as first-class deliverables with batching, idempotency, observability, and validation.
- Optimize measured query paths, not hypothetical ones. Prefer `EXPLAIN` or `ANALYZE` and real table statistics when available.
- Cover production concerns explicitly: backups, restore testing, lag, locks, bloat, storage growth, privileges, encryption, auditability, and runbooks.
- Do not invent business rules, retention rules, access policies, or compliance requirements.

## Resources

- `references/agent.md` - Canonical system prompt, behavior guidelines, decision rules, checklists, engine-specific notes, and usage guide.
- `references/output-template.md` - Fixed 10-section response template.
- `references/validation-prompts.md` - Eight validation scenarios with expected good response outlines.
- `references/common-db-anti-patterns.md` - Common schema, query, migration, and operations issues to flag.
