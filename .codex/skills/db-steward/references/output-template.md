# DB Steward Output Template

Use this exact section order. Include sections `2)` and `9)` only when they are needed.

## 1) Executive Summary

- 5 to 8 bullets.
- Include `If you do nothing else, do these 3 things`.
- Lead with integrity, migration safety, and the highest operational risks.

## 2) Assumptions

- State DB engine and version.
- State expected scale and workload shape.
- State SLA, RPO, and RTO assumptions when they matter.

## 3) Proposed Design

- `Entities and relationships`
- `Schema recommendations`
- `Integrity rules`
- `Naming conventions`
- `Example DDL`
- Put `Option A`, `Option B`, and `Option C` here only when multiple designs materially matter.
- Use engine-specific SQL in fenced blocks.

## 4) Access Patterns & Query Shapes

- List top reads and writes.
- Show example SQL for each critical query.
- Explain how the design supports each query.
- Identify where the design may struggle.

## 5) Indexing & Performance Plan

- List candidate indexes and why they exist.
- State expected gains and write-side costs.
- Explain how to verify with `EXPLAIN`, `ANALYZE`, or engine-specific plan tools.
- Flag anti-patterns such as leading-wildcard search, functions on indexed columns, unstable pagination, and over-indexing.

## 6) Migration & Delivery Plan

- Give expand and contract sequencing in order.
- Explain backfill batching, idempotency, resumability, and observability.
- Analyze lock risks and how to avoid long blocking operations.
- Include rollback steps.
- Include validation steps such as counts, checksums, invariants, or side-by-side query checks.

## 7) Operations & Supportability

- Cover backups and restore testing.
- Cover monitoring and alerting signals.
- Cover runbook checklist items for likely failure modes.
- Cover security controls such as roles, grants, encryption, row filtering, or auditing when relevant.

## 8) Risks

- Rank risks from highest to lowest.
- For each risk, include `impact`, `likelihood`, `detection`, and `mitigation`.

## 9) Questions

- Ask at most 3 questions.
- Ask only questions that materially change design, migration safety, or operational posture.

## 10) DB Steward Scorecard

- Score `integrity`, `performance`, `migration safety`, `operability`, and `simplicity` from 1 to 5.
- Add 1 to 2 sentences per score explaining why.
