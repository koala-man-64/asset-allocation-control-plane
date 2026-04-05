# DB Steward Validation Prompts

Use these scenarios to validate strict output-format compliance, practical database reasoning, realistic migration sequencing, concrete SQL examples, and verification guidance.

## Table of Contents

- 1) Design OLTP schema for orders, payments, and refunds
- 2) Add multi-tenant support with row-level tenancy
- 3) Optimize a slow search query with `LIKE` and pagination
- 4) Add a new `NOT NULL` column with backfill
- 5) Refactor a composite primary key to a surrogate key
- 6) Implement an outbox table for reliable messaging
- 7) Partition a rapidly growing time-series table
- 8) Incident: deadlocks and lock timeouts under load

## 1) Design OLTP schema for orders, payments, and refunds

### Prompt

Review this request as DB Steward.

Context:
- Engine not specified.
- Domain: ecommerce orders, card payments, and refunds.
- Requirements:
  - each order can have multiple payment attempts
  - partial refunds are allowed
  - duplicate payment-provider transaction IDs must be rejected
  - refunds must never exceed captured payment amount
  - finance needs daily aggregates by status and payment method
- Traffic:
  - 4 to 8 million orders per month
  - peak checkout writes during flash sales
- Non-goals:
  - no cross-region writes this year
  - no separate warehouse yet

### Expected Good Response Outline

- Use the exact 10-section structure in order and assume Postgres in `2) Assumptions`.
- `1) Executive Summary`: prioritize payment integrity, refund invariants, and a migration-safe path to finance reporting; include 3 concrete must-do actions.
- `3) Proposed Design`: recommend `Option A` as normalized OLTP tables for `orders`, `order_items`, `payments`, and `refunds`; enforce provider transaction uniqueness, captured-amount constraints, and refund-overrun protection; include Postgres DDL with PK, FK, `UNIQUE`, `CHECK`, and timestamp choices.
- `4) Access Patterns & Query Shapes`: include SQL for checkout lookup, payment capture history, refund issuance, and daily finance aggregation; identify where OLTP tables may struggle for heavy reporting.
- `5) Indexing & Performance Plan`: justify order lookup, payment-provider ID, refund-by-payment, and daily aggregate support indexes; mention write amplification risk on the payments table and how to verify with `EXPLAIN ANALYZE`.
- `6) Migration & Delivery Plan`: include initial create order, optional aggregate table or materialized-view phase, validation queries for refund totals, and rollback notes.
- `7) Operations & Supportability`: cover backups, slow-query monitoring, payment-table growth, and least-privilege roles for finance reads.
- `8) Risks`: rank refund invariant enforcement, reporting load on OLTP tables, and hot-row contention during payment updates.
- `10) DB Steward Scorecard`: score integrity highest only if refund and duplicate-payment constraints are concretely enforced.

## 2) Add multi-tenant support with row-level tenancy

### Prompt

Review this request as DB Steward.

Context:
- Engine: Postgres 15.
- Existing system: single-tenant SaaS with `accounts`, `projects`, `users`, `invoices`, and `audit_logs`.
- New goal: support multiple customer organizations in one cluster.
- Requirements:
  - tenant isolation must survive application bugs
  - admins can move a project between tenants during a migration window
  - most queries filter by tenant and recent activity
  - shared operational dashboards still need cross-tenant reads
- Constraints:
  - no per-tenant database this year
  - downtime budget: under 5 minutes

### Expected Good Response Outline

- Use the exact 10-section structure in order.
- `1) Executive Summary`: recommend shared-schema multi-tenancy with enforced `tenant_id` boundaries as `Option A`, plus 3 must-do actions around RLS, composite uniqueness, and phased rollout.
- `3) Proposed Design`: add `tenant_id` to tenant-owned tables, define composite business uniqueness, preserve global tables where appropriate, and include Postgres DDL and sample RLS policies.
- `4) Access Patterns & Query Shapes`: show tenant-scoped read and write SQL, admin cross-tenant query examples, and a project-transfer workflow that highlights integrity risks.
- `5) Indexing & Performance Plan`: recommend leading `tenant_id` on high-cardinality indexes and explain tradeoffs for cross-tenant admin reporting.
- `6) Migration & Delivery Plan`: add nullable `tenant_id`, backfill in batches, deploy dual-write or defaulting logic, build indexes concurrently, validate orphan counts and tenant ownership, then enforce `NOT NULL`, FKs, and RLS; include a rollback stance.
- `7) Operations & Supportability`: cover policy testing, query audit sampling, support runbooks for tenant-transfer issues, and least-privilege admin access.
- `8) Risks`: rank accidental cross-tenant data exposure, long backfills, and index regressions.
- `10) DB Steward Scorecard`: reduce simplicity if the response skips the operational burden of RLS and tenant-transfer tooling.

## 3) Optimize a slow search query with `LIKE` and pagination

### Prompt

Review this request as DB Steward.

Context:
- Engine: Postgres 14.
- Table: `customers` with 42 million rows.
- Query:
```sql
SELECT id, email, full_name, created_at
FROM customers
WHERE lower(full_name) LIKE lower('%smith%')
ORDER BY created_at DESC
LIMIT 50 OFFSET 5000;
```
- Symptoms:
  - p95 is 4.8 seconds
  - CPU spikes during business hours
  - API requires stable pagination and recent-first ordering
- Known data shape:
  - 15% of names contain repeated common substrings
  - 98% of calls search by prefix or infix on `full_name`

### Expected Good Response Outline

- Use the exact 10-section structure in order.
- `1) Executive Summary`: call out leading-wildcard search and deep offset pagination as the main issues; include 3 concrete actions.
- `3) Proposed Design`: keep schema changes minimal and explain `Option A` as trigram or full-text support plus seek pagination if API can evolve; include any required extension or generated-column DDL.
- `4) Access Patterns & Query Shapes`: rewrite the query with a deterministic tie-breaker and show both current-contract and improved-contract SQL.
- `5) Indexing & Performance Plan`: recommend a `GIN` trigram index or other engine-appropriate index, explain sort-support limitations, call out write and storage cost, and specify `EXPLAIN (ANALYZE, BUFFERS)` checks.
- `6) Migration & Delivery Plan`: include concurrent index creation, API rollout sequencing for seek pagination if adopted, and rollback if the index hurts writes.
- `7) Operations & Supportability`: include search latency dashboards, index build monitoring, and bloat tracking.
- `8) Risks`: rank search-index size, common-substring selectivity, and API compatibility if pagination changes.
- `10) DB Steward Scorecard`: do not award a high performance score without a concrete verification plan and stable pagination discussion.

## 4) Add a new `NOT NULL` column with backfill

### Prompt

Review this request as DB Steward.

Context:
- Engine: MySQL 8.0 with InnoDB.
- Table: `subscriptions` with 180 million rows.
- Change: add `billing_region VARCHAR(16) NOT NULL`.
- Existing data: value can be derived from `country_code` for 99.8% of rows; the rest require fallback to `unknown`.
- Traffic:
  - constant writes all day
  - replicas lag during large maintenance jobs
- Constraint:
  - application deploys happen twice per day

### Expected Good Response Outline

- Use the exact 10-section structure in order.
- `1) Executive Summary`: recommend expand and contract, batched backfill, and replication-aware rollout; include 3 must-do actions.
- `3) Proposed Design`: keep the column small and constrained, define allowed values strategy, and include MySQL DDL for adding the nullable column and later enforcing `NOT NULL`.
- `4) Access Patterns & Query Shapes`: include write-path SQL, batch backfill SQL, and any verification queries by region counts.
- `5) Indexing & Performance Plan`: state whether the new column needs an index now or later, with justification.
- `6) Migration & Delivery Plan`: add nullable column using the safest online DDL available, deploy app dual-write, backfill in chunks by PK range, monitor replica lag, validate no nulls remain, then switch to `NOT NULL`; include rollback and pause conditions.
- `7) Operations & Supportability`: cover lag alerts, backfill progress metrics, and a runbook for stopping or resuming batches.
- `8) Risks`: rank replica lag, metadata lock surprises, and bad derivation logic.
- `10) DB Steward Scorecard`: lower migration-safety score if the response skips resumability or replica impact.

## 5) Refactor a composite primary key to a surrogate key

### Prompt

Review this request as DB Steward.

Context:
- Engine: SQL Server 2022.
- Current table:
  - `invoice_line_items` primary key is `(invoice_id, line_number)`.
- Desired change:
  - add `invoice_line_item_id BIGINT IDENTITY` as the new primary key
  - many downstream tables currently reference the composite key
- Constraints:
  - old integrations still read by composite key for at least 6 months
  - large ETL jobs join on the current key nightly

### Expected Good Response Outline

- Use the exact 10-section structure in order.
- `1) Executive Summary`: recommend a phased surrogate-key introduction and preserving business uniqueness on the old key; include 3 must-do actions.
- `3) Proposed Design`: keep a `UNIQUE` constraint on `(invoice_id, line_number)`, add the surrogate key, and include SQL Server DDL for shadow columns and FK changes.
- `4) Access Patterns & Query Shapes`: show old-key compatibility queries, new-key joins, and ETL impact queries.
- `5) Indexing & Performance Plan`: discuss clustered-index choice, included columns if justified, and write/read tradeoffs for dual-key coexistence.
- `6) Migration & Delivery Plan`: add new column, backfill child tables with the surrogate key, dual-write or dual-read during transition, validate one-to-one mapping, switch FKs gradually, then consider PK flip; include rollback and lock-risk discussion.
- `7) Operations & Supportability`: include Query Store checks, ETL verification, and support guidance for mixed-key incidents.
- `8) Risks`: rank dual-key drift, lock escalation, and integration lag.
- `10) DB Steward Scorecard`: reduce simplicity if the response fails to preserve business uniqueness or glosses over mixed-mode support.

## 6) Implement an outbox table for reliable messaging

### Prompt

Review this request as DB Steward.

Context:
- Engine: Postgres 16.
- Service: billing service emits `invoice.paid` and `refund.issued` events.
- Current problem:
  - app publishes directly to Kafka after commit
  - intermittent publish failures create missing downstream events
- Requirements:
  - at-least-once delivery is acceptable
  - consumers must deduplicate safely
  - publish lag under 30 seconds
  - retention target: 14 days

### Expected Good Response Outline

- Use the exact 10-section structure in order.
- `1) Executive Summary`: recommend a transactional outbox as `Option A`, plus 3 must-do actions around idempotency keys, dispatcher safety, and retention.
- `3) Proposed Design`: include outbox table columns, status model, uniqueness strategy, and Postgres DDL with `created_at`, `available_at`, and dispatch attempt metadata.
- `4) Access Patterns & Query Shapes`: show insert-with-business-transaction SQL, dispatcher polling SQL using `FOR UPDATE SKIP LOCKED`, and purge or archival SQL.
- `5) Indexing & Performance Plan`: justify pending-event and retention indexes, and mention write amplification and cleanup cost.
- `6) Migration & Delivery Plan`: add table, deploy transactional writes first, then dispatcher, then consumer dedupe verification; include rollback and validation steps for event parity.
- `7) Operations & Supportability`: cover lag metrics, stuck-event alerts, retry runbooks, and role separation between app writes and dispatcher updates.
- `8) Risks`: rank dispatcher stalls, duplicate delivery, and retention cleanup backlog.
- `10) DB Steward Scorecard`: do not award a high operability score without monitoring and replay guidance.

## 7) Partition a rapidly growing time-series table

### Prompt

Review this request as DB Steward.

Context:
- Engine: Postgres 15.
- Table: `device_metrics`.
- Current size:
  - 2.4 billion rows
  - grows by 55 million rows per day
- Queries:
  - most reads target the last 7 days for one device or one tenant
  - compliance requires 18 months retention
- Current pain:
  - vacuum falls behind
  - deletes for retention are expensive
  - index size is growing faster than expected

### Expected Good Response Outline

- Use the exact 10-section structure in order.
- `1) Executive Summary`: recommend time-based partitioning as `Option A` only if operationally justified, plus 3 must-do actions around retention, partition pruning, and migration safety.
- `3) Proposed Design`: define partition key, granularity, local index pattern, and any tenant or device key considerations; include Postgres partition DDL.
- `4) Access Patterns & Query Shapes`: show recent-window queries, tenant-filter queries, and retention management queries.
- `5) Indexing & Performance Plan`: explain per-partition indexes, pruning expectations, and maintenance cost.
- `6) Migration & Delivery Plan`: include creation of new partitioned table or attach strategy, dual-write or routing, incremental backfill or historical attach plan, validation, and cutover; discuss lock-sensitive steps.
- `7) Operations & Supportability`: cover partition creation automation, retention-drop runbook, vacuum monitoring, and storage forecasts.
- `8) Risks`: rank migration complexity, partition-count explosion, and queries that miss pruning.
- `10) DB Steward Scorecard`: keep simplicity lower if the response fails to mention the operational tax of partition management.

## 8) Incident: deadlocks and lock timeouts under load

### Prompt

Review this incident as DB Steward.

Context:
- Engine: Postgres 16.
- Symptoms:
  - deadlocks spike during peak traffic
  - API requests updating `inventory` and `reservation` tables time out after 3 seconds
- Observations:
  - workers sometimes update `inventory` first, then `reservation`
  - another code path updates `reservation` first, then `inventory`
  - one batch job also touches both tables every 5 minutes
- Constraints:
  - no downtime for the fix
  - duplicate reservations must be prevented

### Expected Good Response Outline

- Use the exact 10-section structure in order, even though the problem is incident-driven.
- `1) Executive Summary`: prioritize consistent lock ordering, transaction-scope reduction, and diagnostic instrumentation; include 3 concrete actions.
- `3) Proposed Design`: explain the concurrency model, required uniqueness or idempotency protections, and any small schema changes such as missing indexes or reservation keys.
- `4) Access Patterns & Query Shapes`: show the contending update sequences and safer SQL patterns, plus diagnostic SQL against `pg_stat_activity` or `pg_locks`.
- `5) Indexing & Performance Plan`: call out any missing indexes that widen lock scope or prolong updates, and explain how to verify with wait-event and execution data.
- `6) Migration & Delivery Plan`: sequence code changes for deterministic lock order, batch-job throttling, optional retry logic, and validation under staged load; include rollback.
- `7) Operations & Supportability`: include deadlock logging, lock-wait dashboards, alert thresholds, and a runbook for draining or pausing the batch job.
- `8) Risks`: rank recurring deadlocks from partial rollout, retry storms, and integrity regressions if duplicate-prevention logic is weak.
- `10) DB Steward Scorecard`: do not score integrity or operability highly unless duplicate prevention, diagnostics, and rollback are all addressed.
