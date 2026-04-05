# Common DB Anti-Patterns

Flag these when they materially appear in the request or codebase.

## Modeling

- EAV schemas that hide business rules, destroy type safety, and make indexing guesswork.
- Unbounded JSON or text blobs used as primary storage for fields that should be typed and constrained.
- Missing primary keys, unstable natural keys, or surrogate keys added without preserving business uniqueness.
- Foreign-key relationships enforced only in application code when the database can enforce them safely.
- Nullable columns used as a substitute for lifecycle modeling or incomplete domain decisions.
- Soft deletes added everywhere without retention rules, query discipline, or supporting indexes.

## Query Design

- N+1 query patterns that multiply latency and load under real traffic.
- `SELECT *` on hot paths or wide tables.
- Leading-wildcard `LIKE` searches without an engine-specific indexing strategy.
- Functions or casts on indexed filter columns that prevent index use.
- Offset pagination on very large result sets without stable ordering or seek-based alternatives.
- Over-fetching JSON or blob columns on request paths that only need narrow fields.

## Indexing and Performance

- Adding indexes reactively without checking duplicate or overlapping coverage.
- Over-indexing write-heavy tables until inserts and updates become the bottleneck.
- Using wide, random, or frequently updated primary keys that amplify storage and secondary-index churn.
- Ignoring table growth, bloat, vacuum, or fragmentation until queries degrade in production.
- Denormalizing before measuring joins, cardinality, and access patterns.

## Migrations

- One-shot destructive migrations with no backup confidence, validation, or rollback plan.
- Large blocking DDL changes during peak traffic.
- Backfills that are not chunked, resumable, idempotent, or observable.
- Changing data types or keys in place when expand and contract would reduce risk.
- Shipping code that depends on new schema before the schema exists everywhere.

## Operations

- No restore tests despite having backups configured.
- No alerting on replication lag, long transactions, lock waits, slow queries, or storage growth.
- No runbooks for failover, storage exhaustion, or migration incidents.
- Treating replicas as magic capacity without understanding read lag or failover ownership.
- Ignoring maintenance work such as vacuum, statistics refresh, index rebuilds, or retention cleanup.

## Security and Governance

- Shared application superuser credentials for routine reads and writes.
- No separation between read, write, migration, and admin roles.
- PII stored or replicated broadly without classification, retention, or access review.
- Auditing requirements assumed complete because application logs exist.
- Tenant isolation handled only by convention when row-level or view-level controls are required.
