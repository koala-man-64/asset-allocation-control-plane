# Performance Bottleneck Investigator Reference

## Table of Contents

- Mission
- Evidence Standard
- Supported Inputs
- Default Workflow
- Bottleneck Checklist
- Common Mishaps Checklist
- Ranking Rules
- Required Output
- Response Rules

## Mission

- Act like a senior performance engineer reviewing real evidence, not guessing from symptoms.
- Diagnose bottlenecks across jobs, workers, services, APIs, databases, queues, caches, and infrastructure.
- Identify the most likely bottleneck first.
- Separate symptom from root cause and call out contributing factors.
- Recommend fixes that improve latency, throughput, stability, or capacity without creating disproportionate complexity.
- State what is known, what is inferred, and what remains unverified.

## Evidence Standard

- `Confirmed`: Directly supported by code, telemetry, traces, profiles, logs, query plans, configs, dashboards, benchmark results, or reproducible measurements.
- `Likely`: Strongly inferred from the available evidence, but not directly confirmed by decisive measurement.
- `Possible`: Plausible explanation that fits the symptoms, but more data is needed before treating it as a real finding.
- Never present `Likely` or `Possible` items as facts.
- If evidence is thin, say what measurement would confirm or falsify the hypothesis.

## Supported Inputs

Use this skill with any mix of:

- source code and diffs
- architecture descriptions and deployment topology
- traces, spans, flame graphs, and profiler output
- APM dashboards and service-level metrics
- database query plans, slow query logs, and index definitions
- queue depth, lag, retry, and consumer metrics
- cache hit ratio, eviction, and key-distribution signals
- worker pool, thread pool, connection pool, and runtime metrics
- load-test output, synthetic test results, or incident timelines
- infrastructure metrics for CPU, memory, disk, network, and autoscaling

## Default Workflow

### 1. Define the problem precisely

- State the user-visible symptom in concrete terms: latency, throughput loss, timeout rate, queue lag, backlog growth, CPU saturation, or cost/latency regression.
- Identify the affected path, workload, job, endpoint, queue, database, or service boundary.
- Note the scope and blast radius.

### 2. Identify the most likely bottleneck

- Determine the current constraining resource or mechanism: CPU, memory, lock, network, disk, connection pool, downstream dependency, query shape, queue consumer capacity, cache behavior, or concurrency limit.
- Prefer the narrowest statement supported by evidence.
- If multiple candidates exist, rank them instead of collapsing them into one vague explanation.

### 3. Separate symptom from root cause

- Distinguish what users or operators observe from the mechanism causing it.
- Trace the causal chain:
  - trigger
  - bottleneck
  - propagation path
  - visible symptom
- Call out contributing factors separately from the primary cause.

### 4. Check common mishaps

Explicitly evaluate the relevant items below before finalizing conclusions:

- N+1 queries
- bad or missing indexes
- full table or document scans
- chatty service-to-service or client-to-service calls
- oversized payloads or serialization overhead
- synchronous I/O on hot paths
- thread pool starvation
- lock contention
- connection pool exhaustion
- retry storms
- cache stampede
- low cache hit rate
- GC pressure
- memory leaks
- queue lag
- backpressure failures
- DNS overhead
- TLS handshake or certificate overhead
- disk I/O saturation
- autoscaling lag, floor/ceiling misconfiguration, or cold-start amplification

When one of these is present, explain whether it is the root cause, a contributing factor, or only a symptom amplifier.

### 5. Rank the findings

- Rank findings by severity, confidence, and expected impact.
- Severity answers: how bad is this if unresolved?
- Confidence answers: how strong is the evidence?
- Expected impact answers: how much latency, throughput, stability, or capacity gain should this fix create?
- Prefer fewer high-signal findings over a long speculative list.

### 6. Recommend concrete fixes

For each important finding:

- state the fix clearly
- explain why it targets the bottleneck
- note tradeoffs and operational risk
- estimate effort as `Low`, `Medium`, or `High`
- define how to validate the improvement

### 7. Reduce uncertainty

- Name the missing measurements that would most reduce uncertainty.
- Prefer measurements that decisively separate competing hypotheses.
- End with a validation plan that proves whether the fix worked under realistic load.

## Bottleneck Checklist

Inspect the layers most likely to constrain performance:

### Application And Service Layer

- hot endpoints, background jobs, fan-out patterns, batching behavior, and request amplification
- synchronous work on request threads
- serialization and deserialization cost
- payload size, compression, and parsing overhead
- retry, timeout, circuit-breaker, and fallback behavior under load
- thread pool, worker pool, and async scheduling pressure

### Database Layer

- query count and query shape
- execution plans, scans, sorts, spills, and missing indexes
- row-by-row access patterns and N+1 behavior
- lock waits, deadlocks, and transaction length
- replication lag and read/write split issues
- connection pool pressure and server-side resource saturation

### Queue And Worker Layer

- producer rate versus consumer throughput
- lag, backlog growth, visibility timeout, retry, and dead-letter volume
- uneven partition or shard distribution
- worker concurrency, batch size, and acknowledgement strategy
- idempotency or retry behavior that multiplies load

### Cache Layer

- hit ratio, miss penalty, and cache key cardinality
- eviction churn, object size, and serialization overhead
- cache stampede and thundering herd behavior
- stale data policy and pre-warming strategy

### Infrastructure Layer

- CPU saturation, run queue, and throttling
- memory pressure, swapping, GC behavior, and leak symptoms
- disk throughput, IOPS, latency, and spill behavior
- NIC saturation, packet loss, and cross-zone or cross-region chatter
- DNS lookup latency, TLS handshake cost, and connection reuse
- autoscaling responsiveness and scaling constraints

## Common Mishaps Checklist

Use this list as a forcing function. If an item is relevant, either cite evidence for it or state that the current evidence is insufficient.

- N+1 queries or row-by-row access patterns
- missing, unused, or ineffective indexes
- full scans, broad sorts, or hash joins on large data sets
- chatty APIs or excessive round trips
- oversized payloads, repeated serialization, or heavy object mapping
- sync-over-async or blocking I/O in concurrency-sensitive paths
- thread pool starvation or scheduler contention
- mutex, row, table, or distributed lock contention
- connection pool exhaustion at the app or database layer
- retry storms or timeout feedback loops
- cache stampede, low hit rate, or poor cache key design
- GC pauses, allocation churn, or unmanaged memory growth
- memory leaks or unbounded in-memory queues
- queue lag, poison messages, or consumers below steady-state throughput
- backpressure mechanisms that fail open or fail late
- DNS latency, TLS renegotiation, or poor connection reuse
- disk bottlenecks from scans, temp files, flushes, or log pressure
- autoscaling that reacts too slowly or scales the wrong dimension

## Ranking Rules

Use these dimensions for every substantive finding:

- `Severity`: `Critical`, `High`, `Medium`, or `Low`
- `Confidence`: `Confirmed`, `Likely`, or `Possible`
- `Expected impact`: `Very high`, `High`, `Medium`, or `Low`

Tie impact to the user-visible outcome:

- latency reduction
- throughput increase
- queue drain improvement
- timeout or error reduction
- capacity headroom
- stability under burst load

## Required Output

Use this structure whenever the evidence supports a performance assessment.

### Executive summary

- Summarize the current performance posture in a few lines.
- Lead with the most likely bottleneck and the biggest practical wins.
- State the main uncertainty if the evidence is incomplete.

### Ranked findings

For each finding, use this format:

#### <Finding title>

- Severity: `<Critical | High | Medium | Low>`
- Confidence: `<Confirmed | Likely | Possible>`
- Expected impact: `<Very high | High | Medium | Low>`
- Affected component: `<service, job, worker, queue, DB, cache, API, node pool, etc.>`
- Likely bottleneck: `<current constraining resource or mechanism>`
- Symptom: `<what operators or users observe>`
- Root cause: `<primary cause or best current hypothesis>`
- Evidence: `<specific code, telemetry, trace, query-plan, or config evidence>`
- Why this is root cause instead of only symptom: `<causal explanation>`

### Root cause analysis

- Separate symptom from root cause for the top issues.
- Show the causal chain from trigger to bottleneck to visible effect.
- List contributing factors separately from the primary root cause.

### Recommended fixes

For each major fix:

#### <Fix title>

- Targets finding: `<finding title>`
- Recommendation: `<concrete change>`
- Rationale: `<why this should relieve the bottleneck>`
- Tradeoffs: `<complexity, risk, cost, or side effects>`
- Effort: `<Low | Medium | High>`
- Validation steps: `<before/after measurements, experiment, canary, load test, or query-plan comparison>`

### Common mishaps detected

- List the common mishaps that were confirmed or strongly suspected.
- For each, state whether it is the root cause, a contributing factor, or a secondary amplifier.
- Do not pad this section with generic items that were not evidenced.

### Missing data

- List the top missing measurements or artifacts that would most reduce uncertainty.
- Explain why each item matters.
- Prefer decisive measurements such as traces, flame graphs, query plans, pool metrics, queue lag by partition, cache hit ratio by key class, or autoscaling timelines.

### Validation plan

- Define how to prove the fix worked.
- Include the primary KPI, baseline, target, test method, and rollback signal.
- Prefer production-like load, canary comparison, and before/after telemetry.

### Final prioritized action list

- End with the smallest set of next actions in priority order.
- Balance urgency, effort, and expected impact.
- Separate immediate stabilization from deeper structural fixes when both are needed.

## Response Rules

- Be evidence-based. Do not invent facts.
- Separate confirmed findings, likely causes, and possible causes.
- Lead with the bottleneck, not the symptom description.
- Prefer causal explanations over generic tuning advice.
- Recommend the smallest fix that meaningfully changes the constraint.
- Call out when the real problem is observability gaps rather than performance alone.
- If multiple bottlenecks interact, rank the one that currently constrains throughput or latency the most.
- If there is not enough data to identify a bottleneck responsibly, say so and focus on the highest-value measurements to collect next.
