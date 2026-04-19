# Testing Heuristics By System Pattern

## Table Of Contents

- Web Apps And APIs
- Background Jobs, Schedulers, And Data Pipelines
- Queues, Pub/Sub, And Event-Driven Systems
- Microservices And Distributed Flows
- Databases, Repositories, And Migrations
- Authentication And Authorization
- Frontend, Accessibility, And Client State
- CLI And Desktop Applications
- File And Document Processing
- Feature Flags, Analytics, And Experimentation
- Legacy Systems With Weak Tests
- CI/CD And Release Validation

## Web Apps And APIs

### High-value coverage

- request validation, normalization, and error shapes
- business rules behind endpoints
- DTO, domain, and persistence mapping
- pagination, filtering, sorting, partial updates, and idempotent writes
- authorization and tenant isolation
- transaction boundaries and cache coordination

### Common failure modes

- happy-path tests miss deny-by-default behavior
- partial update semantics corrupt data
- null, optional, or default fields behave differently than expected
- retries or duplicate requests create duplicate side effects
- timezone and locale handling drift around boundaries

### Observability and recovery

- verify logs, metrics, traces, and alerting for failed writes, timeouts, and auth failures
- verify rollback or compensation for multi-step writes

## Background Jobs, Schedulers, And Data Pipelines

### High-value coverage

- schedule triggers, missed-run logic, and rerun safety
- item selection rules and cutoff windows
- retries, backoff, poison-item handling, and dead-letter behavior
- idempotency on rerun or restart
- partial completion, checkpointing, resume logic, and concurrency controls
- DST, timezone, and date-boundary behavior

### Common failure modes

- jobs succeed in unit tests but fail under real schedule timing
- retries duplicate writes, notifications, or downstream events
- partial failures leave state inconsistent or invisible
- cutoff logic skips or double-processes records around date boundaries
- watermark or checkpoint logic regresses silently

### Observability and recovery

- verify job-level metrics, lag, throughput, retry counts, dead-letter counts, and error visibility
- verify runbooks, replay procedures, and safe rollback or replay controls

## Queues, Pub/Sub, And Event-Driven Systems

### High-value coverage

- publish and consume payload correctness
- ack, nack, retry, dead-letter, and poison-message behavior
- deduplication, idempotency, replay, and reprocessing
- out-of-order delivery and eventual consistency assumptions
- event version compatibility

### Common failure modes

- handlers assume ordering guarantees that do not exist
- consumer code silently ignores new or missing fields
- retries create duplicate side effects
- dead-letter handling exists on paper but not in tests

### Observability and recovery

- verify correlation IDs, queue depth metrics, consumer lag, dead-letter visibility, and replay tooling
- verify operational guidance for reprocessing and safe backfills

## Microservices And Distributed Flows

### High-value coverage

- service contracts and backward compatibility
- timeouts, retries, circuit breaking, and compensation
- partial failure handling across service boundaries
- saga orchestration or workflow state transitions
- authentication propagation and tenant context propagation

### Common failure modes

- one service changes a payload shape without downstream protection
- retries multiply side effects
- partial success leaves the system in an inconsistent state
- workflow state machines accept invalid transitions

### Observability and recovery

- verify cross-service traceability, correlation IDs, failure alarms, and compensation visibility
- verify rollback, replay, or manual repair procedures for partial failures

## Databases, Repositories, And Migrations

### High-value coverage

- query correctness against realistic data shape
- transaction behavior and rollback assumptions
- unique constraints, null handling, soft-delete semantics, and optimistic locking
- migration ordering and backward-compatible rollout assumptions
- concurrent writes and stale-read behavior

### Common failure modes

- fake repositories hide SQL or ORM behavior
- migration order or backfill assumptions fail in production
- collation, timezone, precision, or encoding mismatches corrupt data
- rollback paths for multi-step persistence flows are untested

### Observability and recovery

- verify migration monitoring, audit trails, backfill metrics, and rollback plans
- verify enough logging exists to diagnose partial writes or lock contention

## Authentication And Authorization

### High-value coverage

- deny-by-default behavior
- role and permission matrix coverage
- tenant isolation
- expired, revoked, malformed, and stale credentials
- privileged approvals, step-up auth, and delegated access when relevant

### Common failure modes

- only happy-path or admin roles are tested
- negative permission cases are missing
- revoked sessions still work
- tenant boundaries leak through caching or shared queries

### Observability and recovery

- verify security-significant auth events are logged and alertable
- verify operators can revoke access, expire sessions, or disable a feature safely

## Frontend, Accessibility, And Client State

### High-value coverage

- critical user journeys and client-side validation
- loading, empty, error, offline, and retry states
- keyboard navigation, focus order, screen-reader cues, and color contrast
- client-side permission handling without trusting it as the only gate
- browser, viewport, and device-specific behavior for critical flows

### Common failure modes

- optimistic UI hides backend failures
- stale client state causes invalid transitions or duplicate submissions
- accessibility only works on the happy path
- analytics fire on success but not on failure or abandonment

### Observability and recovery

- verify client error capture, meaningful user-facing messages, and breadcrumbs for failed flows
- verify users can recover from interrupted or stale sessions

## CLI And Desktop Applications

### High-value coverage

- argument parsing, config loading, and environment fallback behavior
- file permission, path, encoding, and newline handling
- interrupted execution, partial output cleanup, and rerun safety
- upgrade and backward-compatibility behavior for persisted state or config

### Common failure modes

- tests only cover clean paths and default config
- partial writes leave corrupted local state
- interactive flows break in unattended or headless environments

### Observability and recovery

- verify logs, exit codes, and diagnostic output are actionable
- verify resumable or repair-safe behavior after interrupted execution

## File And Document Processing

### High-value coverage

- malformed, partial, oversized, duplicate, and unsupported inputs
- encoding, locale, and metadata issues
- parser failures, fallback behavior, and temporary file cleanup
- resumable processing and content-driven branching

### Common failure modes

- tests use only clean sample files
- large-file behavior is never exercised
- parser exceptions are swallowed or downgraded silently
- temp files or partial outputs accumulate after failure

### Observability and recovery

- verify parse failures, skip counts, and output integrity are visible
- verify cleanup, replay, and quarantine flows are safe

## Feature Flags, Analytics, And Experimentation

### High-value coverage

- flag-off, flag-on, and mid-rollout behavior
- compatibility between old and new code paths
- analytics events on success, failure, abandonment, and retry
- experiment assignment consistency and exposure logging

### Common failure modes

- flag defaults differ across environments
- stale clients or workers execute the wrong code path
- analytics blind spots hide failed or abandoned journeys
- experiment cohorts are polluted by missing exposure or segmentation logic

### Observability and recovery

- verify dashboards, event validation, and rollout kill switches
- verify operators can disable a flag without leaving inconsistent state behind

## Legacy Systems With Weak Tests

### Starting strategy

- protect critical paths first
- add characterization tests before refactoring risky behavior
- anchor external interfaces with contract or integration tests
- extract seams that move important logic toward deterministic lower layers

### Common failure modes

- teams chase broad coverage numbers before understanding business risk
- refactors land without characterization tests
- large end-to-end suites mask the absence of focused lower-level coverage

## CI/CD And Release Validation

### Strong sequencing

- run lint and fast deterministic tests early
- run focused integration or contract checks on PR when the change risk justifies it
- run broader suites on merge, nightly, or pre-release based on cost and risk
- gate releases with the smallest set of checks that proves critical behavior

### High-value checks

- migration validation
- smoke tests for deployment readiness
- environment parity for critical dependencies
- flake tracking with ownership and exit criteria
- monitoring validation and rollback readiness tied to the release decision

### Common failure modes

- expensive suites block developers but still miss key regressions
- flaky tests are normalized and ignored
- release gates do not match deployment risk
- environment drift makes results hard to trust
