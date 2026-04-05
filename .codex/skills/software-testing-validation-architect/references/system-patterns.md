# Testing Heuristics by System Pattern

## Table of Contents

- Web Apps and APIs
- Microservices and Distributed Flows
- Background Jobs, Schedulers, and Workers
- Queues, Pub/Sub, and Event-Driven Systems
- Databases and Repositories
- Authentication and Authorization Flows
- File and Document Processing
- Legacy Systems with Weak Tests
- CI/CD and Release Validation

## Web Apps and APIs

### Protect with unit tests

- request validation and normalization
- business rules behind endpoints
- mapping between DTOs, domain models, and persistence models
- pagination, filtering, sorting, and field selection rules
- authorization decisions that can be isolated from framework glue

### Protect with integration tests

- routing, middleware, dependency wiring, and persistence behavior
- serialization and deserialization
- transaction handling
- cache and repository coordination
- idempotent write behavior where relevant

### Protect with contract tests

- request and response schemas
- error shapes
- backward compatibility for versioned endpoints

### Protect with end-to-end tests

- a thin set of critical user journeys
- login, checkout, approval, onboarding, or other business-critical flows

### Common blind spots

- authorization gaps hidden behind happy-path tests
- pagination or filter edge cases
- partial update semantics
- retry and timeout behavior
- null and optional field handling
- timezone and locale assumptions

## Microservices and Distributed Flows

### Focus areas

- service contracts and backward compatibility
- retries, backoff, and timeout behavior
- partial failure handling
- duplicate message or request handling
- eventual consistency and ordering assumptions
- saga or compensation logic where relevant

### Recommended balance

- keep most business logic at unit level
- verify service seams and persistence in integration tests
- use contract tests aggressively between services
- use workflow tests sparingly for the most critical cross-service flows

### Common blind spots

- one service changes payload shape without downstream protection
- retry logic causes duplicate side effects
- partial success leaves state inconsistent
- consumer assumptions are not version-safe

## Background Jobs, Schedulers, and Workers

### High-value scenarios

- schedule triggers and missed-run behavior
- retries, backoff, and poison-item handling
- idempotency on rerun
- partial completion and resume logic
- concurrency controls and duplicate execution
- cutoff times, DST changes, and timezone boundaries

### Strong test mix

- unit tests for decision logic and item-selection rules
- integration tests for persistence, locking, and external calls
- workflow tests for trigger-to-side-effect behavior when jobs are mission-critical

### Common blind spots

- jobs that succeed locally but fail under real schedule timing
- duplicated work after retries or restarts
- state not rolled back after partial failure
- skipped or double-processed records at date boundaries

## Queues, Pub/Sub, and Event-Driven Systems

### High-value scenarios

- publish and consume payload correctness
- ack, nack, retry, dead-letter, and poison-message behavior
- deduplication and idempotency
- out-of-order delivery
- reprocessing and replay handling
- version compatibility of events

### Recommended balance

- unit tests for handlers and routing decisions
- integration tests for queue adapters and persistence interactions
- contract tests for event schemas and compatibility
- selective workflow tests for critical event chains

### Common blind spots

- handlers assume ordering guarantees that do not exist
- retries create duplicate writes or notifications
- dead-letter paths are untested
- consumer silently ignores new or missing fields

## Databases and Repositories

### High-value scenarios

- query correctness under realistic data shape
- transaction boundaries
- migrations and backward-compatible rollout assumptions
- unique constraints, null handling, and soft-delete behavior
- optimistic locking or concurrency behavior where relevant

### Recommended balance

- unit tests for query-building helpers only when logic is non-trivial
- integration tests for actual repository behavior and migrations
- contract tests only when DB-facing interfaces are versioned across components

### Common blind spots

- tests using fake repositories that miss SQL or ORM behavior
- migration order problems
- collation, timezone, or encoding differences
- missing rollback tests around multi-step persistence flows

## Authentication and Authorization Flows

### High-value scenarios

- deny-by-default behavior
- role and permission matrix coverage
- tenant isolation
- expired, revoked, or malformed credentials
- session refresh and revocation
- privileged workflow approvals and step-up auth when relevant

### Recommended balance

- unit tests for policy evaluation rules
- integration tests for framework wiring and token/session handling
- workflow tests for a few critical role-based journeys

### Common blind spots

- only admin or happy-path roles tested
- missing negative permission cases
- stale or revoked tokens still work
- tenant boundary leakage

## File and Document Processing

### High-value scenarios

- malformed, partial, oversized, duplicate, and unsupported inputs
- encoding and locale issues
- parse failures and fallback behavior
- temporary file cleanup
- resumable or retry-safe processing
- content-driven branching and metadata extraction

### Recommended balance

- unit tests for parsers, validators, and transformation rules
- integration tests with real sample files
- workflow tests only for business-critical file ingestion paths

### Common blind spots

- tests use only clean sample files
- large-file behavior is never exercised
- parser exceptions are swallowed without clear failure signals
- temp files or partial outputs accumulate after failure

## Legacy Systems with Weak Tests

### Starting strategy

- protect critical paths first
- add characterization tests before refactoring risky behavior
- anchor external interfaces with contract or integration tests
- extract seams that let logic move toward unit-testable code

### Practical priorities

- payment, authorization, reporting, scheduling, and data-integrity paths
- modules with incident history
- change-prone areas without focused tests

### Common blind spots

- teams chase broad coverage numbers before understanding system risk
- refactors happen without characterization tests
- heavy end-to-end suites mask the absence of targeted lower-level protection

## CI/CD and Release Validation

### Strong sequencing

- run lint and fast unit tests early
- run focused integration or contract checks on PR when change risk justifies it
- run broader suites on merge, nightly, or pre-release based on cost and risk
- gate releases with the smallest set of checks that proves critical behavior

### High-value checks

- migration validation
- smoke tests for deployment readiness
- environment parity for critical dependencies
- flake tracking with ownership and burn-down expectations
- test result visibility tied to release decisions

### Common blind spots

- expensive suites block developers but still miss key regressions
- flaky tests are normalized and ignored
- release gates do not match deployment risk
- environment drift makes results hard to trust
