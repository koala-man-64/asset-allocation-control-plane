# Strict Failure Checklist

Use this checklist for deep reviews, refactors, or incident-driven audits where failure behavior needs a systematic pass.

## Core Assumptions
- Assume every external call can fail.
- Assume every input can be invalid.
- Assume every async operation can cancel or time out.
- Assume every state transition can stop midway.
- Assume every ambiguous contract will eventually be misused.

## What To Examine In Every Code Path

### 1. Input validation
- Reject null, empty, malformed, out-of-range, missing, and illegal combinations at the boundary.
- Reject silent normalization of invalid data unless explicitly specified and surfaced.

### 2. Type and contract strength
- Prefer enums, discriminated unions, value objects, typed results, or stricter signatures over string flags, generic maps, and ambiguous nullable returns.
- Do not use `null`, empty strings, empty collections, `0`, or `false` as disguised failure signals when they can be confused with valid domain states.

### 3. Exception behavior
- Replace broad catch blocks with precise exception handling wherever feasible.
- Preserve root cause and stack trace when adding context.
- Reject swallow, catch-log-continue, or rethrow-without-context patterns.

### 4. Return-value honesty
- Flag any failure path that returns a success-looking value, partial object, stale data, or synthetic placeholder without an explicit degraded-mode contract.

### 5. Logging and observability
- Ensure failure is logged once at the correct layer with operation, entity, and request context.
- Require messages to state what failed, where, why, and expected-versus-actual values when relevant.
- Never log secrets, credentials, or tokens.

### 6. Resource safety
- Verify files, sockets, streams, transactions, locks, and temporary resources are cleaned up on both success and failure paths.
- Verify rollback or compensation when the operation cannot be atomic.

### 7. External dependency handling
- Require explicit timeouts.
- Require bounded retries with clear terminal failure.
- Reject retries for non-idempotent operations unless the contract explicitly protects correctness.
- Reject hidden fallback to stale or fabricated dependency data.

### 8. Async and concurrency behavior
- Verify cancellation is observed and propagated.
- Verify background-task exceptions are not orphaned.
- Check for race conditions, deadlocks, duplicate work, and misleading completion signals.

### 9. State integrity
- Verify failure cannot leave domain state corrupted or ambiguous while outward behavior remains successful.
- Check transaction boundaries, rollback, and partial-write behavior.

### 10. User-visible and operator-visible behavior
- Keep user-facing messages safe but concrete.
- Preserve diagnostic precision for operators and developers.
- Do not replace a specific internal failure with vague UI copy that blocks correct handling.

### 11. Naming and semantic honesty
- `TryX` must not throw for expected failure conditions.
- `Get` must not create or silently substitute data unless named for that behavior.
- `success=true` must not coexist with degraded, partial, or failed outcomes.

### 12. Test coverage for failure paths
- Require tests for invalid input, invariant violations, parse failures, dependency outages, timeouts, retries, cancellation, partial writes, rollback, cleanup, misleading default prevention, message clarity, and contract consistency.

## Hard Ban Checklist
- Empty catch blocks
- Bare `except`
- Broad catch without a justified boundary reason
- Catch-log-continue on critical flows
- Default return values that conceal failure
- Silent fallback to stale or fabricated data
- Ignored timeout or cancellation outcomes
- Conflated not-found and error semantics
- Conflated partial success and success
- Generic or misleading error messages
- Weak typing where strict typing is feasible
- Placeholder `TODO` or `FIXME` in critical failure paths

## Standard Remediation Moves
- Add strict guards at boundaries.
- Replace weak primitives with stronger domain types.
- Replace ambiguous nullable or boolean contracts with explicit typed outcomes.
- Replace vague exceptions and messages with concrete ones that include operation context.
- Remove silent fallbacks and force callers to handle failure explicitly.
- Add targeted negative tests for each corrected failure path.
