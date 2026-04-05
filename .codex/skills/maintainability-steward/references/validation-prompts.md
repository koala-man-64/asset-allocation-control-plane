# Maintainability Steward Validation Prompts

Use these scenarios to validate behavior, prioritization, and strict output-format compliance.

## 1) PR Review Scenario (Small Diff)
### Prompt
Review this change as Maintainability Steward.

Context:
- Service: `billing-worker`
- Goal: add request tracing ID to logs.

Diff excerpt:
```diff
+ import uuid
  def process_invoice(invoice):
-     logger.info("processing invoice")
+     trace_id = str(uuid.uuid4())
+     logger.info(f"processing invoice trace={trace_id}")
      result = charge(invoice)
+     logger.info(f"finished invoice trace={trace_id}")
      return result
```

Constraints:
- No behavior change expected.
- Team wants low-risk merge this sprint.

### Expected Good Response Outline
- Use the exact 6-section output format in order.
- `1) Executive Summary`: identify low-risk change, call out logging quality and string-formatting issues.
- `If you do nothing else`: include three concrete actions (structured logging, correlation propagation, test for log context).
- `2) Findings`: include Simplicity, Supportability, Testing strategy, Dependencies/versioning.
- `3) Risks`: rank at least one risk about non-propagated trace IDs causing poor diagnostics.
- `4) Recommendations`: include P0/P1 actions with minimal pseudo-diff.
- `6) Scorecard`: score operability and dependency risk explicitly.

## 2) PR Review Scenario (Larger Refactor)
### Prompt
Review this refactor proposal as Maintainability Steward.

Context:
- Existing module: `orders/service.py` (~700 lines).
- Proposed change: split into 8 classes, add plugin registry, add event bus dependency.
- Rationale from author: “future extensibility.”

Constraints:
- No current requirement for plugins.
- Team has 5 engineers, one on-call rotation.

### Expected Good Response Outline
- Use the exact 6-section output format in order.
- Flag over-engineering and configuration sprawl.
- Recommend Option A as simpler decomposition with existing patterns.
- Include explicit on-call pain statement (debugging cross-module flow).
- Prioritize interface stability and incremental extraction plan.
- Include dependency scrutiny for new event bus package.

## 3) Design Review Scenario (Service Split)
### Prompt
Review this architecture proposal as Maintainability Steward.

Proposal:
- Split current `account-api` into `profile-service` and `preferences-service`.
- Introduce async messaging between services.

Current pain:
- p95 latency spikes during profile updates.

Constraints:
- No additional headcount.
- Must keep current API contract for 2 quarters.

Non-goals:
- No multi-region work this year.

### Expected Good Response Outline
- Use the exact 6-section output format in order.
- Recommend simplest viable design first (optimize monolith boundaries before split unless evidence supports split).
- Include reliability/failure-mode analysis for async messaging (retries, dedupe, DLQ).
- Include API/interface stability and migration/compat strategy.
- Include do-nothing/defer option with explicit risk.
- Provide P0/P1/P2 phased decisions.

## 4) Design Review Scenario (New API Endpoint)
### Prompt
Review this API design as Maintainability Steward.

Proposal:
- Add `POST /v2/recommendations/generate`.
- Endpoint starts background job, writes to Redis cache, polls status.
- Add two new dependencies for queue orchestration and schema validation.

Constraints:
- SLA: 99.9% monthly for read APIs.
- Expected traffic uncertain.

### Expected Good Response Outline
- Use the exact 6-section output format in order.
- Evaluate if design is under- or over-engineered for uncertain demand.
- Check dependency necessity and compatibility with existing stack.
- Require explicit failure modes: timeout, retry strategy, idempotency key, cancellation.
- Require operational docs: runbook, alerting thresholds, dashboard metrics.
- Recommend smallest effective endpoint contract and rollout plan.

## 5) Incident Follow-Up Scenario (Intermittent Timeouts)
### Prompt
Review this incident follow-up as Maintainability Steward.

Incident summary:
- Duration: 42 minutes.
- Symptom: 8-12% of checkout requests timed out.
- Contributing factors: connection pool saturation, no per-hop timeout, noisy logs.
- Current fix: increased pool size.

### Expected Good Response Outline
- Use the exact 6-section output format in order.
- Identify why current fix is insufficient alone.
- Provide concrete hardening plan across:
  - timeout budgets
  - retry/backoff guardrails
  - idempotency
  - actionable logs/metrics
  - alert tuning
  - runbook updates
- Include test plan for failure mode regression.
- Include ranked risks and near-term P0 actions.

## 6) Tech Debt Triage Scenario (Backlog Prioritization)
### Prompt
Prioritize this backlog as Maintainability Steward.

Backlog:
1. Replace custom retry helper in 12 services with shared utility.
2. Upgrade ORM major version.
3. Add tracing to cron jobs.
4. Remove dead feature flags (45 total).
5. Rewrite notification service in a new language.
6. Add smoke tests for deployment pipeline.

Constraints:
- 2 engineers for 1 quarter.
- On-call pages are currently high for cron and deploy failures.

### Expected Good Response Outline
- Use the exact 6-section output format in order.
- Rank by ROI + operational pain + risk reduction.
- Avoid sweeping rewrite recommendation unless explicitly justified.
- Provide incremental milestones with quarter-ready plan.
- Classify recommendations with P0-P3.
- Include scorecard reflecting improved operability/testability focus.
