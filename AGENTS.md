# AGENTS.md instructions for /mnt/c/Users/rdpro/Projects/AssetAllocation

## Source of Truth

Repo-local agents live under `.codex/skills`. Keep this file aligned with that directory. Do not treat the older `.agent/` tree or user-home skill folders as part of this repository's agent contract.

## Cross-Repo Contract Routing

When planning or reviewing work in this repo, treat shared cross-repo data contract changes as owned by the sibling repo `asset-allocation-contracts`.

A change counts as a shared data contract change when it adds, removes, renames, retypes, or changes validation, serialization, or schema semantics for:

- shared API request and response payloads
- serialization keys or schema-backed shapes consumed across repos
- types mirrored from `asset-allocation-contracts` or `@asset-allocation/contracts`

This rule does not apply to:

- local DB schema
- internal-only DTOs
- repo-private view models or helpers not exported across repos

Required routing:

- Route shared contract shape changes through `asset-allocation-contracts` first.
- Use `asset-allocation-contracts/docs/architecture/master-design.md` as the contract process reference.
- Plan this repo only for adoption, adapters, migration, or version-bump work until an `asset-allocation-contracts` work item exists or is explicitly included as a prerequisite.

Required planning decision:

- State either `This is a contracts-repo-first change.` or `This is local-only and does not require contracts repo routing.`

Evidence to check when unsure:

- package dependency on `asset-allocation-contracts` or `@asset-allocation/contracts`
- repo docs or tests that describe `asset-allocation-contracts` as the owner of shared contracts

Default:

- If the shape may be shared and ownership is still unclear after checking local evidence, assume it is shared and route through `asset-allocation-contracts`.

## Agents

An agent is a repo-local instruction set stored in a `SKILL.md` file. The following agents are available in this repository.

### Available agents

- actionmedic: Autonomous GitHub Actions troubleshooting and repair for repositories with failing CI. Use when Codex needs to audit workflow runs on a branch or pull request, diagnose failed, cancelled, timed-out, or stuck jobs, implement the smallest justified fix, validate locally, commit and push, and monitor until the latest commit is green or a hard blocker is proven. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/actionmedic/SKILL.md)
- application-project-analyst-technical-explainer: Analyze a codebase, application, service, architecture description, process, or technical document set and explain what it does, why it exists, how it works, when it runs, where it runs, how data moves, and who depends on it. Use when asked to explain an application, document how a repo or service works, summarize modules or workflows, trace request, job, or event lifecycles, describe hosting or deployment context, map integrations or data flow, onboard engineers to a codebase, or identify unclear or undocumented areas without adding fluff. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/application-project-analyst-technical-explainer/SKILL.md)
- architecture-review-agent: Audit architecture and code for cloud-native reliability, security, operability, and performance. Use for architecture reviews or delivery-readiness audits requiring an Architecture & Code Audit Report and actionable work items. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/architecture-review-agent/SKILL.md)
- business-partner-agent: Business-side judgment for technical founders on contracts, negotiation, IP, licensing, data rights, pricing, vendor/customer risk, and business structure. Use when Codex needs to review or draft MSAs, SOWs, NDAs, DPAs, SLAs, SaaS terms, pilot agreements, vendor or partner terms, reseller or referral deals, API or data licenses, subcontractor terms, or when evaluating product, architecture, GTM, AI, or partnership choices for ownership, leverage, liability, margin, and operational risk. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/business-partner-agent/SKILL.md)
- cloud-cost-optimization-efficiency-architect: Analyze AWS, Azure, and GCP systems, services, platforms, architecture descriptions, IaC, Kubernetes clusters, CI/CD pipelines, cost reports, and engineering processes to find realistic cloud cost savings without creating unacceptable reliability, performance, security, compliance, or delivery risk. Use when reviewing cloud spend, finding waste or inefficiency, evaluating FinOps and governance gaps, assessing cost and performance tradeoffs, or producing a prioritized cloud cost reduction plan. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/cloud-cost-optimization-efficiency-architect/SKILL.md)
- cloud-security-vulnerability-expert: Pragmatic application, cloud, and vulnerability security review for modern software systems. Use when assessing or remediating security issues in source code, web apps, APIs, microservices, distributed systems, cloud architecture, IAM, secrets management, containers, Kubernetes, serverless, Terraform, CloudFormation, Bicep, Helm, Kubernetes manifests, CI/CD pipelines, dependency or supply-chain risk, vulnerability scan findings, threat models, or cloud hardening across AWS, Azure, and GCP. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/cloud-security-vulnerability-expert/SKILL.md)
- code-drift-sentinel: Detect, score, attribute, and remediate code drift caused by multiple AI agents and humans across style, architecture, APIs, dependencies, behavior, performance, security, tests, docs, and CI/config. Use when running drift audits locally or in CI, enforcing drift-score quality gates, generating drift reports, or preparing safe auto-remediation patches. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/code-drift-sentinel/SKILL.md)
- code-humanizer: Transform code so it reads like pragmatic, hand-maintained production code without changing behavior. Use when asked to humanize awkward or machine-like code, improve naming, comments, labels, log messages, docstrings, or local structure, and keep the result consistent with surrounding repo conventions and external contracts. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/code-humanizer/SKILL.md)
- code-hygiene-agent: Apply safe, behavior-preserving lint and refactor changes for readability and conventions. Use when asked to clean up code without changing behavior, and confirm CI tool alignment. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/code-hygiene-agent/SKILL.md)
- db-steward: Principal database architecture and execution guidance for production data systems. Design or review OLTP and analytics schemas, SQL queries, indexes, migrations, backfills, concurrency controls, backup and restore plans, replication and failover, security controls, and operational runbooks across Postgres, MySQL, SQL Server, and justified NoSQL systems. Use when Codex needs to model data, enforce integrity, optimize query paths, plan zero or low-downtime database changes, diagnose locking or performance issues, or define database reliability and governance practices. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/db-steward/SKILL.md)
- delivery-engineer-agent: Implement production-ready, testable code changes and cloud-native deployment-ready config. Use for feature or bug delivery tasks requiring an Implementation Report with operational readiness. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/delivery-engineer-agent/SKILL.md)
- delivery-orchestrator-agent: Hybrid orchestrator, scrum master, and tech lead that scopes requests into work items, routes to the right agents, enforces gates, prevents loops and thrash, maintains single-source-of-truth status, and outputs an Orchestrator Update. Use when coordinating multi-agent delivery, managing handoffs, or deciding done versus rest. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/delivery-orchestrator-agent/SKILL.md)
- fail-fast-enforcement-agent: Audit and remediate systems that hide failure, report false success, or degrade silently. Use when reviewing code, diffs, workflows, health or readiness checks, startup configuration, data pipelines, caches, parsers, background jobs, or agent/tool orchestration for swallowed exceptions, misleading success states, stale fallback data, masked dependency or config errors, partial completion, or unknown state treated as healthy. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/fail-fast-enforcement-agent/SKILL.md)
- forensic-debugger: Use for evidence-first technical investigations of system failures spanning applications, infrastructure, networking, databases, CI/CD, and distributed services. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/forensic-debugger/SKILL.md)
- frontend-design: Create distinctive, production-grade frontend interfaces with high design quality and a clear aesthetic point of view. Use when asked to design or restyle web UI in React, Next, Vite, HTML, or CSS, including typography, color systems, layout composition, and motion. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/frontend-design/SKILL.md)
- gateway-bookkeeper: Enforce MCP-first tool usage through a centralized gateway and bookkeeper: discover MCP capabilities, route intents to the best MCP tool, block non-MCP execution when an MCP equivalent exists, and log all tool calls, results, and auditable fallbacks. Use when building or operating a multi-agent system where agents must not access tools or networks directly. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/gateway-bookkeeper/SKILL.md)
- cleanup-change-debris-auditor: Audit PRs, diffs, changed files, modules, and repositories for accidental leftovers, stale compatibility code, debug residue, dead code, duplicate logic, stale tests and fixtures, temp artifacts, and other change debris. Use when a safe, prioritized cleanup plan with evidence and validation steps is needed. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/cleanup-change-debris-auditor/SKILL.md)
- maintainability-steward: Stewardship-focused review for software simplicity, maintainability, supportability, and long-term cost efficiency. Use when reviewing PRs, diffs, architecture proposals, incident follow-ups, and tech-debt backlogs to surface maintainability risks and prioritized P0-P3 actions. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/maintainability-steward/SKILL.md)
- performance-bottleneck-investigator: Analyze jobs, workers, services, APIs, databases, queues, caches, and infrastructure for latency, throughput, utilization, and scaling problems. Use when Codex needs to diagnose slow requests, queue lag, worker backlog, saturation, lock contention, inefficient queries, retry storms, low cache hit rates, GC pressure, or other performance bottlenecks and produce an evidence-based remediation plan. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/performance-bottleneck-investigator/SKILL.md)
- project-workflow-enforcer-agent: Enforce the repository's authoritative multi-agent delivery workflow, routing policy, and compliance gates. Use when building a compliant plan for new work, reviewing a proposed plan, ticket, PR, or workflow for compliance, enforcing incident or CI-failure routing, or recommending periodic workflow-governance reviews without replacing delivery-orchestrator-agent or project-workflow-auditor-agent. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/project-workflow-enforcer-agent/SKILL.md)
- project-workflow-auditor-agent: Audit a repository or project for security practices, CI/CD workflow safety, adherence to project instructions, and consistency across code, config, and docs. Use when preparing for release, reviewing GitHub Actions workflows, enforcing engineering guardrails, or assessing delivery readiness and governance. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/project-workflow-auditor-agent/SKILL.md)
- qa-release-gate-agent: Risk-based testing, verification, and CI/CD quality gating. Use when asked to test or verify changes, create test plans, review CI/CD workflows, or assess release readiness. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/qa-release-gate-agent/SKILL.md)
- senior-trading-desk-reviewer: Institutional equities strategy, trade, execution, portfolio risk, and performance review. Use when Codex needs to critique trading strategies, backtests, trade blotters, execution logs, equity curves, journals, code snippets, portfolio summaries, or performance reports as a skeptical head trader focused on robustness, attribution, execution quality, and risk-adjusted returns rather than idea generation. (file: /mnt/c/Users/rdpro/Projects/asset-allocation-control-plane/.codex/skills/senior-trading-desk-reviewer/SKILL.md)
- software-testing-validation-architect: Assess how well a codebase or system is tested and design pragmatic validation strategies across unit, integration, contract, workflow, regression, and end-to-end layers. Use when reviewing a test suite, coverage report, pull request, repository, or CI/CD pipeline for missing tests, weak assertions, flakiness, regression risk, distorted test layering, or poor use-case coverage. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/software-testing-validation-architect/SKILL.md)
- technical-writer-dev-advocate: Turn engineering work into clear, accurate, usable documentation and developer enablement assets with evidence-backed traceability. Use when creating or updating docs from source code, specs, and tests, auditing docs for correctness and gaps, or preparing PR-ready docs changes that reduce support load and speed time-to-first-success. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/technical-writer-dev-advocate/SKILL.md)
- ui-testing-expert: Senior UI QA lead for web and mobile product changes. Use when reviewing UI PRs or diffs, creating risk-based UI test plans, designing exploratory charters, triaging UI bugs or flaky tests, or improving automation, accessibility, and cross-browser or device coverage without building brittle suites. (file: /mnt/c/Users/rdpro/Projects/AssetAllocation/.codex/skills/ui-testing-expert/SKILL.md)

- strict-branch-and-merge-discipline: Mandatory git safety workflow for any task that changes code in one or more repositories. Use before editing files, creating branches or worktrees, committing, rebasing, pushing, syncing with base, opening PRs, or coordinating multi-repo changes when branch isolation, collision avoidance, ownership boundaries, and safe merge discipline matter. (file: /mnt/c/Users/rdpro/Projects/asset-allocation-control-plane/.codex/skills/strict-branch-and-merge-discipline/SKILL.md)

## How to Use Agents

- Discovery: the list above is the authoritative repo-local agent inventory for this session. Agent bodies live on disk at the listed paths.
- Trigger rules: if the user names an agent, or the task clearly matches an agent description, use that agent for the current turn. Multiple mentions mean use the minimal set that covers the request. Do not carry agents across turns unless re-mentioned.
- Missing or blocked: if a named agent is missing or unreadable, state that briefly and continue with the best fallback.
- Progressive disclosure:
  1. Open the target `SKILL.md`.
  2. Read only enough to follow the workflow.
  3. Resolve relative paths from the agent directory first.
  4. Reuse referenced scripts, assets, and templates instead of recreating them.
- Coordination:
  - If multiple agents apply, state the order you will use them and why.
  - If you skip an obvious agent, say why.
- Context hygiene:
  - Keep context small.
  - Load only the files needed for the current request.
  - Avoid deep reference chasing unless blocked.
- Safety and fallback: if an agent cannot be applied cleanly, state the issue, pick the next-best approach, and continue.

## Evidence

- `.codex/skills`
- `find .codex/skills -maxdepth 2 -name SKILL.md | sort`
