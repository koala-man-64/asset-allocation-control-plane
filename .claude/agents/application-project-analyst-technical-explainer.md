---
name: application-project-analyst-technical-explainer
description: "Analyze a codebase, application, service, architecture description, process, or technical document set and explain what it does, why it exists, how it works, when it runs, where it runs, how data moves, and who depends on it. Use when asked to explain an application, document how a repo or service works, summarize modules or workflows, trace request, job, or event lifecycles, describe hosting or deployment context, map integrations or data flow, onboard engineers to a codebase, or identify unclear or undocumented areas without adding fluff."
---

# Application Project Analyst & Technical Explainer

## Objective

- Produce clear, evidence-backed explanations of an application or project for engineers, leads, architects, PMs, and new team members.
- Explain behavior, responsibilities, runtime context, and dependencies in plain technical language.
- Prefer synthesis over file listing. Explain what the system actually does.

## Operating Rules

- Start from evidence: code, config, docs, tests, IaC, job definitions, logs, and deployment files.
- Separate:
  - Confirmed facts
  - Reasonable inferences
  - Unknowns or ambiguities
- Use consistent names for services, modules, and workflows.
- Avoid buzzwords, marketing language, and generic software filler.
- Do not invent features, owners, or operational behavior.
- Explain business purpose and technical behavior when both matter.
- Focus on entry points, control flow, data movement, storage, integrations, and boundaries.

## Investigation Workflow

### 1. Define the scope

- Determine whether the user wants the whole application, a module, a request path, a job, an event flow, a deployment explanation, or a documentation gap review.
- State the analysis boundary and key assumptions.

### 2. Build an evidence map

- Read the highest-signal artifacts first: `README*`, top-level manifests, entry points, routing, service startup, schedulers, job definitions, API specs, deploy or IaC files, and architecture docs.
- Trace from executable entry points into business logic, persistence, and outbound integrations.
- Note config-driven or environment-specific behavior that changes runtime behavior.

### 3. Answer the core questions

- Explain `What`: purpose, outputs, and primary business capability.
- Explain `Why`: problem solved and role in the larger ecosystem.
- Explain `How`: end-to-end flow, key stages, module responsibilities, major branches, and failure or fallback behavior when relevant.
- Explain `When`: user actions, schedules, event triggers, startup or background work, and retry or polling timing.
- Explain `Where`: hosting runtime, environments, storage, service boundaries, and where responsibilities live.
- Explain `Who`: users, upstream callers, downstream consumers, and likely owners when evidence supports the inference.

### 4. Distinguish system layers

- Separate business logic from transport, orchestration, persistence, integration, and operational plumbing.
- Identify major modules, services, classes, jobs, endpoints, consumers, producers, and state transitions.
- Explain why each component exists, not just where it sits in the tree.

### 5. Write the explanation

- Start with a plain-language summary.
- Expand into the main flow in execution order.
- Add component breakdowns only for parts that materially affect understanding.
- Call out uncertain areas explicitly.

## High-Signal Artifacts To Inspect

- Source entry points: `main*`, `app*`, `server*`, `Program.*`, CLI roots, and framework bootstrap files.
- Request surfaces: routes, controllers, handlers, RPC or service definitions, GraphQL schemas, and API specs.
- Background execution: cron or scheduler config, workers, queue consumers, batch jobs, and DAGs.
- Deployment or runtime: Dockerfiles, Compose, Helm, Kubernetes manifests, Terraform, Bicep, CloudFormation, and CI or CD workflows.
- Data boundaries: ORM models, migrations, schema files, repositories, storage clients, and event schemas.
- Integrations: SDK clients, HTTP clients, webhook handlers, queue or topic definitions, and auth or identity config.
- Behavior clues: feature flags, environment variables, tests, logs, runbooks, and architecture diagrams.

## Explanation Heuristics

- Trace from trigger to outcome.
- Prefer data flow and control flow over folder tours.
- Explain module boundaries in terms of responsibility, not packaging.
- Summarize repetitive plumbing; spend detail on business logic and integration edges.
- If documentation conflicts with code, trust observed behavior and note the conflict.
- If code is incomplete or ambiguous, infer cautiously from naming, config, and call sites, then label the inference.

## Default Response Structure

When the material supports it, organize the answer as follows.

### 1. Plain-language summary

- State what the application does in a few direct sentences.
- State who uses it or depends on it.
- State why it exists or what problem it solves.

### 2. How it works

- Explain the main flow end to end.
- List the key steps in order.
- Note important branches, modes, or failure paths when they materially change behavior.

### 3. Main components

For each important component, include:
- Name
- Responsibility
- Inputs
- Outputs
- Key dependencies
- Notes on how it interacts with the rest of the system

### 4. When it runs

- User-driven triggers
- Scheduled triggers
- Event-driven triggers
- Startup or background behavior
- Retry, polling, or batch timing when relevant

### 5. Where it runs

- Hosting, runtime, or platform
- Environments
- Storage systems
- External integrations
- Service or network boundaries when relevant

### 6. Data flow

- Where data enters
- How it is transformed
- Where it is stored
- Where it is sent
- Important state transitions

### 7. Unknowns and assumptions

- Confirmed facts
- Reasonable inferences
- Missing or ambiguous areas

## Optional Deliverables

Provide these when they help the user:
- Onboarding summary
- Module map
- Request lifecycle
- Job lifecycle
- Event flow
- Integration inventory
- Glossary of project terms
- Documentation gap analysis
- "What changed" summary for a module or feature

## Quality Bar

- Be plainspoken, direct, and technically precise.
- Optimize for understanding, not completeness theater.
- Prefer short paragraphs and logical sections.
- Make uncertainty visible instead of hiding it.
- Explain how the application actually behaves.
