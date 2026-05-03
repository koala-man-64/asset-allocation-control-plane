---
name: code-humanizer
description: "Transform code so it reads like pragmatic, hand-maintained production code without changing behavior. Use when asked to humanize awkward or machine-like code, improve naming, comments, labels, log messages, docstrings, or local structure, and make files feel natural, idiomatic, domain-aware, and consistent with the surrounding codebase while preserving public APIs, data contracts, SQL semantics, tests, and external side effects."
---

# Code Humanizer

## Purpose

Rewrite supplied code so it reads like normal team-written production code.
Preserve behavior, contracts, and local conventions; improve clarity, naming, comments, labels, and small-scale structure without churning the file.

## Expect Inputs

- Language or framework
- One or more files or snippets to revise
- Optional style-anchor files from the same repo
- Optional aggressiveness level: `conservative`, `balanced`, or `aggressive`
- Optional constraints such as `preserve public method names` or `do not touch SQL`

Infer missing style cues from nearby code before asking questions. Ask only when missing context makes a behavior-preserving rewrite unsafe.

## Follow Priorities

Apply these priorities in order:
1. Preserve behavior.
2. Match repo and file-local conventions.
3. Improve naming.
4. Improve comments, labels, logs, and messages.
5. Simplify local structure.
6. Remove generated-code patterns and ceremony.

## Run Workflow

1. Read the target code and at least one nearby style anchor when available.
2. Identify the smallest set of safe improvements with clear payoff:
   - Generic or misleading names
   - Comments that narrate obvious code
   - Mechanical phrasing in labels, logs, docstrings, or tests
   - Local structure that hides intent behind ceremony
3. Separate changes into:
   - Safe and local
   - Risky because they may affect behavior or external contracts
   - Out of scope because they require wider coordination
4. Apply only the safe and local changes needed to make the code read naturally.
5. Re-check behavior-sensitive surfaces:
   - Public APIs and method names
   - Serialization keys and wire contracts
   - SQL text and query semantics
   - Regexes, parsing rules, and formatting rules
   - Telemetry names, log schemas, analytics events, and i18n keys
   - Reflection-based names, DI registration, CSS selectors, and snapshot expectations
6. Run targeted tests, lint checks, or formatters when working in a repo and the cost is reasonable.
7. Return revised code plus a short summary and any risky assumptions or intentional non-changes.

## Humanize Naming

- Prefer domain language over placeholders like `data`, `payload`, `value`, `obj`, `temp`, `helper`, `util`, `processData`, `handleResponse`, or `finalResult`.
- Choose names that describe intent and business meaning, not just type or shape.
- Keep names proportionate. Avoid both one-letter noise and essay-length identifiers.
- Preserve public or externally referenced names unless the user explicitly allows broader refactoring.
- Rewrite test names so the scenario and expected outcome are easy to scan.

## Humanize Comments And Text

- Delete comments that merely restate the code.
- Keep or add comments that explain business rules, compatibility constraints, edge cases, tradeoffs, or why a choice exists.
- Rewrite docstrings to sound practical and maintenance-oriented, not ceremonial.
- Rewrite UI labels, validation messages, error text, log messages, and section headers to sound concise, specific, and natural.
- Keep machine-parsed strings, placeholders, structured log fields, and stable message contracts unchanged unless the user explicitly allows edits.
- Do not mention AI, machine-generated code, or authorship unless the user explicitly asks for it.

## Simplify Structure Carefully

- Prefer local cleanup over broad rewrites.
- Inline helpers that only add ceremony.
- Extract helpers only when repeated logic or intent becomes clearer.
- Remove fake symmetry, empty region headers, and boilerplate docblocks when they add no value.
- Avoid over-defensive code unless the surrounding context clearly requires it.
- Preserve file organization unless a small reorder materially improves readability.

## Use Aggressiveness Modes

### Conservative
- Rename obvious offenders.
- Simplify or remove low-value comments.
- Tighten labels, logs, and test names.
- Make minimal structural edits.

### Balanced (default)
- Do conservative work.
- Apply small local refactors that improve flow, grouping, and readability.

### Aggressive
- Do balanced work.
- Perform deeper local cleanup when behavior and external contracts remain intact.
- Stop before changes that require cross-file coordination unless the user explicitly allows it.

## Respect Guardrails

- Treat runtime behavior, public APIs, data contracts, serialization keys, SQL semantics, test outcomes, and external side effects as fixed by default.
- Avoid changing accessibility hooks, analytics event names, feature-flag keys, dependency injection wiring, reflection targets, or persistence mappings unless the user explicitly permits it.
- Avoid fake typos, fake bugs, random inconsistency, or gimmicks.
- Avoid stylistic rewrites that make the code less clear.
- Avoid changing code only to make it look different.
- If a readability improvement would introduce behavior risk, leave it unchanged and call it out.

## Return Output

Return output in this order:
1. Revised code
2. Short summary of the most meaningful improvements
3. Risky assumptions or places intentionally left unchanged
4. Optional diff only when the user asks for it

Before returning, check:
- Does the code still behave the same way?
- Do names reflect domain intent?
- Do comments explain why instead of narrating the obvious?
- Does the result match the surrounding codebase?
- Would this look normal in a mature repository?
- Did you avoid or clearly flag risky changes?
