# UI Testing Expert Validation Prompts

Use these scenarios to validate strict output-format compliance, risk-based prioritization, and practical automation guidance.

## Table of Contents

- 1) Login + MFA UI change (web)
- 2) Checkout/payment form refactor (web, high risk)
- 3) Table filtering + pagination + URL query params (web)
- 4) File upload with progress + cancel + retry (web)
- 5) Mobile app onboarding flow change (mobile)
- 6) Accessibility complaint: keyboard trap on modal
- 7) Flaky test: intermittent timeout waiting for element
- 8) Cross-browser bug: works on Chrome, broken on Safari

## 1) Login + MFA UI change (web)

### Prompt

Review this change as UI Testing Expert.

Context:
- Platform: web
- Flow: email/password login now uses an inline MFA step instead of redirecting to a separate screen
- New behavior: resend-code timer, remember-device checkbox, inline error messaging
- Supported browsers: latest Chrome, Firefox, Safari
- Criticality: all users pass through this flow
- Acceptance criteria:
  - existing password success and failure paths still work
  - remembered devices skip MFA for 30 days
  - locked accounts must not bypass MFA
  - keyboard-only users can complete the flow

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` focuses on login success, invalid password, expired code, remember-device persistence, and refresh or back-button behavior; `Top risks` stays at 5 or fewer.
- `3) Coverage Map`: includes loading, bad credentials, expired code, resend timer, remembered device, locked account, and route-state recovery.
- `4) Test Cases`: P0 emphasizes auth correctness and session integrity; P1 adds accessibility and browser variance; P2 covers lower-risk responsive polish.
- `5) Automation Plan`: recommends a small E2E auth smoke set plus component or integration tests for validation and timer logic; prefers `data-testid` on username, password, MFA input, resend timer, remember-device control, and inline errors; rejects CSS or XPath selectors.
- `6) Matrix`: includes Chrome, Firefox, Safari desktop plus one narrow mobile viewport for responsive sanity.
- `7) Bug Report Template`: either marks not applicable or provides a filled example tied to an expired-code defect.

## 2) Checkout/payment form refactor (web, high risk)

### Prompt

Create a test plan as UI Testing Expert.

Context:
- Platform: web
- Area: checkout page refactor
- Changes:
  - shipping and billing forms merged into a stepper
  - payment entry moved into a shared card component
  - promo-code field now updates totals asynchronously
- Supported browsers: latest Chrome, Safari, Edge
- Non-goals: tax engine rewrite, new payment methods
- Risks called out by team: conversion drop, duplicate charge, validation regressions

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` prioritizes pay-success, pay-failure, duplicate-submit protection, promo-code recalculation, and refresh or back navigation within the stepper.
- `3) Coverage Map`: explicitly covers loading totals, invalid card or address, partial failure between pricing and payment, empty cart, and refresh persistence.
- `4) Test Cases`: P0 covers payment correctness, totals integrity, and destructive regression prevention; P1 covers keyboard and a11y on the stepper; P2 covers broader browser or viewport polish.
- `5) Automation Plan`: splits shared-card validation into component or integration coverage and keeps only a few E2E checkout journeys; recommends stable selectors on step actions, totals, submit CTA, promo-code state, and server-returned error surfaces.
- `6) Matrix`: starts with Chrome, Safari, Edge desktop plus one mobile browser-width checkout pass, then explains when to add real-device coverage.
- `7) Bug Report Template`: provides a filled payment duplicate-submit example or marks not applicable if no single defect is under triage.

## 3) Table filtering + pagination + URL query params (web)

### Prompt

Review this feature as UI Testing Expert.

Context:
- Platform: web
- Feature: orders table with server-side filtering, sorting, pagination, and URL query param persistence
- Shared component: same table shell is reused by customers, invoices, and refunds pages
- Acceptance criteria:
  - filters persist in URL
  - browser back and forward restores prior state
  - refresh preserves current table state
  - empty state and error state are distinct

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` prioritizes filter-to-URL sync, refresh persistence, back or forward restoration, page-reset rules when filters change, and error versus empty-state distinction.
- `3) Coverage Map`: includes loading, empty results, server error, partial failure between filter controls and table data, and stale URL state.
- `4) Test Cases`: P0 covers state integrity and routing correctness; P1 covers accessibility of sort and filter controls plus shared-component downstream regression; P2 covers lower-risk visual and breakpoint checks.
- `5) Automation Plan`: recommends component or integration tests for query-param serialization rules and a thin E2E set for deep linking and navigation; prefers stable selectors on table region, filter inputs, sort headers, pagination, status banner, and empty-state container.
- `6) Matrix`: minimal desktop matrix with Safari included because URL and history handling often diverge; explains when mobile-table coverage becomes necessary.
- `7) Bug Report Template`: can provide a filled example for back-button state loss.

## 4) File upload with progress + cancel + retry (web)

### Prompt

Create an exploratory charter pack as UI Testing Expert.

Context:
- Platform: web
- Feature: document upload supports drag and drop, progress bar, cancel, retry, and server-side virus-scan pending state
- Risks:
  - users abandon if progress stalls
  - duplicate uploads after retry
  - poor diagnostics when scanning fails
- Timebox: 90 minutes

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` emphasizes successful upload, cancel mid-stream, retry after network failure, pending-scan transition, and duplicate-upload prevention.
- `3) Coverage Map`: includes uploading, stalled progress, cancel, retry, server rejection, and partial failure during post-upload scan.
- `4) Test Cases`: contains explicit exploratory charters inside P0 and P1, with data variants such as large files, unsupported types, duplicate names, and network interruption.
- `5) Automation Plan`: proposes a thin E2E happy path plus targeted lower-level tests for upload state machine logic; recommends stable selectors on drop zone, file rows, progress indicator, cancel action, retry action, and scan-status badge.
- `6) Matrix`: starts small but explains when touch-device and Safari coverage become necessary for file inputs and drag-drop differences.
- `7) Bug Report Template`: includes a filled example for progress stalling or retry duplication.

## 5) Mobile app onboarding flow change (mobile)

### Prompt

Create a test plan as UI Testing Expert.

Context:
- Platform: mobile
- App type: native mobile app
- Flow: onboarding now combines permissions education, account creation, and profile setup into one wizard
- Supported devices: iPhone 15, iPhone SE, Pixel 8
- Risks:
  - permission denial loops
  - keyboard covering form fields
  - progress loss on app background or resume

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `2) Assumptions`: states mobile-native assumptions explicitly and does not default back to web.
- `1) Executive Summary`: `Test first` covers first-run happy path, denial and later-enable permissions, app background or resume recovery, keyboard overlap, and analytics-safe completion.
- `3) Coverage Map`: includes first install, resume, denied permission, network loss during account creation, and partial completion recovery.
- `4) Test Cases`: P0 covers core completion and state persistence; P1 covers accessibility and orientation or device differences; P2 covers lower-risk polish.
- `5) Automation Plan`: recommends platform-appropriate tooling, stable accessibility IDs or test IDs for wizard steps and controls, and anti-flake guidance for device permissions, app lifecycle, and seeded accounts.
- `6) Matrix`: includes the provided device set and explains when to add tablet, older OS, or low-end Android coverage.
- `7) Bug Report Template`: includes a filled example for progress loss on background or resume.

## 6) Accessibility complaint: keyboard trap on modal

### Prompt

Triage this issue as UI Testing Expert.

Context:
- Platform: web
- Report: users cannot tab out of a settings modal after opening the help tooltip inside it
- Components involved: modal, tooltip, close button, save button, background page with focusable links
- Browser reports: Chrome and Safari
- Severity: blocks keyboard-only users from saving or cancelling

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` focuses on open modal, open tooltip, tab and shift-tab loop, Escape behavior, and focus return to trigger on close.
- `3) Coverage Map`: includes modal open, tooltip open, nested focusables, close actions, and screen-reader announcement basics.
- `4) Test Cases`: P0 centers on keyboard and focus correctness; P1 adds screen-reader basics and browser variance; P2 adds broader responsive checks if relevant.
- `5) Automation Plan`: recommends focused keyboard E2E coverage plus component tests around focus trap logic; prefers selectors or accessibility IDs on modal root, tooltip trigger, tooltip content, close action, and save action.
- `6) Matrix`: includes Chrome and Safari because the complaint already spans both.
- `7) Bug Report Template`: provides a filled defect report with exact repro and diagnostics to capture, such as focus order logs or accessibility-tree snapshots.

## 7) Flaky test: intermittent timeout waiting for element

### Prompt

Triage this flaky test as UI Testing Expert.

Context:
- Tool: Playwright
- Failure: `Timeout 5000ms exceeded while waiting for locator('[data-testid=\"save-success\"]')`
- Retries: passes on retry about 70% of the time
- Recent changes: save action now triggers an optimistic update, then refetches server data
- Artifacts: screenshot shows spinner still visible; trace shows two network requests

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` concentrates on save completion contract, spinner dismissal, optimistic-state handling, duplicate network sequencing, and deterministic data setup.
- `3) Coverage Map`: includes optimistic pending state, slow refetch, server error, and stale success banner timing.
- `4) Test Cases`: P0 includes isolating experiments to distinguish UI-ready versus network-ready conditions.
- `5) Automation Plan`: names root-cause categories such as unstable waits, hidden race between optimistic UI and refetch, and uncontrolled data; recommends waiting on a user-visible stable state or network contract instead of raw timeouts; keeps `data-testid` but suggests anchoring readiness to the container and spinner disappearance, not only the transient success toast.
- `6) Matrix`: can stay minimal because this is a stability issue, not a browser matrix problem, but should say when browser expansion is warranted.
- `7) Bug Report Template`: provides a filled flaky-test bug report including repro frequency, trace evidence, and telemetry suggestions.

## 8) Cross-browser bug: works on Chrome, broken on Safari

### Prompt

Triage this browser-specific issue as UI Testing Expert.

Context:
- Platform: web
- Feature: subscription settings form
- Symptom on Safari: save button stays disabled after selecting a billing date and editing a text field
- Chrome behavior: button enables immediately
- Components: native date input, text input, sticky footer with save button
- Impact: users on Safari cannot submit changes

### Expected Good Response Outline

- Use the exact 8-section format in order.
- `1) Executive Summary`: `Test first` focuses on Safari repro, field-change events, date-input behavior, sticky-footer interaction, and keyboard submission fallback.
- `3) Coverage Map`: includes initial load, field edits, date selection, validation state, and error messaging if submit remains blocked.
- `4) Test Cases`: P0 targets Safari-specific correctness and submit availability; P1 adds Firefox and responsive checks to bound the blast radius; P2 covers lower-risk polish.
- `5) Automation Plan`: recommends a minimal Safari or WebKit regression plus lower-level tests for form-dirty-state logic; prefers stable selectors on the form root, billing-date input, edited text field, validation summary, and save button; avoids browser-specific CSS selectors as a testing crutch.
- `6) Matrix`: starts with Safari plus one Chromium browser, then explains when to expand to Firefox or mobile Safari.
- `7) Bug Report Template`: provides a filled Safari defect report with expected versus actual and diagnostics such as event logs, value-change traces, and screenshots.
