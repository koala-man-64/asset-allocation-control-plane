---
name: strict-branch-and-merge-discipline
description: Mandatory git safety workflow for any task that changes code in one or more repositories. Use before editing files, creating branches or worktrees, committing, rebasing, pushing, syncing with base, opening PRs, or coordinating multi-repo changes when branch isolation, collision avoidance, ownership boundaries, and safe merge discipline matter.
---

# Strict Branch and Merge Discipline

## Enforce the Policy

- Apply this skill before the first file edit for any code-changing task.
- Treat this skill as mandatory even when the user does not mention it.
- Prefer isolation over convenience.
- Prefer blocking over guessing.
- Stop on any policy violation and return `BLOCKED` with the exact reason and required remediation.
- Refuse to continue silently after a violation, overlap warning, ownership ambiguity, or stale-branch risk.

## Require or Infer Inputs

- Require `AGENT_ID`.
- Require `TASK_ID`.
- Infer `REPO_SLUG` from the repository directory name or the `origin` remote URL.
- Default `BASE_BRANCH` to the remote default branch from `origin/HEAD` unless explicitly provided.
- Accept `LINKED_REPOS` for multi-repo work.
- Use the repo merge policy when known. Otherwise default `MERGE_STRATEGY` to squash merge through PR.

## Treat These Branches as Protected or Shared

- Treat `main`, `master`, `trunk`, `develop`, `staging`, `production`, `release/*`, `hotfix/*`, and the remote default branch as protected.
- Add any repo-specific protected branches, release branches, integration branches, or merge-queue branches.
- Refuse direct edits, commits, rebases, pushes, local merges, or history rewrites on protected or shared branches.

## Run Preflight Before Any File Edit

1. Confirm the repo root, current branch, current worktree, and `origin` remote.
2. Run `git status --porcelain`.
3. Stop if the working tree is dirty unless the uncommitted changes clearly belong to the same `AGENT_ID` and `TASK_ID` branch you are resuming.
4. Resolve `BASE_BRANCH` from `origin/HEAD` unless explicitly provided.
5. Run `git fetch --all --prune`.
6. Record the exact starting base SHA from `origin/<BASE_BRANCH>`.
7. Discover active `agent/*` branches locally and remotely when visible.
8. Detect whether the same repo is already being worked on in this workspace. Require a new worktree instead of switching branches in place when another active task exists.
9. Create or reuse the task branch from the latest remote base, never from a stale local branch:
   - Use `agent/<AGENT_ID>/<TASK_ID>/<REPO_SLUG>`.
   - Reuse the branch only when it clearly belongs to the same ongoing task.
   - Add a unique suffix such as `/v2` or `/YYYYMMDD-HHMMSS` when the original name already exists for different work.
10. Refuse to continue if the only available path would reuse another agent's branch or a shared integration branch.

## Use One Active Branch per Working Directory

- Keep exactly one active branch per working directory.
- Create a separate git worktree for each concurrent branch in the same repo.
- Avoid branch switching in place when it would mix task state or untracked files across tasks.
- Record whether the workspace is isolated by branch only or by branch plus worktree.

## Check for Collisions Before Editing

- Inspect visible remote and local `agent/*` branches in the same repo.
- Compare planned touched files or directories against other active agent branches when feasible.
- Check `CODEOWNERS` or equivalent ownership files when present.
- Stop and return `BLOCKED` if file overlap, directory overlap, or ownership ambiguity is likely.
- Refuse to guess about ownership boundaries or to improvise conflict resolution on unfamiliar code.

## Follow These Rules While Working

- Keep commits small, atomic, and task-scoped.
- Include `TASK_ID` in every commit message.
- Avoid scope creep. Create a new task branch or explicitly record a scope expansion when the task changes materially.
- Avoid mixing unrelated fixes or features in the same branch.
- Never use plain `git pull`.
- Use `git fetch --all --prune` plus explicit `rebase` as the default sync path.
- Use `git pull --ff-only` only when it is clearly safe on the current agent's own branch and cannot create or hide merge state.
- Never push to, commit on, rebase, amend, cherry-pick into, or force-update another agent's branch.
- Never use `git push --force`. Use `git push --force-with-lease` only on the current agent's own unmerged private branch after a rebase.
- Rebase onto the latest `origin/<BASE_BRANCH>` before every push if the base moved.
- Rerun relevant tests, lint, and checks after the final sync and before pushing.
- Stop if conflict resolution would require dropping, overwriting, or guessing about unfamiliar changes.
- Stop if a conflict touches code outside the task's clear ownership.

## Coordinate Multi-Repo Work Explicitly

- Create one private branch per repo using the same `TASK_ID`.
- Keep a coordination record of `repo -> branch -> head SHA`.
- Link related branches or PRs clearly when repos must land together.
- Avoid calling a downstream repo complete when it depends on unmerged changes in an upstream repo unless the dependency is explicitly handled.
- Refuse to claim the overall task complete until all required repos are merged or explicitly called out as pending.

## Enforce the Merge Gate

Declare `MERGE-READY` only when all conditions are true:

1. The branch is private and owned by the current `AGENT_ID`.
2. The latest `origin/<BASE_BRANCH>` has been fetched.
3. The branch has been rebased onto the latest base or updated using the repo's approved sync method.
4. Relevant tests, lint, and checks have passed after the final sync.
5. No unresolved conflicts remain.
6. The PR targets the correct `BASE_BRANCH`.
7. Any linked multi-repo branches or PRs are referenced.
8. The merge method matches repo policy. Default to squash merge through PR when policy is unknown.
9. Branch protection and required reviews or checks are satisfied.
10. The merge queue is used when the repo requires it.

## Forbid These Actions

- Direct commits to protected or shared branches
- Direct pushes to protected or shared branches
- Plain `git pull` that can create accidental merge commits
- Force push without `--force-with-lease`
- Rebasing or rewriting another agent's branch
- Reusing a shared integration branch for task work
- Merging a stale branch
- Bundling unrelated changes into one branch or PR
- Deleting another agent's branch
- Silent conflict resolution on unfamiliar code

## Return a Status Block Every Time

Return this exact operational summary each time the skill runs or updates status:

```text
Repo name: <repo>
BASE_BRANCH: <base-branch>
Task branch: <agent/<AGENT_ID>/<TASK_ID>/<REPO_SLUG>>
Starting base SHA: <origin/<BASE_BRANCH> sha captured at preflight>
Current HEAD SHA: <current HEAD sha>
Workspace isolation: <branch only | branch + worktree | BLOCKED>
Status: <BLOCKED | IN PROGRESS | MERGE-READY>
Block reasons: <none or exact reasons>
Next safe action: <single explicit next step>
Coordination record:
- <repo> -> <branch> -> <head sha>
```

## Prefer Safe Command Patterns

- Use `git fetch --all --prune` instead of `git pull`.
- Use explicit `git rebase origin/<BASE_BRANCH>` on the current agent's private branch when syncing.
- Use `git worktree add ...` for parallel same-repo work.
- Use `git push --force-with-lease` only after rebasing the current agent's own private branch and only when a non-fast-forward update is unavoidable.
- Use the repo's PR, branch protection, and merge queue flow for final integration.
