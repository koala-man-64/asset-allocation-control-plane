# Code Drift Sentinel Acceptance Scenarios

Run from repository root:

```bash
python3 .codex/skills/code-drift-sentinel/scripts/run_scenarios.py --list
python3 .codex/skills/code-drift-sentinel/scripts/run_scenarios.py --validate
```

Use these scenarios as acceptance targets when testing `codedrift_sentinel.py` in a fixture repo.
Each scenario defines setup actions, expected drift categories/severity, and expected remediation direction.
