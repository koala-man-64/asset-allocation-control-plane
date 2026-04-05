from __future__ import annotations

import json
import sys
from pathlib import Path


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_sarif(payload: dict) -> dict:
    rules: dict[str, dict] = {}
    results: list[dict] = []

    for dependency in payload.get("dependencies", []):
        name = dependency.get("name", "unknown")
        version = dependency.get("version", "unknown")
        for vuln in dependency.get("vulns", []):
            rule_id = vuln.get("id") or (vuln.get("aliases") or ["pip-audit"])[0]
            fix_versions = ", ".join(vuln.get("fix_versions") or []) or "none published"
            description = vuln.get("description") or "No description provided by pip-audit."
            rules.setdefault(
                rule_id,
                {
                    "id": rule_id,
                    "name": rule_id,
                    "shortDescription": {"text": rule_id},
                    "fullDescription": {"text": description},
                    "help": {"text": f"Dependency {name} {version}; fixes: {fix_versions}."},
                    "defaultConfiguration": {"level": "error"},
                },
            )
            results.append(
                {
                    "ruleId": rule_id,
                    "level": "error",
                    "message": {
                        "text": f"{name} {version} is affected by {rule_id}. Available fixes: {fix_versions}.",
                    },
                    "locations": [
                        {
                            "physicalLocation": {
                                "artifactLocation": {"uri": "requirements.lock.txt"},
                            }
                        }
                    ],
                }
            )

    return {
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "pip-audit",
                        "informationUri": "https://github.com/pypa/pip-audit",
                        "rules": list(rules.values()),
                    }
                },
                "results": results,
            }
        ],
    }


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: pip_audit_to_sarif.py <pip-audit.json> <sarif.json>", file=sys.stderr)
        return 2

    source = Path(sys.argv[1])
    destination = Path(sys.argv[2])
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(_build_sarif(_load(source)), indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
