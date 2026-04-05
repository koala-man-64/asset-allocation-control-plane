# Forensic Debugger Reference

Use these commands/checks when the skill is invoked.

## Kubernetes / Container

- `kubectl get pods -A` to confirm rollout health and restart patterns.
- `kubectl describe pod <pod> -n <ns>` for events, image, resources, mount, and scheduling signals.
- `kubectl get events -A --sort-by=.metadata.creationTimestamp | tail -n 200` for recent control-plane signals.
- `kubectl top nodes` and `kubectl top pods -A` to check CPU/memory saturation.
- `kubectl logs <pod> -n <ns> --since=15m --tail=400` for incident window logs.
- `kubectl get ingress,svc,netpol -n <ns>` for network exposure and policy boundaries.

## CI / GitHub Actions

- Pull workflow run and step logs around failed job.
- Compare recent workflow config changes (`.github/workflows/*.yml`) against last passing revision.
- Confirm secrets, OIDC roles, runner image/version, and dependency cache behavior.
- Check artifact retention and publish/download failures around the same timestamp.

## Networking, TLS, DNS

- `kubectl -n <ns> get endpoints <service>` for endpoint drift.
- `dig <host> @<dns-server>` and `nslookup <host>` from the failing node/pod context.
- `openssl s_client -connect <host>:443 -servername <host> </dev/null` for cert chain/SNI/expiry signals.
- Compare cert issuance/rotation timestamps with system timezone and clock skew.

## Datastores

- Capture active locks and waiters (engine-specific): e.g. PostgreSQL `SELECT * FROM pg_locks;`, MySQL `SHOW ENGINE INNODB STATUS\G`.
- Check open connection and pool utilization metrics.
- Validate schema/index drift between deploys and migrations.
- Inspect SSL/certificate settings and `ssl_version`/`sslmode` mismatches.

## Queue / Messaging

- Inspect queue depth, lag, dead-letter counts, and poison-pill growth.
- Verify retry backoff and max-retry policy alignment across producers/consumers.
- Validate idempotency keys and duplicate-delivery handling.
- Compare consumer lag to target processing throughput.

## Structured Remediation Output Requirements

- Confirmed root cause must include: trigger -> failing mechanism -> why it propagated.
- Include confidence per hypothesis (High/Medium/Low).
- Always include immediate, short-term, and long-term remediations.
- Always add systemic risk section for shared dependencies and recurrence pathways.
