# Gold Publication Status

## Summary

All gold jobs now follow the same publication contract:

- Healthy bucket completion persists the gold symbol index and the bucket watermark immediately.
- Root `domain.json` is published once, at end-of-run only.
- Final `artifact_publication_status` logs report split failure counters and a normalized `failure_mode`.

This applies to:

- `market`
- `finance`
- `earnings`
- `price-target`

## Final Log Fields

The final `artifact_publication_status` log now includes:

- `reason`
- `failure_mode`
- `failed`
- `failed_symbols`
- `failed_buckets`
- `failed_finalization`
- `processed`
- `skipped_unchanged`
- `skipped_missing_source`

Legacy fields remain for compatibility:

- `reason`
- `failed`
- `processed`

## Failure Modes

- `none`: the run finalized successfully and the root artifact was published.
- `symbol`: the run wrote at least one bucket, but one or more symbol-level compute paths failed.
- `bucket`: one or more buckets failed at a hard bucket stage such as write or checkpoint.
- `finalization`: the bucket work completed, but final publication failed during end-of-run checks or final index publication.
- `mixed`: more than one failure class occurred in the same run.

## Operational Interpretation

- Treat root `domain.json` as final domain state only. It should not appear mid-run anymore.
- During a live run, bucket artifacts plus the gold symbol index remain the interim source of truth.
- For root-cause analysis, use the final publication log to classify the failure, then inspect earlier per-bucket logs to find the specific failing symbol or bucket.

## Example

```text
artifact_publication_status layer=gold domain=market status=blocked reason=failed_buckets failure_mode=symbol failed=3 failed_symbols=3 failed_buckets=0 failed_finalization=0 processed=12 skipped_unchanged=5 skipped_missing_source=1
```

Interpretation:

- bucket writes were partially successful
- publication was blocked by symbol-level failures, not hard bucket failures
- the root artifact was intentionally withheld at finalization

## Rollback Note

This change does not require a storage migration.

Rollback is limited to:

- `tasks/common/gold_checkpoint_publication.py`
- the four gold job call sites
- test and doc updates

Reverting those files restores the prior publication timing and log contract.
