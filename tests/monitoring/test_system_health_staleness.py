from __future__ import annotations

from datetime import datetime, timedelta, timezone

from monitoring import system_health


class _DummyStore:
    def __init__(self, *, marker_last_modified: datetime | None = None, marker_error: Exception | None = None):
        self._marker_last_modified = marker_last_modified
        self._marker_error = marker_error

    def get_blob_last_modified(self, *, container: str, blob_name: str) -> datetime | None:
        del container
        del blob_name
        if self._marker_error is not None:
            raise self._marker_error
        return self._marker_last_modified


def _marker_cfg(
    *,
    enabled: bool = True,
) -> system_health.MarkerProbeConfig:
    return system_health.MarkerProbeConfig(
        enabled=enabled,
        container="common",
        prefix="system/health_markers",
    )


def test_compute_layer_status_boundary_conditions() -> None:
    now = datetime(2026, 2, 16, 12, 0, tzinfo=timezone.utc)
    at_boundary = now - timedelta(seconds=60)

    assert (
        system_health._compute_layer_status(
            now,
            at_boundary,
            max_age_seconds=60,
            had_error=False,
        )
        == "healthy"
    )
    assert (
        system_health._compute_layer_status(
            now,
            at_boundary,
            max_age_seconds=59,
            had_error=False,
        )
        == "stale"
    )
    assert (
        system_health._compute_layer_status(
            now,
            None,
            max_age_seconds=60,
            had_error=False,
        )
        == "stale"
    )


def test_resolve_freshness_policy_uses_domain_override() -> None:
    policy = system_health._resolve_freshness_policy(
        layer_name="Silver",
        domain_name="market",
        default_max_age_seconds=129600,
        overrides={"silver.market": {"maxAgeSeconds": 43200}},
    )
    assert policy.max_age_seconds == 43200
    assert policy.source == "override:silver.market"


def test_resolve_freshness_policy_falls_back_to_default() -> None:
    policy = system_health._resolve_freshness_policy(
        layer_name="Gold",
        domain_name="earnings",
        default_max_age_seconds=129600,
        overrides={},
    )
    assert policy.max_age_seconds == 129600
    assert policy.source == "default"


def test_marker_probe_uses_marker_timestamp_when_available() -> None:
    marker_time = datetime(2026, 2, 16, 10, 0, tzinfo=timezone.utc)
    store = _DummyStore(marker_last_modified=marker_time)

    resolved = system_health._resolve_last_updated_with_marker_probes(
        layer_name="Silver",
        domain_name="market",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=True),
    )

    assert resolved.status == "ok"
    assert resolved.source == "marker"
    assert resolved.last_updated == marker_time
    assert resolved.warnings == []


def test_marker_missing_returns_error() -> None:
    store = _DummyStore(marker_last_modified=None)

    resolved = system_health._resolve_last_updated_with_marker_probes(
        layer_name="Silver",
        domain_name="finance",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=True),
    )

    assert resolved.status == "error"
    assert "Marker missing" in str(resolved.error)


def test_probe_error_returns_error() -> None:
    store = _DummyStore(marker_error=RuntimeError("403 Forbidden"))

    resolved = system_health._resolve_last_updated_with_marker_probes(
        layer_name="Bronze",
        domain_name="earnings",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=True),
    )

    assert resolved.status == "error"
    assert "403 Forbidden" in str(resolved.error)


def test_marker_disabled_returns_error() -> None:
    store = _DummyStore(marker_last_modified=None)

    resolved = system_health._resolve_last_updated_with_marker_probes(
        layer_name="Bronze",
        domain_name="price-target",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=False),
    )

    assert resolved.status == "error"
    assert "Marker probes are not configured." in str(resolved.error)


def test_resolve_domain_schedule_uses_manual_trigger_metadata() -> None:
    cron, frequency = system_health._resolve_domain_schedule(
        job_name="silver-market-job",
        default_cron="30 14-23 * * *",
        job_schedule_metadata={
            "silver-market-job": system_health.JobScheduleMetadata(
                trigger_type="manual",
                cron_expression="",
            )
        },
    )
    assert cron == ""
    assert frequency == "Manual trigger"


def test_resolve_domain_schedule_defaults_to_manual_trigger_when_metadata_missing() -> None:
    cron, frequency = system_health._resolve_domain_schedule(
        job_name="silver-market-job",
        default_cron="",
        default_trigger_type="manual",
        job_schedule_metadata={},
    )
    assert cron == ""
    assert frequency == "Manual trigger"
