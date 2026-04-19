from __future__ import annotations

import pytest

from api.endpoints import system
from api.service.app import create_app
from monitoring.control_plane import ResourceHealthItem
from tests.api._client import get_test_client


def _metadata_payload(*, layer: str, domain: str) -> dict[str, object]:
    return {
        "layer": layer,
        "domain": domain,
        "container": f"{layer}-container",
        "type": "blob",
        "computedAt": "2026-02-20T00:00:00+00:00",
        "symbolCount": 101,
        "columnCount": 9,
        "totalBytes": 2048,
        "warnings": [],
    }


def test_write_and_read_cached_domain_metadata_snapshot_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(system, "_domain_metadata_cache_path", lambda: "metadata/domain-metadata.json")
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-02-20T12:34:56+00:00")
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: {"version": 1, "entries": {}})

    def _save(payload: dict[str, object], file_path: str) -> None:
        captured["payload"] = payload
        captured["file_path"] = file_path

    monkeypatch.setattr(system.mdc, "save_common_json_content", _save)

    cached_at = system._write_cached_domain_metadata_snapshot(
        "silver",
        "market",
        _metadata_payload(layer="silver", domain="market"),
    )

    assert cached_at == "2026-02-20T12:34:56+00:00"
    assert captured["file_path"] == "metadata/domain-metadata.json"

    persisted = captured["payload"]
    assert isinstance(persisted, dict)
    entries = persisted.get("entries")
    assert isinstance(entries, dict)
    entry = entries.get("silver/market")
    assert isinstance(entry, dict)
    assert entry.get("cachedAt") == cached_at

    history = entry.get("history")
    assert isinstance(history, list)
    assert history[-1].get("symbolCount") == 101
    assert history[-1].get("columnCount") == 9
    assert history[-1].get("totalBytes") == 2048

    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: persisted)
    cached_payload = system._read_cached_domain_metadata_snapshot("silver", "market")

    assert isinstance(cached_payload, dict)
    assert cached_payload["layer"] == "silver"
    assert cached_payload["domain"] == "market"
    assert cached_payload["cachedAt"] == cached_at
    assert cached_payload["cacheSource"] == "snapshot"
    assert cached_payload["symbolCount"] == 101
    assert cached_payload["columnCount"] == 9
    assert cached_payload["totalBytes"] == 2048


@pytest.mark.asyncio
async def test_domain_metadata_returns_cached_snapshot_when_refresh_is_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        system,
        "_read_cached_domain_metadata_snapshot",
        lambda layer, domain, force_refresh=False: {
            **_metadata_payload(layer=layer, domain=domain),
            "cachedAt": "2026-02-20T12:00:00+00:00",
            "cacheSource": "snapshot",
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=bronze&domain=market")

    assert response.status_code == 200
    payload = response.json()
    assert payload["layer"] == "bronze"
    assert payload["domain"] == "market"
    assert payload["cacheSource"] == "snapshot"
    assert payload["cachedAt"] == "2026-02-20T12:00:00+00:00"
    assert response.headers.get("X-Domain-Metadata-Source") == "snapshot"


@pytest.mark.asyncio
async def test_domain_metadata_accepts_government_signals_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        system,
        "_read_cached_domain_metadata_snapshot",
        lambda layer, domain, force_refresh=False: {
            **_metadata_payload(layer=layer, domain=domain),
            "cachedAt": "2026-02-20T12:00:00+00:00",
            "cacheSource": "snapshot",
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=gold&domain=government-signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["domain"] == "government-signals"


@pytest.mark.asyncio
async def test_domain_metadata_refresh_collects_live_metadata_and_persists_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    captured: dict[str, object] = {}

    def _collect(*, layer: str, domain: str, force_refresh: bool = False) -> dict[str, object]:
        captured["layer"] = layer
        captured["domain"] = domain
        captured["force_refresh"] = force_refresh
        return {
            **_metadata_payload(layer=layer, domain=domain),
        }

    def _write(
        *,
        layer: str,
        domain: str,
        metadata: dict[str, object],
        snapshot_path: str,
        ui_snapshot_path: str,
    ) -> dict[str, object]:
        captured["write_layer"] = layer
        captured["write_domain"] = domain
        captured["snapshot_path"] = snapshot_path
        captured["ui_snapshot_path"] = ui_snapshot_path
        captured["metadata"] = metadata
        return {
            **metadata,
            "cachedAt": "2026-02-20T13:00:00+00:00",
            "cacheSource": "snapshot",
        }

    invalidated: list[bool] = []

    monkeypatch.setattr(system, "collect_domain_metadata", _collect)
    monkeypatch.setattr(system, "_domain_metadata_cache_path", lambda: "metadata/domain-metadata.json")
    monkeypatch.setattr(system, "_domain_metadata_ui_cache_path", lambda: "metadata/ui-cache/domain.json")
    monkeypatch.setattr(
        system.domain_metadata_snapshots,
        "write_domain_metadata_snapshot_documents",
        _write,
    )
    monkeypatch.setattr(system, "_invalidate_domain_metadata_document_cache", lambda: invalidated.append(True))

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=gold&domain=finance&refresh=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["layer"] == "gold"
    assert payload["domain"] == "finance"
    assert payload["cacheSource"] == "live-refresh"
    assert payload["cachedAt"] == "2026-02-20T13:00:00+00:00"
    assert response.headers.get("X-Domain-Metadata-Source") == "live-refresh"

    assert captured["layer"] == "gold"
    assert captured["domain"] == "finance"
    assert captured["force_refresh"] is True
    assert captured["write_layer"] == "gold"
    assert captured["write_domain"] == "finance"
    assert captured["snapshot_path"] == "metadata/domain-metadata.json"
    assert captured["ui_snapshot_path"] == "metadata/ui-cache/domain.json"
    assert captured["metadata"] == _metadata_payload(layer="gold", domain="finance")
    assert invalidated == [True]


@pytest.mark.asyncio
async def test_system_status_view_returns_system_health_and_metadata_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-03-17T12:00:00+00:00")
    monkeypatch.setattr(
        system,
        "_resolve_system_health_payload",
        lambda request, refresh: (
            {
                "overall": "healthy",
                "dataLayers": [],
                "recentJobs": [],
                "alerts": [],
                "resources": [],
            },
            False,
            False,
        ),
    )
    monkeypatch.setattr(
        system,
        "_build_domain_metadata_snapshot_payload",
        lambda **kwargs: {
            "version": 1,
            "updatedAt": "2026-03-17T11:59:00+00:00",
            "entries": {
                "bronze/market": {
                    **_metadata_payload(layer="bronze", domain="market"),
                    "cacheSource": "snapshot",
                }
            },
            "warnings": [],
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/status-view")

    assert response.status_code == 200
    payload = response.json()
    assert payload["version"] == 1
    assert payload["generatedAt"] == "2026-03-17T12:00:00+00:00"
    assert payload["systemHealth"]["overall"] == "healthy"
    assert sorted(payload["metadataSnapshot"]["entries"].keys()) == ["bronze/market"]
    assert payload["sources"] == {
        "systemHealth": "live-refresh",
        "metadataSnapshot": "persisted-snapshot",
    }
    assert response.headers.get("X-System-Health-Cache") == "miss"
    assert response.headers.get("X-Domain-Metadata-Source") == "persisted-snapshot"


@pytest.mark.asyncio
async def test_system_status_view_refresh_bypasses_system_health_cache_and_domain_doc_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health_calls: list[bool] = []
    metadata_refresh_flags: list[bool] = []

    def _collect_system_health(*, include_resource_ids: bool = False) -> dict[str, object]:
        health_calls.append(include_resource_ids)
        return {
            "overall": "healthy",
            "dataLayers": [],
            "recentJobs": [],
            "alerts": [],
            "resources": [],
        }

    def _load_snapshot(force_refresh: bool = False) -> dict[str, object]:
        metadata_refresh_flags.append(force_refresh)
        return {
            "version": 1,
            "updatedAt": "2026-03-17T12:00:00+00:00",
            "entries": {},
        }

    monkeypatch.setattr(system, "collect_system_health_snapshot", _collect_system_health)
    monkeypatch.setattr(system, "_load_domain_metadata_document", _load_snapshot)
    monkeypatch.setattr(
        system,
        "collect_domain_metadata",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("live metadata collection is not expected")),
    )

    app = create_app()
    async with get_test_client(app) as client:
        first = await client.get("/api/system/status-view")
        second = await client.get("/api/system/status-view")
        refreshed = await client.get("/api/system/status-view?refresh=true")

    assert first.status_code == 200
    assert second.status_code == 200
    assert refreshed.status_code == 200
    assert len(health_calls) == 2
    assert metadata_refresh_flags == [False, False, True]


@pytest.mark.asyncio
async def test_system_status_view_overlays_live_domain_job_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "aca-job-market")
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-03-17T12:00:00+00:00")
    monkeypatch.setattr(
        system,
        "_resolve_system_health_payload",
        lambda request, refresh=False: (
            {
                "overall": "healthy",
                "dataLayers": [
                    {
                        "name": "Bronze",
                        "domains": [
                            {
                                "name": "market",
                                "jobName": "aca-job-market",
                            }
                        ],
                    }
                ],
                "recentJobs": [
                    {
                        "jobName": "aca-job-market",
                        "jobType": "data-ingest",
                        "status": "success",
                        "statusCode": "Succeeded",
                        "executionName": "aca-job-market-exec-old",
                        "startTime": "2026-03-17T11:45:00+00:00",
                        "duration": 120,
                        "triggeredBy": "azure",
                    }
                ],
                "alerts": [],
                "resources": [
                    {
                        "name": "aca-job-market",
                        "resourceType": "Microsoft.App/jobs",
                        "status": "healthy",
                        "lastChecked": "2026-03-17T11:45:00+00:00",
                        "runningState": "Stopped",
                        "lastModifiedAt": "2026-03-17T11:45:10+00:00",
                    }
                ],
            },
            True,
            False,
        ),
    )
    monkeypatch.setattr(
        system,
        "_build_domain_metadata_snapshot_payload",
        lambda **kwargs: {"version": 1, "updatedAt": None, "entries": {}, "warnings": []},
    )

    class _FakeArmClient:
        def __init__(self, _cfg) -> None:
            return None

        def __enter__(self) -> "_FakeArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr(system, "AzureArmClient", _FakeArmClient)
    monkeypatch.setattr(
        system,
        "collect_jobs_and_executions",
        lambda arm, **kwargs: (
            [
                ResourceHealthItem(
                    name="aca-job-market",
                    resource_type="Microsoft.App/jobs",
                    status="healthy",
                    last_checked="2026-03-17T12:00:00+00:00",
                    details="provisioningState=Succeeded, runningState=Running",
                    running_state="Running",
                    last_modified_at="2026-03-17T11:59:59+00:00",
                )
            ],
            [
                {
                    "jobName": "aca-job-market",
                    "jobType": "data-ingest",
                    "status": "running",
                    "statusCode": "Running",
                    "executionName": "aca-job-market-exec-live",
                    "executionId": "/jobs/aca-job-market/executions/aca-job-market-exec-live",
                    "startTime": "2026-03-17T11:58:00+00:00",
                    "endTime": None,
                    "triggeredBy": "azure",
                }
            ],
        ),
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/status-view")

    assert response.status_code == 200
    payload = response.json()
    job_resource = next(
        resource
        for resource in payload["systemHealth"]["resources"]
        if resource["name"] == "aca-job-market"
    )
    assert job_resource["runningState"] == "Running"
    assert job_resource["lastModifiedAt"] == "2026-03-17T11:59:59+00:00"
    assert payload["systemHealth"]["recentJobs"][0]["executionName"] == "aca-job-market-exec-live"
    assert payload["systemHealth"]["recentJobs"][0]["status"] == "running"
    assert payload["systemHealth"]["recentJobs"][1]["executionName"] == "aca-job-market-exec-old"


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_miss_returns_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(system, "_read_cached_domain_metadata_snapshot", lambda layer, domain, force_refresh=False: None)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=bronze&domain=market")

    assert response.status_code == 200
    payload = response.json()
    assert payload["layer"] == "bronze"
    assert payload["domain"] == "market"
    assert payload["cacheSource"] == "snapshot"
    assert payload["symbolCount"] is None
    assert payload["warnings"]
    assert "No cached domain metadata snapshot found" in payload["warnings"][0]
    assert response.headers.get("X-Domain-Metadata-Source") == "snapshot-miss"
    assert response.headers.get("X-Domain-Metadata-Cache-Miss") == "1"


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_returns_filtered_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        system,
        "_load_domain_metadata_document",
        lambda force_refresh=False: {
            "version": 1,
            "updatedAt": "2026-02-20T12:00:00+00:00",
            "entries": {
                "bronze/market": {
                    "layer": "bronze",
                    "domain": "market",
                    "cachedAt": "2026-02-20T11:59:00+00:00",
                    "metadata": _metadata_payload(layer="bronze", domain="market"),
                },
                "silver/finance": {
                    "layer": "silver",
                    "domain": "finance",
                    "cachedAt": "2026-02-20T11:58:00+00:00",
                    "metadata": _metadata_payload(layer="silver", domain="finance"),
                },
            },
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot?layers=bronze&domains=market,finance")

    assert response.status_code == 200
    payload = response.json()
    assert payload["version"] == 1
    assert payload["updatedAt"] == "2026-02-20T12:00:00+00:00"
    assert sorted(payload["entries"].keys()) == ["bronze/market"]
    entry = payload["entries"]["bronze/market"]
    assert entry["layer"] == "bronze"
    assert entry["domain"] == "market"
    assert entry["cacheSource"] == "snapshot"
    assert response.headers.get("X-Domain-Metadata-Source") == "snapshot-batch"
    assert response.headers.get("X-Domain-Metadata-Entry-Count") == "1"


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_never_live_fills_missing_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        system,
        "_load_domain_metadata_document",
        lambda force_refresh=False: {"version": 1, "updatedAt": None, "entries": {}},
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot?layers=bronze&domains=market")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entries"] == {}
    assert payload["warnings"] == []


@pytest.mark.asyncio
async def test_domain_metadata_rejects_removed_cache_only_param() -> None:
    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=bronze&domain=market&cacheOnly=true")

    assert response.status_code == 400
    assert "Unsupported query parameter(s): cacheOnly" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_rejects_removed_cache_only_param() -> None:
    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot?layers=bronze&cacheOnly=true")

    assert response.status_code == 400
    assert "Unsupported query parameter(s): cacheOnly" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_rejects_invalid_layer_filter(monkeypatch: pytest.MonkeyPatch) -> None:

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot?layers=invalid-layer")

    assert response.status_code == 400
    assert "layers contains unsupported value" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_returns_304_when_etag_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        system,
        "_load_domain_metadata_document",
        lambda force_refresh=False: {
            "version": 1,
            "updatedAt": "2026-02-20T12:00:00+00:00",
            "entries": {
                "bronze/market": {
                    "layer": "bronze",
                    "domain": "market",
                    "cachedAt": "2026-02-20T11:59:00+00:00",
                    "metadata": _metadata_payload(layer="bronze", domain="market"),
                }
            },
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        first = await client.get("/api/system/domain-metadata/snapshot?layers=bronze&domains=market")
        etag = first.headers.get("ETag")
        assert etag
        second = await client.get(
            "/api/system/domain-metadata/snapshot?layers=bronze&domains=market",
            headers={"If-None-Match": etag},
        )

    assert first.status_code == 200
    assert second.status_code == 304
    assert second.text == ""


@pytest.mark.asyncio
async def test_persisted_ui_domain_metadata_cache_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(system, "_domain_metadata_ui_cache_path", lambda: "metadata/ui-cache/domain.json")
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-02-20T14:00:00+00:00")
    monkeypatch.setattr(
        system.mdc,
        "save_common_json_content",
        lambda data, path: captured.update({"payload": data, "path": path}),
    )
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: captured.get("payload"))

    app = create_app()
    async with get_test_client(app) as client:
        write_response = await client.put(
            "/api/system/domain-metadata/snapshot/cache",
            json={
                "version": 1,
                "updatedAt": None,
                "entries": {
                    "bronze/market": {
                        **_metadata_payload(layer="bronze", domain="market"),
                        "cacheSource": "snapshot",
                    }
                },
                "warnings": [],
            },
        )
        read_response = await client.get("/api/system/domain-metadata/snapshot/cache")

    assert write_response.status_code == 200
    assert captured["path"] == "metadata/ui-cache/domain.json"
    written = write_response.json()
    assert written["updatedAt"] == "2026-02-20T14:00:00+00:00"
    assert read_response.status_code == 200
    assert sorted(read_response.json()["entries"].keys()) == ["bronze/market"]
    assert read_response.headers.get("X-Domain-Metadata-UI-Cache") == "hit"


def test_refresh_domain_metadata_snapshot_emits_realtime_change(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, list[dict[str, str]]]] = []

    monkeypatch.setattr(
        system,
        "collect_domain_metadata",
        lambda **kwargs: _metadata_payload(layer="gold", domain="finance"),
    )
    monkeypatch.setattr(system, "_domain_metadata_cache_path", lambda: "metadata/domain-metadata.json")
    monkeypatch.setattr(system, "_domain_metadata_ui_cache_path", lambda: "metadata/ui-cache/domain.json")
    monkeypatch.setattr(
        system.domain_metadata_snapshots,
        "write_domain_metadata_snapshot_documents",
        lambda **kwargs: {
            **_metadata_payload(layer="gold", domain="finance"),
            "cachedAt": "2026-03-17T12:05:00+00:00",
            "cacheSource": "snapshot",
        },
    )
    monkeypatch.setattr(system, "_invalidate_domain_metadata_document_cache", lambda: None)
    monkeypatch.setattr(
        system,
        "_emit_domain_metadata_snapshot_changed",
        lambda reason, targets: captured.append((reason, targets)),
    )

    payload = system._refresh_domain_metadata_snapshot("gold", "finance")

    assert payload["cacheSource"] == "live-refresh"
    assert captured == [("refresh", [{"layer": "gold", "domain": "finance"}])]


@pytest.mark.asyncio
async def test_put_domain_metadata_snapshot_cache_emits_realtime_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted: list[tuple[str, list[dict[str, str]]]] = []

    monkeypatch.setattr(system, "_domain_metadata_ui_cache_path", lambda: "metadata/ui-cache/domain.json")
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-03-17T14:00:00+00:00")
    monkeypatch.setattr(system.mdc, "save_common_json_content", lambda data, path: None)
    monkeypatch.setattr(
        system,
        "_emit_domain_metadata_snapshot_changed",
        lambda reason, targets: emitted.append((reason, targets)),
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.put(
            "/api/system/domain-metadata/snapshot/cache",
            json={
                "version": 1,
                "updatedAt": None,
                "entries": {
                    "bronze/market": {
                        **_metadata_payload(layer="bronze", domain="market"),
                        "cacheSource": "snapshot",
                    }
                },
                "warnings": [],
            },
        )

    assert response.status_code == 200
    assert emitted == [("ui-cache-write", [{"layer": "bronze", "domain": "market"}])]


def test_mark_purged_domain_metadata_snapshots_emits_realtime_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marked: list[tuple[str, str, str | None]] = []
    emitted: list[tuple[str, list[dict[str, str]]]] = []

    monkeypatch.setattr(
        system.domain_metadata_snapshots,
        "mark_domain_metadata_snapshot_purged",
        lambda layer, domain, container=None: marked.append((layer, domain, container)),
    )
    monkeypatch.setattr(system, "_invalidate_domain_metadata_document_cache", lambda: None)
    monkeypatch.setattr(
        system,
        "_emit_domain_metadata_snapshot_changed",
        lambda reason, targets: emitted.append((reason, targets)),
    )

    system._mark_purged_domain_metadata_snapshots(
        [{"layer": "gold", "domain": "market", "container": "gold-container"}]
    )

    assert marked == [("gold", "market", "gold-container")]
    assert emitted == [("purge", [{"layer": "gold", "domain": "market", "container": "gold-container"}])]


@pytest.mark.asyncio
async def test_persisted_ui_domain_metadata_cache_miss_returns_empty_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: None)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot/cache")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entries"] == {}
    assert response.headers.get("X-Domain-Metadata-UI-Cache") == "miss"
