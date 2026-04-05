# System Status Freshness Contract

- The system status page reads a single authoritative payload from `GET /api/system/status-view`.
- `systemHealth` in that payload is live-refreshed for the page query cadence.
- Medallion-domain job runtime state and status inside that payload are overlaid from a live Azure job execution/resource check on every status-view request, using the same anchored-active-execution behavior as the console stream.
- `metadataSnapshot` in that payload is served from the persisted snapshot document, not from a bulk live metadata scan.
- Metadata-changing artifact publishers must write through the shared domain artifact publish path so the persisted snapshot documents are refreshed on every successful publish.
- Manual job triggers still use client-side optimistic overrides until the backend catches up and the status view reflects the run.
- Metadata-changing actions emit `DOMAIN_METADATA_SNAPSHOT_CHANGED`, which invalidates the status view and legacy snapshot queries.
