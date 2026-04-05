from __future__ import annotations

from monitoring.log_analytics import (
    extract_primary_scalar,
    parse_log_analytics_queries_json,
    render_query,
)


def test_parse_log_analytics_queries_json_accepts_array() -> None:
    specs = parse_log_analytics_queries_json(
        '[{"resourceType":"Microsoft.App/containerApps","name":"errors_15m","query":"X","warnAbove":1,"errorAbove":10,"unit":"count"}]'
    )
    assert len(specs) == 1
    assert specs[0].resource_type == "Microsoft.App/containerApps"
    assert specs[0].name == "errors_15m"
    assert specs[0].warn_above == 1.0
    assert specs[0].error_above == 10.0


def test_render_query_escapes_single_quotes() -> None:
    rendered = render_query("where app == '{resourceName}'", resource_name="my'app", resource_id=None)
    assert "my''app" in rendered


def test_extract_primary_scalar_handles_missing_rows() -> None:
    assert extract_primary_scalar({"tables": [{"rows": []}]}) == 0.0


def test_extract_primary_scalar_reads_first_cell() -> None:
    assert extract_primary_scalar({"tables": [{"rows": [[12]]}]}) == 12.0

