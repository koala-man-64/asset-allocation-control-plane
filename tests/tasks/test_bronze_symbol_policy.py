import json
from unittest.mock import MagicMock

from tasks.common import bronze_symbol_policy as policy


def _install_storage_stubs(monkeypatch):
    blobs: dict[str, bytes] = {}
    update_calls: list[tuple[str, str, object]] = []

    def fake_read_raw_bytes(path, client=None, **kwargs):
        del client
        del kwargs
        if path not in blobs:
            raise FileNotFoundError(path)
        return blobs[path]

    def fake_store_raw_bytes(raw, path, client=None):
        del client
        blobs[path] = raw

    def fake_update_csv_set(path, symbol, client=None):
        update_calls.append((path, symbol, client))

    monkeypatch.setattr(policy.mdc, "read_raw_bytes", fake_read_raw_bytes)
    monkeypatch.setattr(policy.mdc, "store_raw_bytes", fake_store_raw_bytes)
    monkeypatch.setattr(policy.mdc, "update_csv_set", fake_update_csv_set)
    return blobs, update_calls


def test_record_invalid_symbol_candidate_creates_marker_without_blacklist_on_first_run(monkeypatch):
    blobs, update_calls = _install_storage_stubs(monkeypatch)

    result = policy.record_invalid_symbol_candidate(
        common_client=object(),
        bronze_client=object(),
        domain="finance",
        symbol="aapl",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )

    assert result["promoted"] is False
    assert result["observedRunCount"] == 1
    assert update_calls == []

    marker_path = policy.invalid_candidate_marker_path(domain="finance", symbol="AAPL")
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["status"] == "candidate"
    assert stored["observedRunCount"] == 1
    assert stored["symbol"] == "AAPL"
    assert stored["blacklistPath"] is None


def test_record_invalid_symbol_candidate_dedupes_same_run_and_promotes_on_second_run(monkeypatch):
    blobs, update_calls = _install_storage_stubs(monkeypatch)
    common_client = object()
    bronze_client = object()

    first = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )
    duplicate = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )
    promoted = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-2",
    )

    assert first["promoted"] is False
    assert duplicate["promoted"] is False
    assert duplicate["observedRunCount"] == 1
    assert promoted["promoted"] is True
    assert promoted["observedRunCount"] == 2
    assert update_calls == [("market-data/blacklist.csv", "MSFT", bronze_client)]

    marker_path = policy.invalid_candidate_marker_path(domain="market", symbol="MSFT")
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["status"] == "promoted"
    assert stored["observedRunCount"] == 2
    assert stored["blacklistPath"] == "market-data/blacklist.csv"
    assert stored["promotedAt"]


def test_clear_invalid_candidate_marker_deletes_only_non_promoted_marker(monkeypatch):
    blobs, _ = _install_storage_stubs(monkeypatch)
    common_client = MagicMock()
    candidate_path = policy.invalid_candidate_marker_path(domain="earnings", symbol="AAPL")
    promoted_path = policy.invalid_candidate_marker_path(domain="finance", symbol="AAPL")
    blobs[candidate_path] = json.dumps(
        {
            "status": "candidate",
            "observedRunCount": 1,
        }
    ).encode("utf-8")
    blobs[promoted_path] = json.dumps(
        {
            "status": "promoted",
            "observedRunCount": 2,
        }
    ).encode("utf-8")

    assert policy.clear_invalid_candidate_marker(common_client=common_client, domain="earnings", symbol="AAPL") is True
    common_client.delete_file.assert_called_once_with(candidate_path)

    common_client.delete_file.reset_mock()
    assert policy.clear_invalid_candidate_marker(common_client=common_client, domain="finance", symbol="AAPL") is False
    common_client.delete_file.assert_not_called()


def test_record_invalid_symbol_candidate_resets_count_when_reason_changes(monkeypatch):
    blobs, update_calls = _install_storage_stubs(monkeypatch)
    common_client = object()
    bronze_client = object()

    first_reason = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )
    second_reason_first_run = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_no_market_history",
        run_id="run-2",
    )
    second_reason_promoted = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_no_market_history",
        run_id="run-3",
    )

    assert first_reason["promoted"] is False
    assert first_reason["observedRunCount"] == 1
    assert second_reason_first_run["promoted"] is False
    assert second_reason_first_run["observedRunCount"] == 1
    assert second_reason_promoted["promoted"] is True
    assert second_reason_promoted["observedRunCount"] == 2
    assert update_calls == [("market-data/blacklist.csv", "MSFT", bronze_client)]

    marker_path = policy.invalid_candidate_marker_path(domain="market", symbol="MSFT")
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["reasonCode"] == "provider_no_market_history"
