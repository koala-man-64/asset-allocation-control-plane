from __future__ import annotations

from core import bronze_bucketing as owner_bronze_bucketing
from core import layer_bucketing as owner_layer_bucketing
from tasks.common import bronze_bucketing as legacy_bronze_bucketing
from tasks.common import layer_bucketing as legacy_layer_bucketing


def test_legacy_bronze_bucketing_wrapper_exposes_core_behavior() -> None:
    assert legacy_bronze_bucketing.ALPHABET_BUCKETS == owner_bronze_bucketing.ALPHABET_BUCKETS
    assert legacy_bronze_bucketing.bucket_letter("AAPL") == owner_bronze_bucketing.bucket_letter("AAPL")
    assert legacy_bronze_bucketing.bucket_blob_path("market-data", "A") == owner_bronze_bucketing.bucket_blob_path(
        "market-data",
        "A",
    )


def test_legacy_layer_bucketing_wrapper_exposes_core_behavior() -> None:
    assert legacy_layer_bucketing.ALPHABET_BUCKETS == owner_layer_bucketing.ALPHABET_BUCKETS
    assert legacy_layer_bucketing.silver_bucket_path(domain="finance", bucket="A", finance_sub_domain="valuation") == (
        owner_layer_bucketing.silver_bucket_path(domain="finance", bucket="A", finance_sub_domain="valuation")
    )
    assert legacy_layer_bucketing.gold_bucket_path(domain="market", bucket="B") == owner_layer_bucketing.gold_bucket_path(
        domain="market",
        bucket="B",
    )
