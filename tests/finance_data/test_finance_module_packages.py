from tasks.finance_data import bronze_finance_data as bronze
from tasks.finance_data import gold_finance_data as gold
from tasks.finance_data import silver_finance_data as silver
from tasks.finance_data import silver_frames
from tasks.finance_data import silver_parsing
from tasks.finance_data.bronze_modules import assembly as bronze_assembly
from tasks.finance_data.bronze_modules import coverage as bronze_coverage
from tasks.finance_data.bronze_modules import provider as bronze_provider
from tasks.finance_data.bronze_modules import publication as bronze_publication
from tasks.finance_data.gold_modules import features as gold_features
from tasks.finance_data.gold_modules import reconciliation as gold_reconciliation
from tasks.finance_data.gold_modules import schema as gold_schema
from tasks.finance_data.gold_modules import sync as gold_sync
from tasks.finance_data.gold_modules import watermarks as gold_watermarks
from tasks.finance_data.silver_modules import discovery as silver_discovery
from tasks.finance_data.silver_modules import frames as silver_module_frames
from tasks.finance_data.silver_modules import indexing as silver_indexing
from tasks.finance_data.silver_modules import parsing as silver_module_parsing
from tasks.finance_data.silver_modules import reconciliation as silver_reconciliation
from tasks.finance_data.silver_modules import writes as silver_writes


def test_silver_internal_modules_expose_current_owners() -> None:
    assert silver_parsing._read_finance_json is silver_module_parsing._read_finance_json
    assert silver_frames._prepare_finance_delta_write_frame is silver_module_frames._prepare_finance_delta_write_frame
    assert silver_discovery._list_alpha26_finance_bucket_candidates is silver._list_alpha26_finance_bucket_candidates
    assert silver_indexing._resolve_existing_finance_symbol_maps is silver._resolve_existing_finance_symbol_maps
    assert silver_writes._write_alpha26_finance_silver_buckets is silver._write_alpha26_finance_silver_buckets
    assert silver_reconciliation._run_finance_reconciliation is silver._run_finance_reconciliation


def test_bronze_module_packages_track_top_level_finance_helpers() -> None:
    assert bronze_coverage._empty_coverage_summary is bronze._empty_coverage_summary
    assert bronze_provider.fetch_and_save_raw is bronze.fetch_and_save_raw
    assert bronze_assembly._build_finance_bucket_row is bronze._build_finance_bucket_row
    assert bronze_publication._write_alpha26_finance_buckets is bronze._write_alpha26_finance_buckets


def test_gold_module_packages_track_top_level_finance_helpers() -> None:
    assert gold_features.compute_features is gold.compute_features
    assert gold_schema._project_gold_finance_piotroski_frame is gold._project_gold_finance_piotroski_frame
    assert gold_watermarks._build_job_config is gold._build_job_config
    assert gold_sync._load_existing_gold_finance_symbol_to_bucket_map is gold._load_existing_gold_finance_symbol_to_bucket_map
    assert gold_reconciliation._run_finance_reconciliation is gold._run_finance_reconciliation
