============================= test session starts =============================
platform win32 -- Python 3.13.12, pytest-9.0.2, pluggy-1.6.0 -- C:\Data\Projects\health-data\venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Data\Projects\health-data\test-suite
plugins: anyio-4.12.1
collecting ... collected 47 items

test_api.py::TestHealthEndpoint::test_health_returns_200 PASSED          [  2%]
test_api.py::TestHealthEndpoint::test_health_has_status PASSED           [  4%]
test_api.py::TestHealthEndpoint::test_health_has_provider_count PASSED   [  6%]
test_api.py::TestProviderLookup::test_valid_npi_returns_200 PASSED       [  8%]
test_api.py::TestProviderLookup::test_valid_npi_has_name FAILED          [ 10%]
test_api.py::TestProviderLookup::test_invalid_npi_returns_404 PASSED     [ 12%]
test_api.py::TestProviderLookup::test_non_numeric_npi_returns_error PASSED [ 14%]
test_api.py::TestProviderSearch::test_search_by_name_returns_results PASSED [ 17%]
test_api.py::TestProviderSearch::test_search_with_state_filter PASSED    [ 19%]
test_api.py::TestProviderSearch::test_search_empty_returns_empty PASSED  [ 21%]
test_api.py::TestProviderSearch::test_search_without_params PASSED       [ 23%]
test_api.py::TestStatsEndpoints::test_stats_returns_200 PASSED           [ 25%]
test_api.py::TestStatsEndpoints::test_stats_has_total PASSED             [ 27%]
test_api.py::TestStatsEndpoints::test_coverage_returns_200 PASSED        [ 29%]
test_api.py::TestStatsEndpoints::test_coverage_has_entries PASSED        [ 31%]
test_api.py::TestPaymentEndpoint::test_payments_returns_200 PASSED       [ 34%]
test_api.py::TestPaymentEndpoint::test_invalid_npi_payments_returns_404 PASSED [ 36%]
test_unified_table.py::TestSchema::test_has_required_columns PASSED      [ 38%]
test_unified_table.py::TestSchema::test_min_row_count PASSED             [ 40%]
test_unified_table.py::TestSchema::test_provider_id_unique PASSED        [ 42%]
test_unified_table.py::TestSchema::test_npi_unique_within_entity_type PASSED [ 44%]
test_unified_table.py::TestSchema::test_npi_not_null PASSED              [ 46%]
test_unified_table.py::TestSchema::test_provider_id_not_null PASSED      [ 48%]
test_unified_table.py::TestCoverageFlags::test_has_op_payments_is_bool PASSED [ 51%]
test_unified_table.py::TestCoverageFlags::test_has_pecos_enrollment_is_bool PASSED [ 53%]
test_unified_table.py::TestCoverageFlags::test_linkage_coverage_range PASSED [ 55%]
test_unified_table.py::TestCoverageFlags::test_linkage_coverage_matches_flags PASSED [ 57%]
test_unified_table.py::TestCoverageFlags::test_pecos_coverage_above_90_pct PASSED [ 59%]
test_unified_table.py::TestCoverageFlags::test_data_sources_not_null PASSED [ 61%]
test_unified_table.py::TestCoverageFlags::test_data_sources_starts_with_medicare PASSED [ 63%]
test_unified_table.py::TestNameReconciliation::test_reconciled_lastname_not_null PASSED [ 65%]
test_unified_table.py::TestNameReconciliation::test_reconciled_state_not_null PASSED [ 68%]
test_unified_table.py::TestNameReconciliation::test_reconciled_prefers_medicare PASSED [ 70%]
test_unified_table.py::TestNameReconciliation::test_orgs_have_no_first_name_in_med PASSED [ 72%]
test_unified_table.py::TestPayments::test_payments_provider_id_unique PASSED [ 74%]
test_unified_table.py::TestPayments::test_no_negative_payments PASSED    [ 76%]
test_unified_table.py::TestPayments::test_sum_payment_geq_max_payment PASSED [ 78%]
test_unified_table.py::TestPayments::test_n_payments_positive PASSED     [ 80%]
test_unified_table.py::TestPayments::test_date_range_valid PASSED        [ 82%]
test_unified_table.py::TestTransitiveChains::test_transitive_has_provider_id PASSED [ 85%]
test_unified_table.py::TestTransitiveChains::test_match_tier_valid PASSED [ 87%]
test_unified_table.py::TestTransitiveChains::test_full_chain_count_bounded PASSED [ 89%]
test_unified_table.py::TestTransitiveChains::test_linkage_path_populated PASSED [ 91%]
test_unified_table.py::TestConflicts::test_conflict_types_present PASSED [ 93%]
test_unified_table.py::TestConflicts::test_multi_match_under_100 PASSED  [ 95%]
test_unified_table.py::TestConflicts::test_name_mismatch_under_5_pct PASSED [ 97%]
test_unified_table.py::TestConflicts::test_counts_are_positive PASSED    [100%]

================================== FAILURES ===================================
_________________ TestProviderLookup.test_valid_npi_has_name __________________

self = <test_api.TestProviderLookup object at 0x000001D7FE921950>
client = <starlette.testclient.TestClient object at 0x000001D7980FE510>

    def test_valid_npi_has_name(self, client):
        data = client.get(f"/providers/{self.KNOWN_NPI}").json()
>       assert "lastname_reconciled" in data or "name" in data
E       AssertionError: assert ('lastname_reconciled' in {'avg_payment': 20.78, 'data_sources': 'Medicare+OP+PECOS', 'entity_type': 'I', 'first_med': 'ARDALAN', ...} or 'name' in {'avg_payment': 20.78, 'data_sources': 'Medicare+OP+PECOS', 'entity_type': 'I', 'first_med': 'ARDALAN', ...})

test_api.py:65: AssertionError
=========================== short test summary info ===========================
FAILED test_api.py::TestProviderLookup::test_valid_npi_has_name - AssertionEr...
======================== 1 failed, 46 passed in 10.10s ========================
============================= test session starts =============================
platform win32 -- Python 3.13.12, pytest-9.0.2, pluggy-1.6.0 -- C:\Data\Projects\health-data\venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Data\Projects\health-data\test-suite
plugins: anyio-4.12.1
collecting ... collected 47 items

test_api.py::TestHealthEndpoint::test_health_returns_200 PASSED          [  2%]
test_api.py::TestHealthEndpoint::test_health_has_status PASSED           [  4%]
test_api.py::TestHealthEndpoint::test_health_has_provider_count PASSED   [  6%]
test_api.py::TestProviderLookup::test_valid_npi_returns_200 PASSED       [  8%]
test_api.py::TestProviderLookup::test_valid_npi_has_name PASSED          [ 10%]
test_api.py::TestProviderLookup::test_invalid_npi_returns_404 PASSED     [ 12%]
test_api.py::TestProviderLookup::test_non_numeric_npi_returns_error PASSED [ 14%]
test_api.py::TestProviderSearch::test_search_by_name_returns_results PASSED [ 17%]
test_api.py::TestProviderSearch::test_search_with_state_filter PASSED    [ 19%]
test_api.py::TestProviderSearch::test_search_empty_returns_empty 

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! KeyboardInterrupt !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.13_3.13.3312.0_x64__qbz5n2kfra8p0\Lib\threading.py:359: KeyboardInterrupt
(to show a full traceback on KeyboardInterrupt use --full-trace)
============================== 9 passed in 6.74s ==============================
============================= test session starts =============================
platform win32 -- Python 3.13.12, pytest-9.0.2, pluggy-1.6.0 -- C:\Data\Projects\health-data\venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Data\Projects\health-data\test-suite
plugins: anyio-4.12.1
collecting ... collected 47 items

test_api.py::TestHealthEndpoint::test_health_returns_200 PASSED          [  2%]
test_api.py::TestHealthEndpoint::test_health_has_status PASSED           [  4%]
test_api.py::TestHealthEndpoint::test_health_has_provider_count PASSED   [  6%]
test_api.py::TestProviderLookup::test_valid_npi_returns_200 PASSED       [  8%]
test_api.py::TestProviderLookup::test_valid_npi_has_name PASSED          [ 10%]
test_api.py::TestProviderLookup::test_invalid_npi_returns_404 PASSED     [ 12%]
test_api.py::TestProviderLookup::test_non_numeric_npi_returns_error PASSED [ 14%]
test_api.py::TestProviderSearch::test_search_by_name_returns_results PASSED [ 17%]
test_api.py::TestProviderSearch::test_search_with_state_filter PASSED    [ 19%]
test_api.py::TestProviderSearch::test_search_empty_returns_empty PASSED  [ 21%]
test_api.py::TestProviderSearch::test_search_without_params PASSED       [ 23%]
test_api.py::TestStatsEndpoints::test_stats_returns_200 PASSED           [ 25%]
test_api.py::TestStatsEndpoints::test_stats_has_total PASSED             [ 27%]
test_api.py::TestStatsEndpoints::test_coverage_returns_200 PASSED        [ 29%]
test_api.py::TestStatsEndpoints::test_coverage_has_entries PASSED        [ 31%]
test_api.py::TestPaymentEndpoint::test_payments_returns_200 PASSED       [ 34%]
test_api.py::TestPaymentEndpoint::test_invalid_npi_payments_returns_404 PASSED [ 36%]
test_unified_table.py::TestSchema::test_has_required_columns PASSED      [ 38%]
test_unified_table.py::TestSchema::test_min_row_count PASSED             [ 40%]
test_unified_table.py::TestSchema::test_provider_id_unique PASSED        [ 42%]
test_unified_table.py::TestSchema::test_npi_unique_within_entity_type PASSED [ 44%]
test_unified_table.py::TestSchema::test_npi_not_null PASSED              [ 46%]
test_unified_table.py::TestSchema::test_provider_id_not_null PASSED      [ 48%]
test_unified_table.py::TestCoverageFlags::test_has_op_payments_is_bool PASSED [ 51%]
test_unified_table.py::TestCoverageFlags::test_has_pecos_enrollment_is_bool PASSED [ 53%]
test_unified_table.py::TestCoverageFlags::test_linkage_coverage_range PASSED [ 55%]
test_unified_table.py::TestCoverageFlags::test_linkage_coverage_matches_flags PASSED [ 57%]
test_unified_table.py::TestCoverageFlags::test_pecos_coverage_above_90_pct PASSED [ 59%]
test_unified_table.py::TestCoverageFlags::test_data_sources_not_null PASSED [ 61%]
test_unified_table.py::TestCoverageFlags::test_data_sources_starts_with_medicare PASSED [ 63%]
test_unified_table.py::TestNameReconciliation::test_reconciled_lastname_not_null PASSED [ 65%]
test_unified_table.py::TestNameReconciliation::test_reconciled_state_not_null PASSED [ 68%]
test_unified_table.py::TestNameReconciliation::test_reconciled_prefers_medicare PASSED [ 70%]
test_unified_table.py::TestNameReconciliation::test_orgs_have_no_first_name_in_med PASSED [ 72%]
test_unified_table.py::TestPayments::test_payments_provider_id_unique PASSED [ 74%]
test_unified_table.py::TestPayments::test_no_negative_payments PASSED    [ 76%]
test_unified_table.py::TestPayments::test_sum_payment_geq_max_payment PASSED [ 78%]
test_unified_table.py::TestPayments::test_n_payments_positive PASSED     [ 80%]
test_unified_table.py::TestPayments::test_date_range_valid PASSED        [ 82%]
test_unified_table.py::TestTransitiveChains::test_transitive_has_provider_id PASSED [ 85%]
test_unified_table.py::TestTransitiveChains::test_match_tier_valid PASSED [ 87%]
test_unified_table.py::TestTransitiveChains::test_full_chain_count_bounded PASSED [ 89%]
test_unified_table.py::TestTransitiveChains::test_linkage_path_populated PASSED [ 91%]
test_unified_table.py::TestConflicts::test_conflict_types_present PASSED [ 93%]
test_unified_table.py::TestConflicts::test_multi_match_under_100 PASSED  [ 95%]
test_unified_table.py::TestConflicts::test_name_mismatch_under_5_pct PASSED [ 97%]
test_unified_table.py::TestConflicts::test_counts_are_positive PASSED    [100%]

============================= 47 passed in 8.52s ==============================
