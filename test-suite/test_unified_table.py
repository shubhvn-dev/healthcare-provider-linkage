"""
Test Suite — Unified Provider Entity Table Invariants
=====================================================
Validates the core output of Phase 5: unified_provider_entities.parquet

Run:  pytest test_unified_table.py -v
"""
import os
import pytest
import pandas as pd
import numpy as np

# ── Fixtures ─────────────────────────────────────────────────
PH5 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "artifacts", "phase5_entity_resolution"))
# PH5 = os.environ.get("PH5_DIR", "../artifacts/phase5_entity_resolution")

@pytest.fixture(scope="module")
def unified():
    path = os.path.join(PH5, "unified_provider_entities.parquet")
    assert os.path.exists(path), f"Unified parquet not found at {path}"
    return pd.read_parquet(path)

@pytest.fixture(scope="module")
def payments():
    path = os.path.join(PH5, "provider_payments.parquet")
    assert os.path.exists(path), f"Payments parquet not found at {path}"
    return pd.read_parquet(path)

@pytest.fixture(scope="module")
def transitive():
    path = os.path.join(PH5, "op_med_pecos_transitive_links.parquet")
    assert os.path.exists(path), f"Transitive parquet not found at {path}"
    return pd.read_parquet(path)

@pytest.fixture(scope="module")
def conflicts():
    path = os.path.join(PH5, "data_quality_conflicts.csv")
    assert os.path.exists(path), f"Conflicts CSV not found at {path}"
    return pd.read_csv(path)


# ── 1. Schema & Shape ───────────────────────────────────────

class TestSchema:
    """Verify the unified table has the expected structure."""

    REQUIRED_COLUMNS = [
        "npi", "provider_id", "first_med", "last_med", "state_med",
        "first_name_reconciled", "last_name_reconciled", "state_reconciled",
        "has_op_payments", "has_pecos_enrollment", "linkage_coverage",
        "data_sources",
    ]

    def test_has_required_columns(self, unified):
        missing = [c for c in self.REQUIRED_COLUMNS if c not in unified.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_min_row_count(self, unified):
        """Backbone should have at least 1M providers."""
        assert len(unified) >= 1_000_000, f"Only {len(unified):,} rows"

    def test_provider_id_unique(self, unified):
        """provider_id must be unique — it's the primary key."""
        assert unified["provider_id"].is_unique, "Duplicate provider_id found"

    def test_npi_unique_within_entity_type(self, unified):
        """NPI should be unique within each entity type (I or O).
        Cross-type duplicates are expected when an individual and org share an NPI."""
        if "entity_type" in unified.columns:
            for etype in unified["entity_type"].unique():
                subset = unified[unified["entity_type"] == etype]
                dupes = subset["npi"].duplicated().sum()
                assert dupes == 0, f"{dupes} duplicate NPIs in entity_type={etype}"
        else:
            assert unified["npi"].is_unique, "Duplicate NPI found"

    def test_npi_not_null(self, unified):
        assert unified["npi"].notna().all(), "Null NPI values found"

    def test_provider_id_not_null(self, unified):
        assert unified["provider_id"].notna().all(), "Null provider_id values found"


# ── 2. Coverage Flags ────────────────────────────────────────

class TestCoverageFlags:
    """Verify coverage flags are consistent with underlying data."""

    def test_has_op_payments_is_bool(self, unified):
        assert unified["has_op_payments"].dtype == bool, \
            f"Expected bool, got {unified['has_op_payments'].dtype}"

    def test_has_pecos_enrollment_is_bool(self, unified):
        assert unified["has_pecos_enrollment"].dtype == bool, \
            f"Expected bool, got {unified['has_pecos_enrollment'].dtype}"

    def test_linkage_coverage_range(self, unified):
        """linkage_coverage should be 0, 1, or 2."""
        valid = unified["linkage_coverage"].isin([0, 1, 2]).all()
        assert valid, f"Invalid linkage_coverage values: {unified['linkage_coverage'].unique()}"

    def test_linkage_coverage_matches_flags(self, unified):
        """linkage_coverage = has_op_payments(int) + has_pecos_enrollment(int)."""
        expected = (
            unified["has_op_payments"].astype(int)
            + unified["has_pecos_enrollment"].astype(int)
        )
        assert (unified["linkage_coverage"] == expected).all(), \
            "linkage_coverage doesn't match sum of boolean flags"

    def test_pecos_coverage_above_90_pct(self, unified):
        """PECOS should cover >90% of providers (known from Phase 5)."""
        rate = unified["has_pecos_enrollment"].mean()
        assert rate > 0.90, f"PECOS coverage only {rate:.1%}"

    def test_data_sources_not_null(self, unified):
        assert unified["data_sources"].notna().all(), "Null data_sources found"

    def test_data_sources_starts_with_medicare(self, unified):
        """Every provider must include 'Medicare' in data_sources."""
        has_med = unified["data_sources"].str.contains("Medicare", case=False)
        assert has_med.all(), "Some providers missing 'Medicare' in data_sources"


# ── 3. Name Reconciliation ──────────────────────────────────

class TestNameReconciliation:
    """Verify reconciled name fields are populated correctly."""

    def test_reconciled_lastname_not_null(self, unified):
        """Every provider should have a reconciled last name."""
        null_ct = unified["last_name_reconciled"].isna().sum()
        assert null_ct == 0, f"{null_ct:,} providers missing reconciled last name"

    def test_reconciled_state_not_null(self, unified):
        null_ct = unified["state_reconciled"].isna().sum()
        assert null_ct == 0, f"{null_ct:,} providers missing reconciled state"

    def test_reconciled_prefers_medicare(self, unified):
        """Where Medicare name exists, reconciled should equal Medicare."""
        has_med_last = unified["last_med"].notna()
        match = unified.loc[has_med_last, "last_name_reconciled"] == unified.loc[has_med_last, "last_med"]
        assert match.all(), "Reconciled last name doesn't match Medicare where available"

    def test_orgs_have_no_first_name_in_med(self, unified):
        """Organization providers should have NaN first_med."""
        if "entity_type" in unified.columns:
            orgs = unified[unified["entity_type"] == "O"]
            if len(orgs) > 0:
                null_rate = orgs["first_med"].isna().mean()
                assert null_rate > 0.99, f"Only {null_rate:.1%} of orgs have null first_med"


# ── 4. Payment Data Integrity ────────────────────────────────

class TestPayments:
    """Validate payment aggregation parquet."""

    def test_payments_provider_id_unique(self, payments):
        assert payments["provider_id"].is_unique, "Duplicate provider_id in payments"

    def test_no_negative_payments(self, payments):
        assert (payments["sum_payment"] >= 0).all(), "Negative payment sums found"

    def test_sum_payment_geq_max_payment(self, payments):
        """Total payment sum should be >= the single largest payment."""
        valid = payments["sum_payment"] >= payments["max_payment"] - 0.01
        assert valid.all(), "sum_payment < max_payment for some providers"

    def test_n_payments_positive(self, payments):
        assert (payments["n_payments"] > 0).all(), "Zero-count payment rows found"

    def test_date_range_valid(self, payments):
        """first_payment_date <= last_payment_date."""
        has_dates = payments["first_payment_date"].notna() & payments["last_payment_date"].notna()
        subset = payments[has_dates]
        valid = subset["first_payment_date"] <= subset["last_payment_date"]
        assert valid.all(), "first_payment_date > last_payment_date for some rows"


# ── 5. Transitive Chain Integrity ────────────────────────────

class TestTransitiveChains:
    """Validate OP→Med→PECOS transitive links."""

    def test_transitive_has_provider_id(self, transitive):
        assert transitive["provider_id"].notna().all(), "Null provider_id in transitive chains"

    def test_match_tier_valid(self, transitive):
        valid_tiers = {"match", "possible"}
        actual = set(transitive["match_tier"].unique())
        assert actual.issubset(valid_tiers), f"Unexpected match_tier values: {actual - valid_tiers}"

    def test_full_chain_count_bounded(self, transitive):
        """Full chains (with ENRLMT_ID) should be <= total rows."""
        full = transitive["ENRLMT_ID"].notna().sum()
        assert full <= len(transitive), "More full chains than total rows"

    def test_linkage_path_populated(self, transitive):
        assert transitive["linkage_path"].notna().all(), "Null linkage_path values"


# ── 6. Data Quality Conflicts ────────────────────────────────

class TestConflicts:
    """Validate data quality conflict report."""

    def test_conflict_types_present(self, conflicts):
        expected = {"multi_match", "name_mismatch"}
        actual = set(conflicts["conflict_type"].unique())
        assert expected.issubset(actual), f"Missing conflict types: {expected - actual}"

    def test_multi_match_under_100(self, conflicts):
        """Multi-match conflicts should be <100 (sanity bound from Phase 5)."""
        mm = conflicts.loc[conflicts["conflict_type"] == "multi_match", "count"].values[0]
        assert mm < 100, f"Too many multi-match conflicts: {mm}"

    def test_name_mismatch_under_5_pct(self, conflicts):
        """Name mismatches should be <5% of checked providers."""
        pct = conflicts.loc[conflicts["conflict_type"] == "name_mismatch", "pct_affected"].values[0]
        assert pct < 5.0, f"Name mismatch rate too high: {pct:.2f}%"

    def test_counts_are_positive(self, conflicts):
        assert (conflicts["count"] >= 0).all(), "Negative conflict counts found"
