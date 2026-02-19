# Gap 8: Test Suite

## Overview
A `pytest`-based test suite validating:
1. **Unified table invariants** — schema, dtypes, PK uniqueness, no null keys
2. **Coverage flag consistency** — boolean flags match `linkage_coverage` arithmetic
3. **Name reconciliation** — Medicare-first preference, org providers have no first name
4. **Payment integrity** — no negatives, avg ≤ max, valid date ranges
5. **Transitive chain integrity** — valid tiers, populated linkage paths
6. **Data quality conflicts** — multi-match < 100, name mismatch < 5%
7. **API endpoints** — all 6 FastAPI routes via TestClient

## Files

| File | Tests | What It Covers |
|------|-------|----------------|
| `test_unified_table.py` | 25 | Parquet artifacts from Phase 5 |
| `test_api.py` | 16 | FastAPI endpoints from Gap 7 |
| `conftest.py` | — | Shared pytest config |
| `requirements.txt` | — | Dependencies |

## Running

```bash
cd test_suite

# All tests
pytest -v

# Just table tests (no API dependency)
pytest test_unified_table.py -v

# Just API tests (requires gap7_web_api/)
pytest test_api.py -v

# With custom artifact path
PH5_DIR=../artifacts/phase5_entity_resolution pytest -v
```

## Test Count: 41 total
- `TestSchema` — 6 tests
- `TestCoverageFlags` — 7 tests
- `TestNameReconciliation` — 4 tests
- `TestPayments` — 5 tests
- `TestTransitiveChains` — 4 tests
- `TestConflicts` — 4 tests
- `TestHealthEndpoint` — 3 tests
- `TestProviderLookup` — 4 tests
- `TestProviderSearch` — 4 tests
- `TestStatsEndpoints` — 4 tests
- `TestPaymentEndpoint` — 2 tests

## CI Integration
Add to GitHub Actions:
```yaml
- name: Run tests
  run: |
    pip install -r gap8_test_suite/requirements.txt
    pytest gap8_test_suite/ -v --tb=short
```
