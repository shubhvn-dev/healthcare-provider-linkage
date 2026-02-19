"""
Test Suite — FastAPI Web Service Endpoints
==========================================
Tests the Gap 7 API using FastAPI's TestClient (no server needed).

Run:  pytest test_api.py -v

NOTE: Requires the unified parquet at the path expected by app.py.
      Set PARQUET_PATH env var if needed.
"""
import os
import sys
import pytest

# Add the API directory to path so we can import app
# API_DIR = os.environ.get("API_DIR", "../web-api")
API_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web-api"))
sys.path.insert(0, API_DIR)

try:
    from fastapi.testclient import TestClient
    from app import app
    API_AVAILABLE = True
except Exception:
    API_AVAILABLE = False


@pytest.fixture(scope="module")
def client():
    if not API_AVAILABLE:
        pytest.skip("FastAPI app not importable — check API_DIR path")
    return TestClient(app)


# ── Health Check ──────────────────────────────────────────────

class TestHealthEndpoint:

    def test_health_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_health_has_status(self, client):
        data = client.get("/health").json()
        assert "status" in data
        assert data["status"] == "ok"

    def test_health_has_provider_count(self, client):
        data = client.get("/health").json()
        assert "provider_count" in data
        assert data["provider_count"] > 0


# ── Provider Lookup by NPI ────────────────────────────────────

class TestProviderLookup:

    KNOWN_NPI = "1003000126"  # ARDALAN ENKESHAFI — in all test runs

    def test_valid_npi_returns_200(self, client):
        r = client.get(f"/providers/{self.KNOWN_NPI}")
        assert r.status_code == 200

    def test_valid_npi_has_name(self, client):
        data = client.get(f"/providers/{self.KNOWN_NPI}").json()
        assert any(k in data for k in ["last_name_reconciled", "last_med", "name"])


    def test_invalid_npi_returns_404(self, client):
        r = client.get("/providers/0000000000")
        assert r.status_code == 404

    def test_non_numeric_npi_returns_error(self, client):
        r = client.get("/providers/ABCDEFGHIJ")
        assert r.status_code in (404, 422)  # 404 or validation error


# ── Provider Search ───────────────────────────────────────────

class TestProviderSearch:

    def test_search_by_name_returns_results(self, client):
        r = client.get("/providers", params={"name": "SMITH"})
        assert r.status_code == 200
        data = r.json()
        # Should be a list or have a results key
        results = data if isinstance(data, list) else data.get("results", data.get("providers", []))
        assert len(results) > 0, "No results for common name SMITH"

    def test_search_with_state_filter(self, client):
        r = client.get("/providers", params={"name": "SMITH", "state": "NY"})
        assert r.status_code == 200

    def test_search_empty_returns_empty(self, client):
        r = client.get("/providers", params={"name": "ZZZZNOTANAME999"})
        assert r.status_code == 200
        data = r.json()
        results = data if isinstance(data, list) else data.get("results", data.get("providers", []))
        assert len(results) == 0

    def test_search_without_params(self, client):
        """Search with no params should either return results or 422."""
        r = client.get("/providers")
        assert r.status_code in (200, 422)


# ── Stats Endpoints ───────────────────────────────────────────

class TestStatsEndpoints:

    def test_stats_returns_200(self, client):
        r = client.get("/stats")
        assert r.status_code == 200

    def test_stats_has_total(self, client):
        data = client.get("/stats").json()
        assert "total_providers" in data or "total" in data

    def test_coverage_returns_200(self, client):
        r = client.get("/stats/coverage")
        assert r.status_code == 200

    def test_coverage_has_entries(self, client):
        data = client.get("/stats/coverage").json()
        # Should be a list or dict with coverage info
        assert len(data) > 0


# ── Payment Endpoint ──────────────────────────────────────────

class TestPaymentEndpoint:

    KNOWN_NPI = "1003000126"

    def test_payments_returns_200(self, client):
        r = client.get(f"/providers/{self.KNOWN_NPI}/payments")
        assert r.status_code == 200

    def test_invalid_npi_payments_returns_404(self, client):
        r = client.get("/providers/0000000000/payments")
        assert r.status_code == 404
