# Gap 7: Provider Entity Lookup API

A FastAPI service that exposes the unified 3-way linked provider table
(Medicare × Open Payments × PECOS) from Phase 5 as a REST API.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the server
uvicorn app:app --reload --port 8000

# 3. Open docs
# http://localhost:8000/docs
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `UNIFIED_PARQUET` | `../artifacts/phase5_entity_resolution/unified_provider_entities.parquet` | Path to the unified parquet file |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check — returns provider count |
| `GET` | `/providers/{npi}` | Lookup single provider by NPI (full detail) |
| `GET` | `/providers?name=&state=` | Search providers with filters (paginated) |
| `GET` | `/providers/{npi}/payments` | Payment summary for a provider |
| `GET` | `/stats` | Dataset-level summary statistics |
| `GET` | `/stats/coverage` | Venn coverage breakdown by data source |

## Search Filters

The `/providers` endpoint supports:
- `name` — case-insensitive substring match on first or last name
- `state` — exact state abbreviation (e.g., `NY`, `CA`)
- `entity_type` — `I` (individual) or `O` (organization)
- `has_op` — `true`/`false` filter by Open Payments linkage
- `has_pecos` — `true`/`false` filter by PECOS enrollment
- `page` / `page_size` — pagination (default: page 1, 20 results)

## Example Requests

```bash
# Health check
curl http://localhost:8000/health

# Lookup by NPI
curl http://localhost:8000/providers/1003000126

# Search by name in NY
curl "http://localhost:8000/providers?name=SMITH&state=NY&page_size=5"

# Get payment data
curl http://localhost:8000/providers/1003000126/payments

# Dataset stats
curl http://localhost:8000/stats

# Coverage breakdown
curl http://localhost:8000/stats/coverage
```

## Architecture

```
unified_provider_entities.parquet  (1.24M rows, ~60MB)
        │
        ▼
   FastAPI app (app.py)
        │
        ├── /providers/{npi}      → single NPI lookup
        ├── /providers?...        → filtered search (paginated)
        ├── /providers/{npi}/payments → payment detail
        ├── /stats                → aggregate metrics
        └── /stats/coverage       → Venn breakdown
```

The parquet is loaded into a pandas DataFrame at startup (~2s, ~500MB RAM).
All queries run as in-memory DataFrame operations.

## Deployment

For production (e.g., Render):

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

Or add a `Procfile`:
```
web: uvicorn app:app --host 0.0.0.0 --port $PORT
```
