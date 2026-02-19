"""
CMS Provider Entity Resolution — FastAPI Web Service
=====================================================
Serves the unified provider entity table via REST endpoints.

Run:  uvicorn app:app --reload --port 8000
Docs: http://localhost:8000/docs
"""
import os
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

# ── Load Data ────────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_CANDIDATES = [
    os.environ.get("PARQUET_PATH", ""),
    os.path.join(_THIS_DIR, "..", "artifacts", "phase5_entity_resolution", "unified_provider_entities.parquet"),
    os.path.join(_THIS_DIR, "artifacts", "phase5_entity_resolution", "unified_provider_entities.parquet"),
    os.path.join(_THIS_DIR, "unified_provider_entities.parquet"),
    os.path.join(_THIS_DIR, "..", "unified_provider_entities.parquet"),
]

PARQUET_PATH = None
for candidate in _CANDIDATES:
    if candidate and os.path.isfile(candidate):
        PARQUET_PATH = os.path.abspath(candidate)
        break

if PARQUET_PATH:
    df = pd.read_parquet(PARQUET_PATH)
    print(f"Loaded {len(df):,} providers from {PARQUET_PATH}")
    print(f"Columns: {df.columns.tolist()}")
else:
    df = pd.DataFrame()
    searched = [c for c in _CANDIDATES if c]
    print(f"WARNING: No parquet found. Searched: {searched}")

# ── Column Name Resolution ───────────────────────────────────
def _col(name: str) -> str:
    """Return the actual column name, handling underscore variants."""
    if name in df.columns:
        return name
    no_under = name.replace("_", "")
    if no_under in df.columns:
        return no_under
    return name

COL_NPI = _col("npi")
COL_FIRST_MED = _col("first_med")
COL_LAST_MED = _col("last_med")
COL_STATE_MED = _col("state_med")
COL_PROVIDER_ID = _col("provider_id")
COL_ENTITY_TYPE = _col("entity_type")
COL_FIRST_NAME = _col("first_name_reconciled")
COL_LAST_NAME = _col("last_name_reconciled")
COL_STATE = _col("state_reconciled")
COL_HAS_OP = _col("has_op_payments")
COL_HAS_PECOS = _col("has_pecos_enrollment")
COL_LINKAGE = _col("linkage_coverage")
COL_SOURCES = _col("data_sources")


# ── Helpers ──────────────────────────────────────────────────
def _safe_dict(row_or_df, orient="records"):
    """Convert a row/DataFrame to JSON-safe dict, handling nullable int types."""
    if isinstance(row_or_df, pd.Series):
        d = {}
        for k, v in row_or_df.items():
            if pd.isna(v):
                d[k] = None
            elif isinstance(v, (np.integer,)):
                d[k] = int(v)
            elif isinstance(v, (np.floating,)):
                d[k] = float(v)
            elif isinstance(v, (np.bool_,)):
                d[k] = bool(v)
            else:
                d[k] = v
        return d

    # DataFrame path
    records = []
    for _, row in row_or_df.iterrows():
        d = {}
        for k, v in row.items():
            if pd.isna(v):
                d[k] = None
            elif isinstance(v, (np.integer,)):
                d[k] = int(v)
            elif isinstance(v, (np.floating,)):
                d[k] = float(v)
            elif isinstance(v, (np.bool_,)):
                d[k] = bool(v)
            else:
                d[k] = v
        records.append(d)
    return records


# ── App Setup ────────────────────────────────────────────────
app = FastAPI(
    title="CMS Provider Entity Resolution API",
    description="Query the unified provider entity table across Medicare, PECOS, and Open Payments.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status": "ok",
        "provider_count": len(df),
        "parquet_path": PARQUET_PATH or "NOT FOUND",
    }


@app.get("/providers/{npi}")
def get_provider(npi: str):
    """Look up a single provider by NPI."""
    if len(df) == 0:
        raise HTTPException(status_code=503, detail="No data loaded")

    try:
        npi_val = int(npi)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Invalid NPI: {npi}")

    matches = df[df[COL_NPI] == npi_val]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"NPI {npi} not found")

    row = matches.iloc[0]
    return _safe_dict(row)


@app.get("/providers")
def search_providers(
    name: Optional[str] = Query(None, description="Name to search (first or last)"),
    state: Optional[str] = Query(None, description="Two-letter state code"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Search providers by name and/or state."""
    if len(df) == 0:
        raise HTTPException(status_code=503, detail="No data loaded")

    if name is None and state is None:
        raise HTTPException(status_code=422, detail="Provide at least 'name' or 'state'")

    result = df

    if name:
        name_upper = name.upper()
        name_mask = (
            result[COL_FIRST_NAME].fillna("").astype(str).str.upper().str.contains(name_upper, na=False) |
            result[COL_LAST_NAME].fillna("").astype(str).str.upper().str.contains(name_upper, na=False)
        )
        result = result[name_mask]

    if state:
        result = result[result[COL_STATE].astype(str).str.upper() == state.upper()]

    total = len(result)
    page = result.iloc[offset:offset + limit]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": _safe_dict(page),
    }


@app.get("/providers/{npi}/payments")
def get_payments(npi: str):
    """Get payment summary for a provider."""
    if len(df) == 0:
        raise HTTPException(status_code=503, detail="No data loaded")

    try:
        npi_val = int(npi)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Invalid NPI: {npi}")

    matches = df[df[COL_NPI] == npi_val]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"NPI {npi} not found")

    row = matches.iloc[0]
    return _safe_dict(row)


@app.get("/stats")
def get_stats():
    """Dataset-level summary statistics."""
    if len(df) == 0:
        raise HTTPException(status_code=503, detail="No data loaded")

    total = len(df)
    result = {"total_providers": total}

    if COL_ENTITY_TYPE in df.columns:
        result["individuals"] = int((df[COL_ENTITY_TYPE] == "I").sum())
        result["organizations"] = int((df[COL_ENTITY_TYPE] == "O").sum())

    result["with_pecos"] = int(df[COL_HAS_PECOS].sum())
    result["with_op_payments"] = int(df[COL_HAS_OP].sum())
    result["pecos_coverage_pct"] = round(float(df[COL_HAS_PECOS].mean()) * 100, 2)

    return result


@app.get("/stats/coverage")
def get_coverage():
    """Coverage breakdown by data source combination."""
    if len(df) == 0:
        raise HTTPException(status_code=503, detail="No data loaded")

    counts = df[COL_SOURCES].value_counts().reset_index()
    counts.columns = ["data_sources", "count"]
    return counts.to_dict(orient="records")
