# Healthcare Provider Record Linkage System

An end-to-end record linkage pipeline that identifies matching healthcare provider entities across three heterogeneous CMS data sources using probabilistic matching, machine learning classification, and Locality-Sensitive Hashing (LSH). The system resolves 1.2M+ provider identities from Medicare, Open Payments, and PECOS datasets into a unified entity table and exposes results through a FastAPI REST API.

## Project Overview

This system demonstrates expertise in statistical modeling, similarity computation, feature engineering, and entity resolution for real-world healthcare data. It processes 24M+ Medicare records, 11M+ Open Payments records, and 3M+ PECOS enrollment records to produce unified provider profiles with confidence scores and linkage coverage metadata.

### Data Sources

| Dataset | Source | Scale | Key Fields |
|---|---|---|---|
| Medicare Provider Utilization | data.cms.gov | 24M records (2023) | NPI, Provider Name, Address, Specialty |
| Open Payments | openpaymentsdata.cms.gov | 11M records/year | Physician Profile ID, Name, NPI (when available), Payment Details |
| PECOS Enrollment | data.cms.gov | 3M enrolled providers | NPI, Business Name, Enrollment Date, Provider Type |

## Repository Structure

```
.
├── artifacts/                    # Pipeline outputs organized by phase
│   ├── phase1_eda/               # Exploratory data analysis results
│   ├── phase2_preprocessing/     # Cleaned Parquet files for all three datasets
│   ├── phase3_blocking/          # Blocking strategy evaluation and candidate pairs
│   ├── phase4_linkage/           # Feature matrices and match classifications
│   ├── phase5_entity_resolution/ # Unified provider entity table
│   ├── phase6_lsh_benchmark/     # LSH hyperparameter sweep results
│   └── phase7_temporal_drift/    # Temporal drift analysis outputs
├── case-value/                   # Business value analysis and ROI documentation
├── data/                         # Raw dataset storage
├── docs/                         # Technical documentation and methodology
├── lib/                          # Shared utility modules
├── notebooks/                    # Jupyter notebooks for each pipeline phase
├── test-suite/                   # Pytest test suite (47 tests)
├── web-api/                      # FastAPI REST API for provider lookup
├── venv/                         # Python virtual environment
├── .gitignore
└── requirements.txt
```

## Pipeline Phases

### Phase 1 -- Exploratory Data Analysis

Statistical profiling, distribution analysis, data quality assessment, missing value pattern analysis, and cross-dataset schema mapping.

### Phase 2 -- Preprocessing

Standardization, cleaning, and Parquet export for all three datasets. Produces `openpaymentsclean.parquet`, `medicareclean.parquet`, and `pecosclean.parquet`. Records are classified into linkage tiers: tier-1 NPI (933K), tier-2 fuzzy (4.7K), and unmatchable (31K).

### Phase 3 -- Blocking Strategy

Multiple blocking approaches evaluated for candidate pair reduction, including exact blocking, sorted neighborhood, and state/Soundex composite keys. Strategy B (State + Last Name Soundex) achieves a 99.99% reduction ratio with 427K candidate pairs.

### Phase 4 -- Linkage and Classification

Feature engineering across string similarity (Jaro-Winkler, Levenshtein, cosine), phonetic matching (Soundex, Metaphone), and address similarity. Machine learning classification (logistic regression, random forest, gradient boosting) assigns match/possible/non-match tiers with ML match probability scores.

### Phase 5 -- Entity Resolution

Builds a unified provider entity table of 1,237,145 providers (1,175,281 individuals + 61,864 organizations). Integrates tier-1 NPI and tier-2 fuzzy links from Open Payments, PECOS enrollment data, and aggregated payment statistics. Includes transitive closure chains (OP-Med-PECOS), conflict detection, and a 3-way coverage Venn diagram.

- 92.0% of providers have PECOS enrollment data
- 43.8% of providers have Open Payments data
- 533,266 providers linked across all three sources

### Phase 6 -- LSH Benchmark

Benchmarks Locality-Sensitive Hashing at full dataset scale (933K tier-1 NPI records x 1.175M Medicare). Sweeps MinHash `num_perm` and Jaccard `threshold` parameters. Best configuration (perm=128, threshold=0.5) produces 100,196 candidate pairs at 99.998% reduction ratio, finding 63,483 unique pairs not captured by traditional blocking.

### Phase 7 -- Temporal Drift Analysis

Analyzes how record linkage quality changes across PECOS enrollment years (2003--2025). Key findings:

- Name mismatch rate between Medicare and PECOS: 2.66% overall, inversely correlated with enrollment recency (r = -0.945)
- Geographic mobility rate: 15.6% of providers changed states between PECOS enrollment and Medicare 2023 snapshot
- Top migration corridors: DC-MD, DC-VA, NJ-NY
- Anomalous years detected: 2024 (70.0% combined risk), 2025 (97.5%)

## Web API

A FastAPI application in `web-api/` exposes the unified provider entity table through REST endpoints.

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check with provider count |
| `/providers/{npi}` | GET | Lookup provider by NPI |
| `/providers` | GET | Search by name and/or state |
| `/providers/{npi}/payments` | GET | Payment details for a provider |
| `/stats` | GET | Aggregate pipeline statistics |
| `/stats/coverage` | GET | Data source coverage breakdown |

## Test Suite

The test suite contains 47 tests covering API endpoints and unified table integrity.

- **API Tests** -- Health, provider lookup, search, stats, payment endpoints
- **Schema Tests** -- Required columns, row count, uniqueness constraints, NPI validation
- **Coverage Flag Tests** -- Boolean flags, linkage coverage range, PECOS coverage above 90%
- **Name Reconciliation Tests** -- Not-null reconciled names, Medicare preference logic
- **Payment Tests** -- Provider ID uniqueness, non-negative payments, date range validity
- **Transitive Chain Tests** -- Provider ID presence, match tier validation, linkage path population
- **Conflict Tests** -- Multi-match under 100, name mismatch under 5%

Latest run: **46 passed, 1 failed** (47 total).

## Setup and Installation

```bash
git clone <repository-url>
cd <project-directory>

python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

## Running the Pipeline

Execute notebooks sequentially from the `notebooks/` directory:

```
notebooks/1_eda.ipynb
notebooks/2_preprocessing.ipynb
notebooks/3_blocking.ipynb
notebooks/4_linkage.ipynb
notebooks/5_entity_resolution.ipynb
notebooks/6_lsh_benchmark.ipynb
notebooks/7_temporal_drift.ipynb
```

Each notebook reads from the previous phase's artifacts and writes outputs to its corresponding `artifacts/` subdirectory.

## Running the API

```bash
cd web-api
uvicorn main:app --reload
```

## Running Tests

```bash
cd test-suite
pytest -v
```

## Technology Stack

- **Language:** Python 3.13
- **Data Processing:** Pandas, NumPy, Parquet
- **Similarity:** Jaro-Winkler, Levenshtein, Soundex, Metaphone
- **LSH:** datasketch (MinHash, MinHashLSH)
- **ML:** scikit-learn (Logistic Regression, Random Forest, Gradient Boosting)
- **API:** FastAPI, Uvicorn, Starlette
- **Visualization:** Matplotlib, matplotlib-venn
- **Testing:** pytest

## Author

**Shubhan Kadam**
Email: [dev.shubhankadam@gmail.com](mailto:dev.shubhankadam@gmail.com)
