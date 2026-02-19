# Phase 1: Exploratory Data Analysis (EDA)

## Overview
This notebook performs a comprehensive exploratory analysis of three CMS healthcare datasets to understand their structure, quality, and linkage potential before building the record linkage system.

**Goal**: Answer three key questions before building the pipeline:
1. What fields are available for matching?
2. How much overlap exists between datasets via NPI?
3. What data quality issues need preprocessing?

## Datasets Analyzed
| Dataset | Rows | Columns | Memory (MB) | Unique NPIs | Description |
|---------|------|---------|-------------|-------------|-------------|
| PECOS | 2,936,748 | 11 | 1,415.0 | 2,521,536 | Medicare provider enrollment data |
| Medicare | 9,660,647 | 28 | 11,188.0 | 1,175,281 | Provider utilization and payment data |
| Open Payments | 14,700,786 | 15 | 9,620.6 | 933,619 | Pharmaceutical/device manufacturer payments to providers |
| **TOTAL** | **27,298,181** | — | **22,223.5** | — | — |

---

## Part 1: Dataset Overview & Schema
- Loads all three raw CSV files (~27.3M total rows, ~22 GB memory). Open Payments loaded in 200K-row chunks due to size.
- Profiles every column: dtype, non-null counts, unique values.

**Key Schema Details**:
- **PECOS**: `ENRLMTID` is a unique enrollment identifier (2,936,748 unique — one per row). `FIRSTNAME`/`LASTNAME` cover 85.2% of rows (individuals); `ORGNAME` covers 14.8% (organizations).
- **Medicare**: Service-level data — each row is one NPI + one HCPCS code. 6,405 unique procedure codes. Financial columns (`AvgSbmtdChrg`, `AvgMdcrAlowdAmt`, etc.) are all 100% non-null.
- **Open Payments**: 91 columns in the raw file, reduced to 15 key columns for linkage. `CoveredRecipientProfileID` (938,292 unique) is the OP-internal provider key.

---

## Part 2: Missing Value Analysis

### Missing Value Rates (Key Fields)
| Field | PECOS | Medicare | Open Payments |
|-------|-------|----------|---------------|
| NPI | 0.00% | 0.00% | 0.30% |
| First Name | 14.79% | 5.56% | 0.21% |
| Last Name | 14.79% | 0.00% | 0.22% |
| Middle Name | 46.42% | 35.08% | — |
| Street Address Line 2 | — | 76.12% | — |
| Org Name | 85.21% | — | — |

### Key Insight: Structural Missingness (MNAR)
The missing values are **Not Missing At Random** — they are by design:

| Dataset | Individuals | Organizations |
|---------|-------------|---------------|
| PECOS | 85.2% (have `FIRSTNAME`) | 14.8% (have `ORGNAME`) |
| Medicare | 94.4% (Entity Code = I) | 5.6% (Entity Code = O) |
| Open Payments | 99.7% (have NPI) | 0.3% (no NPI) |

- PECOS has **zero records** with both `FIRSTNAME` and `ORGNAME` populated, and only 30 with neither.
- **Implication**: Must handle individuals and organizations as **separate matching pipelines** since they use different name fields. This is not a data quality issue — it's intentional schema design.

---

## Part 3: Field Distribution Analysis

### 3.1 Cardinality (Discriminative Power)
| Field | PECOS Unique | Medicare Unique | OP Unique |
|-------|-------------|-----------------|-----------|
| NPI | 2,521,536 | 1,175,281 | 933,619 |
| State | 56 | 62 | 59 |
| First Name | 137,422 | 81,125 | 97,757 |
| Last Name | 407,686 | 285,735 | 290,120 |
| ZIP5 | — | 20,681 | 284,747 |

### 3.2 State Distribution
Top states are consistent across all three datasets: **CA, TX, NY, FL** each hold 6–9% of records. This confirms state is a reliable blocking attribute.

### 3.3 Provider Type Distribution
- PECOS: Nurse Practitioner (13.4%), Clinic/Group Practice (8.1%), Physician Assistant (6.5%)
- Medicare: Diagnostic Radiology (11.7%), Nurse Practitioner (9.2%), Internal Medicine (8.4%)

### 3.4 Name Frequency — Critical Finding
| Metric | PECOS | Medicare |
|--------|-------|---------|
| Top 100 first names cover | 38.5% of individuals | 42.9% of records |
| Most common first name | MICHAEL (36,494) | Michael (200,553) |
| Most common last name | SMITH (13,853) | Patel (64,006) |

- **"Walgreen Co"** appears 50,784 times as a Medicare last/org name — organizations inflate Medicare name frequencies.
- **Implication**: Name-only matching will produce many false positives. Must combine with NPI, state, or address for disambiguation.

---

## Part 4: Data Quality Assessment

### 4.1 NPI Validation
100K-sample Luhn checksum validation (with 80840 prefix):
- **PECOS**: 100,000/100,000 valid (100.00%)
- **Medicare**: 100,000/100,000 valid (100.00%)
- **Open Payments**: 100,000/100,000 valid (100.00%)

**Result**: NPIs are structurally trustworthy as exact match keys across all datasets.

### 4.2 Name Case Quality
| Dataset | All Uppercase | Mixed Case | Action Needed |
|---------|--------------|------------|---------------|
| PECOS `FIRSTNAME` | 100.00% | 0.00% | Already standardized |
| Medicare First Name | 0.14% | 99.86% | Convert to UPPER |
| Open Payments First Name | 96.56% | 3.44% | Convert to UPPER |

- OP's 3.44% mixed-case minority (~504K records) suggests two different data entry pipelines within the Open Payments system.
- PECOS has 5 first names containing digits and 722 single-character names — minor cleanup needed.

### 4.3 ZIP Code Quality
| Dataset | Clean 5-digit | Needs Fixing | Issue |
|---------|--------------|--------------|-------|
| Medicare | 99.99% | ~30 records | Short ZIPs (leading zeros stripped) |
| Open Payments | 97.3% | ~391K records | Mixed formats: ZIP+4, short codes, 7-char anomalies |

- **Fix**: Left-pad short ZIPs to 5 digits; extract first 5 chars from longer ZIPs.

---

## Part 5: Cross-Dataset NPI Overlap

### 5.1 Unique NPI Counts
- PECOS: **2,521,536** unique NPIs
- Medicare: **1,175,281** unique NPIs
- Open Payments: **933,619** unique NPIs
- Total union across all datasets: **2,719,106** unique NPIs

### 5.2 Pairwise Overlap
| Pair | Overlap | % of Dataset A | % of Dataset B |
|------|---------|---------------|----------------|
| PECOS ∩ Medicare | 1,138,464 | 45.1% of PECOS | **96.9% of Medicare** |
| PECOS ∩ Open Payments | 763,703 | 30.3% of PECOS | 81.8% of OP |
| Medicare ∩ Open Payments | 542,329 | 46.1% of Medicare | 58.1% of OP |
| **All Three** | **533,166** | — | — |

### 5.3 Exclusive NPIs
- Only in PECOS: **1,152,535** (45.7%) — enrolled but didn't bill Medicare or receive industry payments in 2023
- Only in Medicare: **27,654** (2.4%) — small gap, likely enrollment data changes
- Only in Open Payments: **160,753** (17.2%) — will need fuzzy matching as fallback

### 5.4 NPI Duplication (Rows per Unique NPI)
| Metric | PECOS | Medicare | Open Payments |
|--------|-------|---------|---------------|
| Mean rows/NPI | 1.2 | 8.2 | 15.7 |
| Median rows/NPI | 1.0 | 5.0 | 4.0 |
| Max rows/NPI | 75 | 658 | 1,175 |
| NPIs with 1 row | 2,239,363 | 135,639 | 249,674 |
| NPIs with >10 rows | 2,258 | 262,642 | 280,796 |

**Implication**: Medicare and Open Payments **must be deduplicated to provider-level** (one row per NPI) before linkage.

### 5.5 Multi-NPI Organizations (PECOS-Specific)
- 14,210 `PECOS_ASCTCNTL_ID` values link to **multiple NPIs** — these are parent organizations with multiple billing locations (e.g., "United Neighborhood Health Services" has 11 NPIs under one control ID).
- 575 NPIs appear as both Individual (I) and Organization (O) — likely data entry errors (0.02%).
- `MULTIPLE_NPI_FLAG = Y` tracks organizations with multiple NPIs (14,345 unique NPIs), not individuals with dual roles.

---

## Part 6: Temporal & Correlation Analysis

### 6.1 Numeric Correlation (Medicare)
- `AvgMdcrAlowdAmt`, `AvgMdcrPymtAmt`, and `AvgMdcrStdzdAmt` are **highly correlated (r > 0.90)** — redundant columns can be dropped.

### 6.2 Categorical Independence (Cramér's V)
- **State vs. Provider Type**: Weak association — confirms they are **effective as independent blocking keys**.
- `RndrngPrvdrEntCd` (entity code) is redundant with provider type and should not be used as an additional blocking dimension.

### 6.3 Spearman Rank Correlation (Open Payments)
- **ρ = 0.8511** (p < 0.001) between payment count and total payment amount (200K NPI sample).
- Strong positive monotonic relationship: providers who receive more payments also tend to receive higher totals.
- **Implication**: `payment_count` and `payment_sum` are correlated but **not redundant** — both can serve as provider-level validation features.

### 6.4 Temporal Range
- Open Payments: All of 2023 (2023-01-01 to 2023-12-31)
- PECOS enrollment dates: **2002–2025** (cumulative registry, not a single snapshot)

---

## Part 7: Statistical Data Quality Testing

### 7.1 Missingness Mechanism Tests
| Test | Dataset/Field | Statistic | p-value | Result |
|------|--------------|-----------|---------|--------|
| Chi-squared: NPI missing vs. State | OP | χ² = 11,692 | < 0.001 | **MAR** — certain states have higher missing NPI rates |
| Mann-Whitney U: Payment by NPI presence | OP | U = 612.8M | < 0.001 | **MAR** — NPI-missing records have higher median payment ($500 vs $19.44) |
| Chi-squared: First Name vs. Entity Code | Medicare | — | < 0.001 | **MNAR** — organizations inherently lack first names |

**Summary**: MNAR fields (names for orgs) should be **routed to a separate pipeline, not imputed**. MAR fields (OP NPI) can be addressed via fuzzy matching fallback.

### 7.2 Distribution Normality Testing
- **Shapiro-Wilk** and **D'Agostino-Pearson** tests both reject normality for all payment/utilization columns (p < 0.001).
- All distributions are **heavily right-skewed** with **extreme kurtosis** (leptokurtic).
- **Implication**: Log-transformation is strongly recommended. Use non-parametric tests (Mann-Whitney, Spearman) instead of t-tests or Pearson correlation.

### 7.3 IQR Outlier Detection
- **16.48%** of Open Payments records are outliers by the IQR method — a significant minority driven by large one-time transfers.

---

## Part 8: EDA Summary & Linkage Feasibility

### Linkage Feasibility Matrix (Cross-Dataset Field Map)
| Field | PECOS (Coverage) | Medicare (Coverage) | OP (Coverage) | Linkage Role | Match Type |
|-------|-----------------|--------------------|--------------|--------------| -----------|
| NPI | `NPI` (100%) | `RndrngNPI` (100%) | `CoveredRecipientNPI` (99.7%) | Primary Key | Exact |
| First Name | `FIRSTNAME` (85.2%) | `RndrngPrvdrFirstName` (94.4%) | `CoveredRecipientFirstName` (99.8%) | Fuzzy Fallback | Jaro-Winkler |
| Last Name | `LASTNAME` (85.2%) | `RndrngPrvdrLastOrgName` (100%) | `CoveredRecipientLastName` (99.8%) | Fuzzy + Blocking | Jaro-Winkler |
| State | `STATECD` (100%) | `RndrngPrvdrStateAbrvtn` (100%) | `RecipientState` (100%) | Blocking | Exact |
| ZIP | — | `RndrngPrvdrZip5` (100%) | `RecipientZipCode` (100%) | Blocking + Scoring | Exact (5-digit) |
| Street | — | `RndrngPrvdrSt1` (100%) | Address Line 1 (100%) | Confirmation | Token/Edit Distance |
| City | — | `RndrngPrvdrCity` (100%) | `RecipientCity` (100%) | Confirmation | Exact/Fuzzy |

### Two-Tier Linkage Strategy
- **Tier 1 — NPI Exact Match**: Primary joins across all three datasets. Covers ~96% of linkable records. Uses O(n) hash joins (minutes).
- **Tier 2 — Blocked Fuzzy Match**: Fallback for records without NPI matches. Blocking by State + Last Name Prefix to keep blocks small (median 4 records). Scoring via name similarity + ZIP proximity.

### Scalability Assessment
| Strategy | Comparisons | Feasibility |
|----------|-------------|-------------|
| Naive cross-join (no blocking) | ~2.96 trillion (PECOS×Medicare) | **Infeasible** |
| NPI Exact Match | ~1.1M joins | Minutes |
| State Blocking | ~52K per block avg | Minutes–Hours |
| State + Last Name Prefix (3 chars) | Median 4 per block, 147,345 blocks | Minutes |

### Preprocessing Needed (Phase 2)
1. Case normalization → UPPERCASE
2. ZIP standardization → 5-digit
3. Provider-level dedup → collapse multiple rows per NPI
4. Separate Individual vs. Organization pipelines
5. Use `ENRLMT_DATE` from PECOS for temporal filters

---

## Key Outputs
| Artifact | Description |
|----------|-------------|
| `../artifacts/phase1_eda/data_profile.json` | Full field-level profiling for all datasets (~11 KB) |
| `../artifacts/phase1_eda/linkage_feasibility_matrix.csv` | Cross-dataset field mapping with coverage, match types, and linkage roles |
