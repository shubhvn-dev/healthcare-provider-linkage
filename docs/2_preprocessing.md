# Phase 2: Data Preprocessing & Standardization

## Overview
This notebook transforms the three raw healthcare datasets into clean, standardized, provider-level parquet files ready for blocking and matching. It uses a reusable `preprocessing.py` module stored in `../lib/` for core cleaning and validation functions.

**Goal**: Build reusable cleaning functions for names, addresses, and identifiers that will feed into blocking and matching.

---

## 2.1: Import Preprocessing Module

Loads reusable functions from `../lib/preprocessing.py`:

| Function | Purpose |
|----------|---------|
| `clean_name()` | Uppercase, strip whitespace, collapse multiple spaces, remove trailing punctuation |
| `soundex_code()` | Generate Soundex phonetic code for a name string |
| `metaphone_code()` | Generate Double Metaphone phonetic code for a name string |
| `clean_street()` | Standardize street address formatting |
| `clean_city()` | Standardize city names |
| `clean_state()` | Standardize state abbreviations |
| `normalize_zip5()` | Convert any ZIP format (ZIP+4, short codes) to 5-digit |
| `is_valid_npi()` | Validate NPI via Luhn checksum with 80840 prefix |

**Spot check**: `is_valid_npi(1053656744)` → `True` ✓

---

## 2.2: PECOS Enrollment Identity Standardization

### Enrollment ID Decoding
Parses the composite `ENRLMTID` field into its logical components:
- `ENRLMT_ENTITY`: First character — `I` (Individual) or `O` (Organization)
- `ENRLMT_DATE`: Parsed from the `YYYYMMDD` segment (positions 1–9)
- `ENRLMT_YEAR`: Calendar year extracted for temporal filtering
- `ENRLMT_SEQ`: Trailing sequence number (position 9+)

**Results**:
- Enrollment date range: **2002-08-01 → 2025-09-13** (confirms PECOS is a cumulative registry, not a single snapshot)
- Invalid `ENRLMT_DATE` rows: **0**

### Name Standardization
Applied `clean_name()` to `FIRSTNAME`, `MDLNAME`, `LASTNAME`, and `ORGNAME`:
- Convert to uppercase
- Strip leading/trailing whitespace
- Collapse multiple internal spaces
- Remove trailing punctuation

### Phonetic Feature Engineering
For individual records only (`ENRLMT_ENTITY == 'I'`), computed:
- `FIRSTNAME_SOUNDEX`, `LASTNAME_SOUNDEX` — for coarse phonetic blocking
- `FIRSTNAME_METAPHONE`, `LASTNAME_METAPHONE` — for finer phonetic matching (Scenario 2 support)

**Result**: Phonetic columns are null for the 434,372 organization records (by design — orgs don't have first/last names).

### NPI Validation
- Applied Luhn checksum (80840 prefix) to all 2,936,748 NPIs.
- **Invalid NPIs: 0** — 100% pass rate.
- `NPI_VALID = True` for all records.

### Name Presence Sanity Check
- Records with no `FIRSTNAME`, `LASTNAME`, or `ORGNAME`: **0**
- No PECOS rows need to be dropped on this basis.

### Fix 2: Dual-Entity NPI Flagging
- From EDA Part 5: **575 NPIs** appear as both Individual (I) and Organization (O) — likely data entry errors.
- Flagged with `DUAL_ENTITY_FLAG = True`: **1,198 rows** (575 unique NPIs).
- These are flagged for cautious handling in linkage rather than dropped.

### Fix 3: Temporal Alignment Flag
- Medicare and Open Payments are both program year 2023.
- Added `ENROLLED_BY_2023` flag: `ENRLMT_YEAR <= 2023`.
- **Result**: 2,405,538 of 2,936,748 enrolled by 2023 (**81.9%**).
- The remaining 18.1% enrolled in 2024–2025 — they won't have 2023 Medicare/OP activity.

### Fix 6: Multi-NPI Organization Dedup Prep
- 14,210 `PECOS_ASCTCNTL_ID` values link to multiple NPIs (parent orgs with multiple billing locations).
- Flagged with `MULTI_NPI_ORG = True`: **114,062 rows**.
- Assigned canonical `PARENT_NPI` (lowest NPI per group) for optional org-level matching in Phase 3.

### Output
- **File**: `pecos_clean.parquet`
- **Shape**: 2,936,748 rows × 24 columns (13 new columns added)
- **New columns**: `ENRLMT_ENTITY`, `ENRLMT_DATE`, `ENRLMT_YEAR`, `ENRLMT_SEQ`, `FIRSTNAME_SOUNDEX`, `LASTNAME_SOUNDEX`, `FIRSTNAME_METAPHONE`, `LASTNAME_METAPHONE`, `NPI_VALID`, `DUAL_ENTITY_FLAG`, `ENROLLED_BY_2023`, `MULTI_NPI_ORG`, `PARENT_NPI`
- **Size**: 117.2 MB
- **Memory freed**: Raw `pecosdf` deleted after save (~1.4 GB recovered)

---

## 2.3: Medicare Provider-Level Aggregation

### The Problem
The raw Medicare file is at the **service level** — each row is one NPI × one HCPCS code. A single provider can appear on hundreds of rows. The linkage pipeline needs **one row per NPI**.

### Step-by-Step Processing

**1. Name & Credential Cleaning**
- Applied `clean_name()` to `RndrngPrvdrFirstName`, `RndrngPrvdrLastOrgName`, `RndrngPrvdrMI`.
- Credential normalization: `M.D.` → `MD`, `D.O.` → `DO`, etc. (strip periods, commas, whitespace).

**2. Address Standardization**
- Applied `clean_street()`, `clean_city()`, `clean_state()` to address fields.
- ZIP codes were already clean 5-digit format in Medicare — no normalization needed.

**3. Column Drops (EDA-informed)**
- Dropped `RndrngPrvdrSt2`: 76.1% null — too sparse for matching.
- Dropped `AvgMdcrAlowdAmt` and `AvgMdcrStdzdAmt`: r > 0.90 correlation with `AvgMdcrPymtAmt` (redundant, per EDA Section 6).

**4. NPI Validation**
- Applied Luhn checksum to all 9,660,647 rows.
- **Invalid NPIs: 0**.

**5. Phonetic Features**
- Soundex and Metaphone for individual records (`RndrngPrvdrEntCd == 'I'`) only.
- 61,864 organization records receive null phonetic columns.

**6. Provider-Level Deduplication (Key Step)**
Grouped by `RndrngNPI` and computed:

| Aggregated Field | Method | Notes |
|-----------------|--------|-------|
| `total_services` | `sum(TotSrvcs)` | Total procedures across all HCPCS codes |
| `total_beneficiaries` | `sum(TotBenes)` | Total unique patients |
| `total_submitted_charges` | `sum(AvgSbmtdChrg × TotSrvcs)` | **Weighted total** (not sum of averages) |
| `total_medicare_payment` | `sum(AvgMdcrPymtAmt × TotSrvcs)` | **Weighted total** (not sum of averages) |
| `unique_hcpcs_count` | `nunique(HCPCSCd)` | Breadth of services offered |
| `service_row_count` | `count(HCPCSCd)` | Number of distinct service lines |

For provider attributes (name, address, specialty), kept the **first occurrence** as the canonical record.

### Results
| Metric | Value |
|--------|-------|
| Medicare BEFORE dedup | 9,660,647 rows |
| Medicare AFTER dedup | **1,175,281 rows** |
| Reduction | **87.8%** |
| Shape | 1,175,281 × 27 columns |
| File size | 78.0 MB |

**Why this matters**: Later phases (blocking, matching, multi-source linkage) operate at the provider level. This aggregation reduces computation by ~8× while preserving all information needed for linkage.

---

## 2.4: Open Payments Preprocessing

### Step-by-Step Processing

**1. Unlinkable Record Flagging**
- Rows with no name AND no NPI: **31,400** → flagged as `LINKABLE = False`.
- These records cannot be matched to any other dataset.

**2. NPI Cleanup**
- Converted NPI from float to int (handling NaN safely via `pd.to_numeric` + coerce).

**3. Name & Address Standardization**
- Same `clean_name()`, `clean_street()`, `clean_city()`, `clean_state()` pipeline.

**4. ZIP Normalization**
- Applied `normalize_zip5()` to convert ZIP+4, short codes, and other formats to standardized 5-digit.
- Sample output: `['55369', '46219', '98101', '89014', '43551']` ✓

**5. NPI Validation**
- **Only 4 invalid NPIs** found in the entire 14.7M-row dataset:

| NPI | Name | State |
|-----|------|-------|
| 1202321xxx | REZA FARDSHISHEH | VA |
| 1356763xxx | CHRISTOPHER AQUINO | FL |
| 1374625xxx | TRACEY TOBACK | NY |
| 1851791xxx | MEL IRVINE | FL |

**6. Phonetic Features**
- Soundex and Metaphone computed for all rows with a non-null name.

**7. Linkage Tier Assignment**
Each row classified by matchability:

| Tier | Logic | Row Count (Pre-Dedup) | Post-Dedup |
|------|-------|----------------------|------------|
| `tier1_npi` | Has valid NPI | 14,656,549 | **933,615** |
| `tier2_fuzzy` | No NPI but has real name + state | 12,813 | **4,683** |
| `unmatchable` | No NPI and no name | 31,424 | **31,405** |

**8. Tier-2 Quality Verification**
Post-dedup spot-check confirmed:
- All 4,683 `tier2_fuzzy` records have real names (0 contain "NAN" strings, 0 empty strings, 0 whitespace-only).
- Top names: JENNIFER (61), MICHAEL (51), DAVID (50), JOHN (49) — plausible name distributions.

### Provider-Level Deduplication
Grouped by NPI (primary) → ProfileID (fallback) → row index (last resort):

| Aggregated Field | Method |
|-----------------|--------|
| `total_payment_amount` | `sum(TotalAmountofPaymentUSDollars)` |
| `payment_count` | `count(TotalAmountofPaymentUSDollars)` |
| `avg_payment` | `mean(TotalAmountofPaymentUSDollars)` |
| `max_payment` | `max(TotalAmountofPaymentUSDollars)` |
| `unique_manufacturers` | `nunique(ManufacturerName)` |
| `min_payment_date` / `max_payment_date` | Date range of payments |

### Results
| Metric | Value |
|--------|-------|
| Open Payments BEFORE dedup | 14,700,786 rows |
| Open Payments AFTER dedup | **969,703 rows** |
| Reduction | **93.4%** |
| Shape | 969,703 × 22 columns |
| File size | 61.2 MB |

---

## 2.5: Post-Preprocessing Validation Report

Loads all three cleaned parquet files from disk and generates a comprehensive validation report as a phase checkpoint.

### Final Dataset Summary
| Dataset | Rows | Columns | File Size | Key NPIs | NPI_VALID |
|---------|------|---------|-----------|----------|-----------|
| PECOS | 2,936,748 | 24 | 117.2 MB | 2,521,536 | 100% True |
| Medicare | 1,175,281 | 27 | 78.0 MB | 1,175,281 | 100% True |
| Open Payments | 969,703 | 22 | 61.2 MB | 933,619 | 99.9% True |

### Key Null Counts After Cleaning
| Dataset | Field | Nulls | Notes |
|---------|-------|-------|-------|
| PECOS | `MDLNAME` | 10 | Cleaned from 1.36M — most were orgs |
| PECOS | Phonetic columns | 434,372 | Organizations (by design) |
| PECOS | `PARENT_NPI` | 2,502,376 | Only populated for multi-NPI orgs |
| Medicare | `RndrngPrvdrCrdntls` | 153,337 | Orgs + some individuals without credentials |
| Medicare | Phonetic columns | 61,864 | Organizations (by design) |
| Open Payments | `CoveredRecipientNPI` | 36,084 | Tier-2 fuzzy + unmatchable records |

### Cardinality Verification
- PECOS: `ENRLMT_ENTITY` has exactly 2 values (I, O) ✓
- Medicare: `RndrngNPI` has 1,175,281 unique values = row count ✓ (perfect dedup)
- Open Payments: `linkage_tier` has exactly 3 values ✓

**Output**: `preprocessing_report.json` — complete validation report for programmatic access.

---

## Key Insights & Results

### Data Reduction Summary
| Dataset | Raw Rows | Clean Rows | Reduction | New Columns Added |
|---------|----------|------------|-----------|------------------|
| PECOS | 2,936,748 | 2,936,748 | 0% (no row drops) | +13 (enrichment only) |
| Medicare | 9,660,647 | 1,175,281 | **87.8%** | +6 aggregated stats |
| Open Payments | 14,700,786 | 969,703 | **93.4%** | +7 aggregated stats |
| **Total** | **27,298,181** | **5,081,732** | **81.4%** | — |

### Critical Design Decisions
1. **Weighted totals, not sum of averages**: Medicare `total_submitted_charges = sum(AvgSbmtdChrg × TotSrvcs)` avoids the statistical error of averaging averages.
2. **Separate Individual vs. Organization handling**: Phonetic features only computed for individuals; orgs get null phonetic columns (MNAR by design).
3. **Three-tier OP linkage**: 96.3% of OP providers go through fast NPI matching; only 0.5% need expensive fuzzy matching; 3.2% are unsalvageable.
4. **Dual-entity flagging over dropping**: The 575 dual-entity NPIs are flagged, not removed, preserving data for cautious downstream handling.
5. **Temporal alignment**: The `ENROLLED_BY_2023` flag enables Phase 3 to filter for temporal relevance without re-parsing enrollment IDs.

## Key Outputs
| Artifact | Description | Size |
|----------|-------------|------|
| `pecos_clean.parquet` | Enriched PECOS with entity types, phonetics, flags | 117.2 MB |
| `medicare_clean.parquet` | Provider-level Medicare with aggregated stats | 78.0 MB |
| `open_payments_clean.parquet` | Provider-level OP with payment aggregates + linkage tiers | 61.2 MB |
| `preprocessing_report.json` | Validation report: shapes, dtypes, nulls, cardinality | <1 MB |
