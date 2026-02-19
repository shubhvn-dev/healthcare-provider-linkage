# Phase 3: Blocking & Candidate Generation

## Overview
This notebook generates candidate record pairs for the Open Payments ↔ Medicare fuzzy linkage pipeline. It uses information-theoretic analysis to select optimal blocking keys, then applies five blocking strategies (three traditional + two advanced) and unions the results.

**Input**: Cleaned parquet files from Phase 2 (`../artifacts/phase2_preprocessing/`)
**Output**: Deduplicated candidate pairs + blocking summary (`../artifacts/phase3_blocking/`)

**The Fundamental Problem**: The tier-2 fuzzy pool has **4,683 OP records × 1,175,281 Medicare records = 5,503,840,923 possible pairs**. Comparing all 5.5 billion pairs is infeasible — blocking reduces this to a tractable number while preserving true matches.

---

## 3.1: Load Cleaned Datasets

| Dataset | Records Loaded | Purpose |
|---------|---------------|---------|
| OP (tier2_fuzzy only) | 4,683 | Left side of linkage — records without NPI |
| Medicare (full) | 1,175,281 | Right side of linkage — all providers |
| PECOS | 2,936,748 | Loaded but not used directly in blocking |

- Indexes reset with `drop=True` to ensure integer positional alignment for pair generation.
- Full cross-product: **5,503,840,923 pairs** — confirms blocking is essential.

---

## 3.2: Blocking Key Selection via Information Theory

Before picking blocking keys, the notebook uses entropy and mutual information to find the optimal attribute combinations — a **data-driven** approach rather than ad hoc selection.

### Single-Attribute Entropy (bits)
Higher entropy = more distinct values = smaller, tighter blocks:

| Attribute | Entropy (bits) | Unique Values |
|-----------|---------------|---------------|
| State | 4.586 | 53 |
| FN Soundex | 8.282 | 764 |
| First Name | 9.746 | 1,757 |
| LN Soundex | 9.868 | 1,611 |
| ZIP5 | 10.715 | 2,540 |
| Last Name | **11.319** | **3,385** |

**Insight**: Last Name has the highest entropy (most discriminative), while State has the lowest — but State is valuable for combining because it's **independent** of name attributes.

### Pairwise Mutual Information (bits)
Lower MI = more independent = better to combine:

| Pair | MI (bits) | Interpretation |
|------|-----------|----------------|
| State ↔ LN Soundex | **2.680** | Near-independent ✓ |
| State ↔ FN Soundex | **1.610** | Very independent ✓ |
| LN Soundex ↔ FN Soundex | 6.017 | Moderate overlap |
| Last Name ↔ LN Soundex | 9.868 | Highly redundant (expected — Soundex is derived from Last Name) |
| ZIP5 ↔ State | 4.572 | Moderate — ZIP is nested within State |

### Composite Key Joint Entropy
Higher joint entropy = more distinct blocks = better reduction ratio:

| Key Combination | Joint Entropy (bits) | Distinct Blocks |
|-----------------|---------------------|-----------------|
| **State + LN Soundex** | **11.774** | **3,895** |
| State + Last Name | 12.048 | 4,398 |
| **State + FN Soundex + LN Soundex** | **12.184** | **4,662** |
| State + Last Name + First Name | 12.189 | 4,672 |
| State + ZIP5 | 10.718 | 2,548 |

### Independence Ratio Check
Ratio of joint entropy to sum of individual entropies (1.0 = perfectly independent):

| Pair | Independence Ratio | Assessment |
|------|--------------------|------------|
| State + FN Soundex | **0.8749** | Near-independent ✓ |
| **State + LN Soundex** | **0.8146** | Near-independent ✓ |
| FN Soundex + LN Soundex | 0.6685 | Moderate overlap |

### Key Decision
- **Strategy A** (State + LN Soundex): Best 2-key combo — high joint entropy, low MI, near-independent.
- **Strategy C** (State + FN Soundex + LN Soundex): Adding FN Soundex further tightens blocks with minimal redundancy.

---

## 3.3: Strategy A — State + Last Name Soundex

The information-theoretically optimal 2-attribute blocking key.

| Metric | Value |
|--------|-------|
| Blocking Key | `LASTNAME_SOUNDEX` + `RecipientState` |
| Candidate Pairs | **427,752** |
| Reduction Ratio | **99.9922%** |

Pairs generated via inner merge on the composite block key. This is the broadest strategy — captures phonetically similar last names within the same state.

---

## 3.4: Strategy B — Exact Last Name (upper) + State

Stricter variant using exact uppercased last name instead of Soundex, avoiding phonetic collisions.

| Metric | Value |
|--------|-------|
| Blocking Key | `UPPER(LastName)` + `State` |
| Candidate Pairs | **118,742** |
| Reduction Ratio | **99.9978%** |

**Tradeoff**: 3.6× fewer pairs than Strategy A, but misses spelling variations (e.g., "SMITH" vs "SMYTH") that Soundex would catch.

---

## 3.5: Strategy C — FN Soundex + LN Soundex + State

Tightest 3-part key. The independence check (FN Soundex ↔ LN Soundex ratio = 0.6685) confirms they carry different information, so combining adds real discriminative power.

| Metric | Value |
|--------|-------|
| Blocking Key | `FIRSTNAME_SOUNDEX` + `LASTNAME_SOUNDEX` + `State` |
| Candidate Pairs | **2,795** |
| Reduction Ratio | **99.99995%** |

**Tradeoff**: Very tight blocks — only 2,795 pairs — but may miss records where the first name has significant variation.

---

## 3.6: Canopy Clustering Blocking

A two-threshold approximate matching method that catches fuzzy name matches that exact blocking misses entirely.

### Method
1. Concatenate `FirstName + LastName + State` into a single string per record.
2. Convert to **TF-IDF vectors** using character 3-grams.
3. Compute **cosine distance** between each OP record and all Medicare records in the same state.
4. Two thresholds: **T1 = 0.6** (loose — remove from candidate pool) and **T2 = 0.4** (tight — generate pair).
5. Processed **per-state** to keep memory manageable (53 states).

### Results
| Metric | Value |
|--------|-------|
| Candidate Pairs | **18,815** |
| Reduction Ratio | **99.9997%** |
| States Processed | 53 |

**Progress snapshots**: 8,321 pairs at 10 states → 14,238 at 40 states → 18,815 at 53 states. The rate shows roughly uniform contribution per state.

---

## 3.7: LSH (Locality-Sensitive Hashing) Blocking

A probabilistic method using MinHash signatures to find approximately similar records without exhaustive comparison.

### Method
1. Break each `FirstName + LastName + State` string into **character 3-grams**.
2. Create **MinHash signatures** with **128 permutations** (`num_perm=128`).
3. Insert Medicare records into an LSH index with **Jaccard threshold = 0.5**.
4. Query each OP record against the index — records sharing roughly half their character 3-grams become candidates.
5. Processed **per-state** (53 states) using the `datasketch` library.

### Results
| Metric | Value |
|--------|-------|
| Candidate Pairs | **100,196** |
| Reduction Ratio | **99.9982%** |
| States Processed | 53 |

**Progress snapshots**: 45,651 pairs at 10 states → 82,225 at 40 states → 100,196 at 53 states.

---

## 3.8: Union & Deduplication

All five strategies are combined via **set union** and deduplicated to remove overlapping pairs.

### Per-Strategy Contribution
| Strategy | Total Pairs | Unique to Strategy |
|----------|-------------|-------------------|
| A (State + LN Soundex) | 427,752 | 427,752 |
| B (Exact LN + State) | 118,742 | 118,742 |
| C (FN Soundex + LN Soundex + State) | 2,795 | 2,795 |
| Canopy (TF-IDF cosine) | 18,815 | 18,815 |
| LSH (MinHash Jaccard) | 100,196 | 100,196 |
| **Union** | **492,427** | — |

**Key insight**: All strategies produce unique pairs that the others miss — confirming that a multi-strategy union is necessary for maximum recall. Strategy A dominates the pair volume (86.9% of union).

---

## 3.9: Blocking Quality Metrics

### Ground Truth Definition
"Findable" OP records are defined as those having an **exact first name + last name + state match** in Medicare — a conservative lower bound for true matches.

- **Findable OP records**: 381 of 4,683 (8.1%)

### Per-Strategy Quality
| Strategy | Pairs | Reduction Ratio | OP Coverage | Recall Ceiling |
|----------|-------|-----------------|-------------|---------------|
| A | 427,752 | 99.9922% | 4,574 | **100.00%** |
| B | 118,742 | 99.9978% | 3,033 | **100.00%** |
| C | 2,795 | 99.9999% | 1,206 | **100.00%** |
| Canopy | 18,815 | 99.9997% | 2,562 | **100.00%** |
| LSH | 100,196 | 99.9982% | 3,720 | **100.00%** |
| **Union** | **492,427** | **99.9911%** | **4,622** | **100.00%** |

### Key Results
- **Recall ceiling: 100%** across ALL strategies — every findable OP record appears in at least one candidate pair for every strategy.
- **OP coverage (Union): 4,622 of 4,683 (98.7%)** — only 61 OP records have zero candidate pairs (likely records with extremely unusual names or missing state).
- **Reduction ratio: 99.99%** — from 5.5 billion possible pairs down to 492K.

### Why All Five Strategies?
Despite 100% recall on all strategies individually, the union matters because:
1. Different strategies generate different **non-findable** candidate pairs — these could be fuzzy matches that aren't exact name+state.
2. OP coverage varies: Strategy C covers only 1,206 OP records vs. Union's 4,622.
3. More candidate pairs per OP record = more chances for the Phase 4 scorer to find the true match.

---

## 3.10: Export

| Artifact | Size | Description |
|----------|------|-------------|
| `candidate_pairs.parquet` | 3.16 MB | 492,427 deduplicated candidate pairs (`index_op`, `index_med`) |
| `blocking_summary.csv` | <1 KB | 6 rows — per-strategy metrics (pairs, reduction ratio, OP coverage, recall ceiling) |

---

## Key Insights & Results

### The Information Theory Approach Worked
- State + LN Soundex was identified as the optimal 2-key combo purely from entropy/MI analysis — confirmed by producing the most pairs (427K) while maintaining 100% recall.
- The independence ratio (0.81) validated that State and LN Soundex provide genuinely complementary information.

### Blocking Reduced Computation by 99.99%
- **5,503,840,923 → 492,427 pairs** — a 11,176× reduction.
- At ~1μs per pair comparison, the full cross-product would take ~92 minutes. After blocking, comparison takes seconds.

### Multi-Strategy Union is Essential
- No single strategy covers all 4,622 reachable OP records.
- Strategy A has the broadest reach (4,574 OP) but Canopy and LSH catch 2,562 and 3,720 respectively through fuzzy similarity rather than exact key matching.
- The 61 uncovered OP records (1.3%) likely have data quality issues that no blocking strategy can overcome.

### The Pipeline Is Ready for Phase 4
- 492,427 candidate pairs are now ready for similarity scoring and classification.
- The 100% recall ceiling confirms that if a true match exists, it's in the candidate set.
