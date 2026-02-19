# Phase 6: Large-Scale LSH Benchmark (Supplemental)

## Overview
This supplemental notebook benchmarks Locality-Sensitive Hashing (LSH) blocking at full dataset scale (933K tier-1 NPI records × 1.175M Medicare), sweeping hyperparameters and comparing against exact-NPI matching and traditional blocking from Phase 3.

**Inputs**: Cleaned parquets from Phase 2 (`../artifacts/phase2_preprocessing`)
**Outputs**: `../artifacts/phase6_lsh_benchmark/` — benchmark results, baselines comparison, visualizations

### Key Questions
1. How does LSH runtime scale with `num_perm` and `threshold`?
2. What is the precision/recall tradeoff at different similarity thresholds?
3. Does LSH find matches that exact NPI misses (and vice versa)?
4. What is the optimal configuration for production use?

---

## 4.0–4.1: Setup & Load Full-Scale Datasets

| Dataset | Records |
|---------|---------|
| Full OP dataset | 969,703 |
| Tier-1 NPI | 933,615 |
| Tier-2 fuzzy | 4,683 |
| Unmatchable | 31,405 |
| Medicare backbone | 1,175,281 |
| **Full cross-product** | **5,503,840,923 pairs** |

The full cross-product (5.5 billion pairs) is completely infeasible for brute-force comparison — motivating the need for blocking/LSH.

---

## 4.2: Baseline 1 — Exact NPI Matching

The simplest and fastest approach: inner-join on NPI. Gold standard for tier-1 records.

| Metric | Value |
|--------|-------|
| Unique OP tier-1 NPIs | 933,615 |
| Unique Medicare NPIs | 1,175,281 |
| **NPI overlap (exact matches)** | **542,329** |
| Match rate (of OP tier-1) | **58.1%** |
| **Runtime** | **0.65s** |

Exact NPI is blazingly fast but only works for tier-1 records (those with valid NPIs). It cannot handle the 4,683 tier-2 fuzzy records that lack reliable NPIs.

---

## 4.3: Baseline 2 — Traditional Blocking (Strategy B)

Runs Strategy B (State + Last Name Soundex) on tier-2 fuzzy records — the best traditional blocking strategy from Phase 3.

| Metric | Value |
|--------|-------|
| Tier-2 records | 4,683 |
| Medicare records | 1,175,281 |
| **Candidate pairs (B)** | **427,752** |
| **Reduction ratio** | **99.9922%** |
| Runtime | **0.84s** |
| OP coverage | 4,574 of 4,683 (97.7%) |

Strategy B produces 427,752 candidate pairs in under a second with 99.99% reduction from the full cross-product.

---

## 4.4: LSH Hyperparameter Sweep

Uses `datasketch` library for production-grade MinHash LSH. Builds MinHash signatures from character trigrams of concatenated `(first, last, state)` name strings, then queries within-state LSH indices.

### Configuration
- **Q-gram size**: 3 (trigrams), fixed
- **`num_perm`**: 128 (MinHash signature size)
- **`threshold`**: 0.5 (Jaccard similarity cutoff)
- **Scope**: Within-state only (53 common states)

### Name String Construction
```
name_string = "FIRSTNAME LASTNAME STATE"  (uppercased, stripped)
```
Converted to character trigrams for MinHash hashing.

### Sweep Results (Limited by Compute Constraints)

| num_perm | threshold | Candidate Pairs | Reduction Ratio | PC vs Strategy B | Unique to LSH | Overlap with B | Runtime |
|----------|-----------|----------------|-----------------|-----------------|---------------|----------------|---------|
| 128 | 0.5 | **100,196** | 99.9982% | 8.6% | **63,483** | 36,713 | **1,695.8s** |

- **100,196 candidate pairs** generated — 76.6% fewer than Strategy B's 427,752
- **63,483 pairs** found by LSH but NOT by Strategy B — these are genuinely new candidates
- **36,713 pairs** overlap with Strategy B
- Pairs per second: **59.0** (vs Strategy B's ~500K/s)

---

## 4.5: Optimal Configuration Analysis

| Config | Candidate Pairs | Reduction Ratio | PC vs B | Runtime |
|--------|----------------|-----------------|---------|---------|
| Best LSH (perm=128, thresh=0.5) | 100,196 | 99.9982% | 8.6% | 1,695.8s |
| Strategy B | 427,752 | 99.9922% | 100% | 0.84s |
| Exact NPI | 542,329 | N/A | N/A | 0.65s |

### Efficiency Comparison
| Method | Scope | Matches/Pairs | Runtime |
|--------|-------|---------------|---------|
| Exact NPI | Tier-1 only | 542,329 matches | **0.65s** |
| Strategy B | Tier-2 | 427,752 pairs | **0.84s** |
| Best LSH | Tier-2 | 100,196 pairs | **1,695.8s** |

LSH is **~2,000× slower** than Strategy B for tier-2 blocking, while producing fewer candidate pairs. However, its value is in the **63,483 unique pairs** that Strategy B misses.

---

## 4.6: LSH + NPI Complementarity Analysis

The real value of LSH is finding matches that exact NPI cannot — tier-2 records without NPIs.

### Combined Pipeline Coverage
| Method | Records | % of Total OP |
|--------|---------|--------------|
| Total OP records | 969,703 | 100% |
| Tier-1 NPI matched | 542,329 | **55.9%** |
| Tier-2 B-covered | 4,574 | 0.5% |
| Unmatchable | 31,405 | 3.2% |
| **Overall linkable** | **546,903** | **56.4%** |

The combined pipeline (NPI + traditional blocking) covers 56.4% of all OP records. LSH adds unique pairs within the tier-2 space but doesn't expand the tier-1 coverage.

---

## 4.7: Runtime Scaling Analysis

Models how LSH runtime scales with dataset size and signature length.

### Current Scale
- 4,683 OP × 1,175,281 Med = **5.5B potential pairs**
- LSH runtime at perm=128, thresh=0.5: **1,695.8s** (~28 minutes)

### Projected Full-Scale (14.7M OP Records)
| Config | Projected Runtime |
|--------|------------------|
| perm=128, thresh=0.5 | **88,719 min (~1,479 hours)** |

### Scaling Characteristics
- Full cross-join at full scale: **17.3 trillion pairs** — completely infeasible
- Strategy B: Scales linearly with block sizes — minutes at any scale
- LSH: Scales O(n) per state with constant overhead for hashing, but the constant is large

---

## 4.8: Exported Artifacts

| File | Size | Description |
|------|------|-------------|
| `lsh_benchmark_results.csv` | 0.2 KB | Hyperparameter sweep results (1 config tested) |
| `lsh_vs_baselines.csv` | 0.3 KB | LSH vs exact NPI vs traditional blocking comparison |

---

## Key Insights & Results

### LSH Finds Unique Candidates
- **63,483 pairs** found by LSH but not by Strategy B — demonstrates LSH captures different similarity patterns (character-level trigram overlap vs. phonetic code matching)
- These unique pairs could contain matches missed by traditional Soundex-based blocking

### LSH Is Not Practical at Current Scale
- **1,696 seconds** vs **0.84 seconds** for Strategy B — a 2,019× slowdown
- Projected to ~1,479 hours at full OP scale (14.7M records)
- The trigram-based MinHash approach is computationally expensive per-pair

### Strategy B Remains the Best Tier-2 Approach
- 427,752 pairs in <1 second with 97.7% OP coverage
- Already achieves 99.99% reduction ratio
- Combined with Phase 4's five-path classifier, delivers 98.2% recall

### Recommended Production Configuration
- **Tier-1**: Exact NPI join (0.65s, 542K matches)
- **Tier-2**: Strategy B blocking → five-path classification (0.84s + 9.5s scoring)
- **LSH**: Supplemental only — use for discovery of edge cases, not as primary blocking
