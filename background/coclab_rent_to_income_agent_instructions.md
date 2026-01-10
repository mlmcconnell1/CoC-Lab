# Instructions for Agents: Add `rent_to_income` to the CoC Lab Analysis Panel

## Purpose

Implement a derived affordability predictor:

```
rent_to_income = zori_coc / (median_household_income / 12.0)
```

This variable **must live in the analysis panel**, not in raw ZORI outputs and not in ACS measures. It is a *cross-domain, model-facing construct* and should only be created once all inputs are aligned and eligibility rules are applied.

These instructions are written to allow **parallel work by multiple agents** with clean interfaces and minimal coordination.

---

## Architectural Rule (Non‑Negotiable)

- ❌ Do **not** add `rent_to_income` to:
  - `data/curated/zori/`
  - `data/curated/measures/`
- ✅ Add `rent_to_income` **only** to:
  - `data/curated/panels/` (the output of `coclab build-panel`, or an adjacent derive step)

This preserves CoC Lab’s separation of concerns:
- *Ingest* → raw measures
- *Aggregate* → domain-specific metrics
- *Panel* → analysis-ready predictors

---

## High-Level Outcome

After this work, a user should be able to run:

```bash
coclab build-panel --start 2018 --end 2024 --include-zori
```

and receive a CoC × year panel that includes:

- `zori_coc`
- `zori_coverage_ratio`
- `zori_is_eligible`
- `rent_to_income`

with clear provenance and diagnostics.

---

# Parallel Work Plan

## Agent A — Panel Integration (Core Logic)

### Responsibility
Extend the panel assembly step to join yearly ZORI and compute `rent_to_income`.

### Tasks
1. Locate the code path used by `coclab build-panel` that assembles the final CoC × year panel.
2. Load yearly ZORI data:
   - Prefer explicit input via `--zori-yearly-path`
   - Otherwise infer the latest compatible artifact
3. Join ZORI to the panel on:
   - `coc_id`
   - `year`
4. Compute the derived column:
   ```python
   panel["rent_to_income"] = (
       panel["zori_coc"] / (panel["median_household_income"] / 12.0)
   )
   ```
5. Guardrails:
   - If `zori_coc` is null → `rent_to_income = null`
   - If income is null or zero → `rent_to_income = null`
6. Ensure the panel output remains under:
   ```
   data/curated/panels/
   ```

### Acceptance Criteria
- Panel builds successfully with and without ZORI enabled
- `rent_to_income` appears only when ZORI is included
- Existing downstream consumers are not broken

---

## Agent B — CLI Surface Area

### Responsibility
Expose ZORI integration cleanly through the CLI.

### Tasks
1. Extend `coclab build-panel` with:
   - `--include-zori / --no-include-zori` (default: off)
   - `--zori-yearly-path PATH` (optional override)
   - `--zori-min-coverage FLOAT` (default: `0.90`)
2. Validate inputs:
   - If `--include-zori` and no ZORI data is available → exit with clear error
3. Update `--help` output to document:
   - ZORI inclusion behavior
   - Coverage threshold semantics
4. Console summary should report:
   - Number of CoCs with ZORI
   - Number eligible under coverage threshold
   - Number with computed `rent_to_income`

### Acceptance Criteria
- CLI help clearly documents new flags
- Default behavior (no ZORI) is unchanged
- Errors are explicit and actionable

---

## Agent C — Eligibility Rules & Provenance

### Responsibility
Define and enforce the “ZORI‑eligible analysis universe” and record provenance.

### Eligibility Logic
A CoC-year is **ZORI-eligible** if:
- `coverage_ratio >= zori_min_coverage` (default `0.90`)

Notes:
- High dominance is **not** a hard exclusion; it should generate warnings only.
- CoCs with zero coverage must not be imputed.

### Tasks
1. Add boolean column:
   - `zori_is_eligible`
2. For ineligible rows:
   - Set `zori_coc = null`
   - Set `rent_to_income = null`
3. Add provenance fields (columns or parquet metadata):
   - `rent_metric = "ZORI"`
   - `rent_alignment = "pit_january"` (or actual method used)
   - `zori_min_coverage`
4. Optionally add:
   - `zori_excluded_reason` (`missing`, `low_coverage`, etc.)

### Acceptance Criteria
- Eligibility logic is transparent and reproducible
- No silent exclusions or imputations
- Panel explicitly encodes eligibility state

---

## Agent D — Tests

### Responsibility
Ensure correctness and prevent regressions.

### Unit Tests
- `rent_to_income` math:
  - Income = 60,000; ZORI = 1,500 → ratio = 0.30
- Null handling:
  - Null ZORI or zero income → null ratio
- Eligibility logic:
  - coverage < threshold → ineligible, null ratio

### Integration Test
- Build a minimal synthetic panel + ZORI yearly fixture
- Run panel assembly with `--include-zori`
- Assert:
  - Output file exists
  - Expected columns present
  - Counts of eligible/ineligible rows match expectations

### Regression Test
- Run `build-panel` without ZORI flags
- Assert schema and row counts are unchanged from baseline

---

# Required Columns in Final Panel (When ZORI Enabled)

| Column | Description |
|------|-------------|
| `zori_coc` | CoC-level ZORI (yearly) |
| `zori_coverage_ratio` | Coverage of base geography weights |
| `zori_is_eligible` | Boolean eligibility flag |
| `rent_to_income` | ZORI divided by monthly median income |

---

# Provenance Requirements

Each panel artifact including ZORI must record:

- ZORI aggregation method (e.g., `pit_january`)
- Coverage threshold used
- Boundary vintage
- ACS income vintage
- ZORI source attribution (Zillow Economic Research)

This can be stored as:
- Parquet metadata, and/or
- Explicit provenance columns

---

# Common Pitfalls (Avoid These)

- ❌ Computing affordability inside `rents/`
- ❌ Mixing income logic into ACS measures
- ❌ Imputing ZORI for rural / PR CoCs
- ❌ Silent changes to January vs annual alignment

---

## Optional Extension (Future)

If a two-step workflow is desired later:

```bash
coclab derive-affordability --panel <panel.parquet> --zori <zori_yearly.parquet>
```

This is **not required** for the current phase.

---

## Summary

- `rent_to_income` is a **panel-only derived predictor**
- Eligibility rules must be explicit and encoded
- Provenance matters as much as the math
- This work unlocks the next analytic phase with minimal technical debt
