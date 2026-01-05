# CoC Lab — Phase 3 Specification  
## PIT Ingestion, Panel Assembly, and Modeling Readiness

**Status prerequisite:**  
- Phase 1 (Boundary Infrastructure): complete  
- Phase 2 (Geographic Attribution & Measurement): complete  

**Audience:** Multiple AI agents working in parallel  
**Primary objective:** Introduce **Point-in-Time (PIT) homelessness counts**, align them with boundary and ACS vintages, and construct **analysis-ready CoC × year panels** suitable for downstream modeling.

---

## Phase 3 Goal (Authoritative Statement)

> Phase 3 integrates **observed homelessness outcomes** (PIT counts) with the geographic and demographic measurement infrastructure built in Phases 1–2, producing reproducible, versioned **CoC × year panels** that are ready for regression and Bayesian modeling.

Phase 3 **does not yet implement substantive models** (e.g., Byrne-style regressions or Glynn-style clustering). Its purpose is to make such modeling possible, correct, and defensible.

---

## Scope Boundaries

### Included
- PIT data source discovery and ingestion
- Canonical PIT schema and registry
- Alignment of PIT years with boundary and ACS vintages
- Construction of CoC × year panel datasets
- Exploratory validation and sensitivity checks

### Explicitly Excluded
- Final regression or Bayesian modeling
- Policy interpretation or causal inference
- Forecasting or simulation
- Publication-quality figures

---

## Repository Additions (High-level)

```
coclab/
  pit/
    ingest/
      hud_exchange.py
      csv_archive.py
    registry.py
    qa.py
  panel/
    assemble.py
    policies.py
    diagnostics.py
  cli/
    ingest_pit.py
    build_panel.py
    panel_diagnostics.py
data/
  raw/
    pit/
  curated/
    pit/
    panel/
```

---

## Data Contracts (Non-negotiable Interfaces)

---

### Canonical PIT Schema

Stored as Parquet:
```
data/curated/pit/pit_counts__{pit_year}.parquet
```

Columns:
- `pit_year` *(int)* — Calendar year of PIT count
- `coc_id` *(str)* — Normalized CoC ID (`ST-NNN`)
- `pit_total` *(int)* — Total persons experiencing homelessness
- `pit_sheltered` *(int, nullable)*
- `pit_unsheltered` *(int, nullable)*
- `data_source` *(str)* — e.g., `hud_exchange`
- `source_ref` *(str)* — URL or dataset identifier
- `ingested_at` *(datetime, UTC)*
- `notes` *(str, nullable)* — Data quirks, caveats

---

### PIT Registry Schema

Tracks all ingested PIT vintages.

```
data/curated/pit/pit_registry.parquet
```

Columns:
- `pit_year` *(int, PK)*
- `source`
- `path`
- `row_count`
- `hash_of_file`
- `ingested_at`

---

### Canonical CoC × Year Panel Schema

Stored as Parquet:
```
data/curated/panel/coc_panel__{start_year}_{end_year}.parquet
```

Columns:
- `coc_id`
- `year`
- `pit_total`
- `pit_sheltered`
- `pit_unsheltered`
- `boundary_vintage_used`
- `acs_vintage_used`
- `weighting_method`
- `total_population`
- `adult_population`
- `population_below_poverty`
- `median_household_income`
- `median_gross_rent`
- `coverage_ratio`
- `boundary_changed` *(bool)*
- `source` = `coclab_panel`

---

## Work Packages (Parallel Implementation)

---

## WP-3A: PIT Source Discovery & Download
**Owner:** Agent A  
**Purpose:** Identify and archive authoritative PIT sources.

### Tasks
1. Identify HUD Exchange PIT data sources by year
2. Download CSV/Excel files to `data/raw/pit/{year}/`
3. Record source URLs and metadata
4. Support multiple formats (CSV, XLSX)

### Deliverables
- `coclab/pit/ingest/hud_exchange.py`
- Raw PIT files archived locally

---

## WP-3B: PIT Parsing & Canonicalization
**Owner:** Agent B  
**Purpose:** Convert raw PIT files into canonical schema.

### Tasks
1. Parse PIT files for each year
2. Normalize CoC identifiers (`CO-500`, etc.)
3. Extract total, sheltered, unsheltered counts where available
4. Handle missing or merged CoCs explicitly (no silent fixes)
5. Write curated Parquet outputs

### Deliverables
- `data/curated/pit/pit_counts__{year}.parquet`
- Parsing logic with unit tests

---

## WP-3C: PIT Registry & Provenance
**Owner:** Agent C  
**Purpose:** Track PIT ingestion reproducibly.

### Tasks
1. Implement PIT registry mirroring boundary registry design
2. Compute file hashes
3. Embed provenance metadata in Parquet files
4. Provide registry query helpers

### Deliverables
- `coclab/pit/registry.py`
- `data/curated/pit/pit_registry.parquet`

---

## WP-3D: PIT QA & Validation
**Owner:** Agent D  
**Purpose:** Detect data quality issues early.

### Checks
- Duplicate CoC IDs per year
- Missing CoCs relative to boundary vintages
- Non-integer or negative counts
- Extreme year-over-year changes (flagged, not corrected)

### Deliverables
- `coclab/pit/qa.py`
- CLI-readable QA summaries

---

## WP-3E: Panel Assembly Policies
**Owner:** Agent E  
**Purpose:** Define explicit alignment rules.

### Required Policies
1. **PIT year → boundary vintage**
   - Default: PIT year Y uses boundary vintage Y
2. **PIT year → ACS vintage**
   - Default: ACS vintage Y-1 (per Phase 2 rule)
3. **Weighting method**
   - Area vs population (explicit parameter)

Policies must be **pure functions** and recorded in provenance.

### Deliverables
- `coclab/panel/policies.py`

---

## WP-3F: Panel Assembly Engine
**Owner:** Agent F  
**Purpose:** Build analysis-ready CoC × year panels.

### Tasks
1. Load PIT counts
2. Join boundary-aware ACS measures
3. Apply alignment policies
4. Attach diagnostics and flags
5. Persist panel Parquet

### Deliverables
- `coclab/panel/assemble.py`
- Panel Parquet files

---

## WP-3G: Panel Diagnostics & Sensitivity Checks
**Owner:** Agent G  
**Purpose:** Validate panel integrity before modeling.

### Diagnostics
- Coverage ratio distribution over time
- Boundary change flags by CoC/year
- PIT rate sensitivity to weighting method
- Missingness summaries

### Deliverables
- `coclab/panel/diagnostics.py`
- CLI summaries and CSV exports

---

## WP-3H: CLI Integration
**Owner:** Agent H  
**Purpose:** Make Phase 3 operational.

### Commands
```bash
coclab ingest-pit --year 2024
coclab build-panel --start 2018 --end 2024 --weighting population
coclab panel-diagnostics --panel coc_panel__2018_2024.parquet
```

### Deliverables
- `cli/ingest_pit.py`
- `cli/build_panel.py`
- `cli/panel_diagnostics.py`

---

## Sequencing & Critical Path

### Fully Parallel
- WP-3A, 3B, 3C can proceed independently
- WP-3E can be implemented early

### Dependencies
- WP-3D depends on WP-3B
- WP-3F depends on WP-3B, WP-3E
- WP-3G depends on WP-3F
- WP-3H depends on all

---

## Acceptance Criteria (Phase 3 Complete)

1. PIT counts ingested for multiple years with registry tracking
2. CoC × year panels constructed reproducibly
3. Alignment policies explicit and embedded in provenance
4. Diagnostics identify boundary and weighting sensitivities
5. Panels ready for Phase 4 modeling

---

## Phase 4 Preview (Context Only)

- Replication of Byrne-style regression models
- Replication and extension of Glynn-style nonlinear clustering
- Threshold detection and counterfactual analysis
- Interactive dashboards for results exploration

---

*End of Phase 3 Specification*
