# Temporal Terminology

This document defines shorthand notation and vocabulary for describing the various dates, vintages, and temporal relationships in CoC Lab datasets.

## Core Temporal Concepts

| Concept | Definition |
|---------|------------|
| **Vintage** | A version of a dataset as released/published, not necessarily when the underlying data was collected |
| **Reference Year** | The year a measure purports to describe (e.g., PIT 2023 describes homelessness in January 2023) |
| **Collection Window** | The actual time span of data collection (ACS 2022 vintage = 2018–2022 collection window) |

## Shorthand Notation

Single-letter prefixes identify dataset types:

| Concept | Shorthand | Example | Notes |
|---------|-----------|---------|-------|
| CoC boundary version | **B**{year} | B2025 | The geographic shape definition |
| Census tract geometry | **T**{year} | T2023 | TIGER tract shapes |
| Census county geometry | **C**{year} | C2023 | TIGER county shapes |
| ACS vintage (end year) | **A**{year} | A2022 | Implies 5-year window ending that year |
| PIT count year | **P**{year} | P2024 | The January count year |
| Panel year | **Y**{year} | Y2023 | The "as-of" year for analysis |

### ACS Collection Windows

ACS 5-year estimates have an implicit collection window. The vintage year is the *end* of that window:

| Notation | Collection Window | Release Year (typical) |
|----------|-------------------|------------------------|
| A2022 | 2018–2022 | Late 2023 |
| A2023 | 2019–2023 | Late 2024 |
| A2024 | 2020–2024 | Late 2025 |

## Compound Notation

When describing which vintages were combined in a derived dataset, use `@` for "analyzed using" and `×` for crosswalk joins:

| Notation | Meaning |
|----------|---------|
| **P2024@B2025** | 2024 PIT counts analyzed using 2025 boundaries |
| **A2022@B2025** | ACS 2022 aggregated to 2025 CoC boundaries |
| **A2022@B2025×T2023** | ACS 2022 aggregated to 2025 CoC boundaries via 2023 tract crosswalk |
| **P2020@B2025×T2023** | 2020 PIT re-aligned to 2025 boundaries using 2023 tracts |

### Reading Compound Notation

- The first element is the **source data** being analyzed
- `@B{year}` specifies the **target CoC boundaries**
- `×T{year}` or `×C{year}` specifies the **intermediary geometry** used for spatial joins

## Temporal Mismatch Terminology

These terms describe common scenarios where vintages don't align:

| Term | Definition | Example |
|------|------------|---------|
| **Retrospective alignment** | Applying newer boundaries to older data | Analyzing P2018 using B2025 |
| **Period-faithful** | Using boundaries that were in effect when data was collected | Analyzing P2018 using B2018 |
| **Vintage gap** | Normal lag between data collection and availability | A2022 is latest available for B2025 analysis |
| **Geometry mismatch** | Tract/county geometry differs from boundary vintage | T2020 tracts crossed with B2025 boundaries |

### When Mismatches Matter

- **Retrospective alignment** is necessary for consistent time-series analysis but may misattribute counts if CoC boundaries changed significantly
- **Period-faithful** analysis preserves original reporting relationships but complicates cross-year comparisons
- **Vintage gaps** are unavoidable; ACS data typically lags 2+ years behind boundary releases
- **Geometry mismatches** introduce small interpolation errors at tract boundaries that changed between vintages

## Temporal Flags and Spans

Terms for describing temporal characteristics of panel data:

| Term | Definition | Use Case |
|------|------------|----------|
| **Boundary break** | A CoC's geography changed between consecutive years | Flagging discontinuities in time series |
| **Stable span** | Consecutive years with identical boundaries | Identifying periods safe for trend analysis |
| **Backfilled** | Data re-associated with boundaries published after the original report | Documenting retrospective alignment |

### In Schema

These concepts map to schema fields:

```
COC_PANEL.boundary_changed = True  →  Boundary break from prior year
COC_PANEL.boundary_vintage_used   →  Documents which B was applied
COC_PANEL.acs_vintage_used        →  Documents which A was applied
```

## Usage Examples

### Documentation

> "The 2018–2024 panel uses retrospective alignment (P{year}@B2025×T2023) to enable consistent time-series analysis. Years with boundary breaks are flagged."

### Filenames

The shorthand maps directly to filenames using `@` for "analyzed using" and `x` for crosswalk joins (ASCII-safe version of `×`).

#### Naming Convention

**Pattern:** `{dataset}__{temporal-notation}.parquet`

- The `{dataset}` prefix describes what the file contains
- The `{temporal-notation}` suffix encodes vintages using the shorthand notation
- Double underscores (`__`) separate the dataset name from temporal notation
- Single underscores within the temporal notation separate year ranges

#### Simple Datasets (Single Vintage)

| Shorthand | Filename |
|-----------|----------|
| B2025 | `boundaries__B2025.parquet` |
| T2023 | `tracts__T2023.parquet` |
| C2023 | `counties__C2023.parquet` |
| P2024 | `pit__P2024.parquet` |
| A2023 | `acs_tracts__A2023.parquet` |

#### Derived Datasets (Compound Notation)

| Shorthand | Filename | Description |
|-----------|----------|-------------|
| A2023@B2025 | `measures__A2023@B2025.parquet` | ACS 2023 aggregated to 2025 boundaries |
| A2023@B2025xT2023 | `measures__A2023@B2025xT2023.parquet` | Same, via 2023 tract crosswalk |
| P2024@B2025 | `pit__P2024@B2025.parquet` | PIT 2024 aligned to 2025 boundaries |

#### Crosswalks

Crosswalks join two geometry vintages. The notation shows what is being crossed:

| Shorthand | Filename | Description |
|-----------|----------|-------------|
| B2025xT2023 | `xwalk__B2025xT2023.parquet` | CoC 2025 to tract 2023 crosswalk |
| B2025xC2023 | `xwalk__B2025xC2023.parquet` | CoC 2025 to county 2023 crosswalk |

#### ZORI Files

ZORI files have additional parameters for weighting and time resolution:

| Current Name | New Name | Notation |
|--------------|----------|----------|
| `coc_zori__county__b2025__c2023__acs2019-2023__wrenter_households.parquet` | `zori__A2023@B2025xC2023__wrenter.parquet` | A2023@B2025xC2023 |
| `coc_zori_yearly__...` | `zori_yearly__A2023@B2025xC2023__wrenter.parquet` | Same with yearly suffix |

#### Panel Files

Panel files span multiple years and use a target boundary alignment:

| Current Name | New Name | Description |
|--------------|----------|-------------|
| `coc_panel__2015_2024.parquet` | `panel__Y2015-2024@B2025.parquet` | Panel years 2015-2024 aligned to B2025 |

#### Migration Mapping

| Current Filename Pattern | New Filename Pattern |
|-------------------------|---------------------|
| `coc_boundaries__2025.parquet` | `boundaries__B2025.parquet` |
| `tracts__2023.parquet` | `tracts__T2023.parquet` |
| `counties__2023.parquet` | `counties__C2023.parquet` |
| `pit_counts__2024.parquet` | `pit__P2024.parquet` |
| `coc_measures__2025__2019-2023.parquet` | `measures__A2023@B2025.parquet` |
| `coc_tract_xwalk__2025__2023.parquet` | `xwalk__B2025xT2023.parquet` |
| `tract_population__2019-2023__2023.parquet` | `acs_tracts__A2023xT2023.parquet` |
| `coc_panel__2015_2024.parquet` | `panel__Y2015-2024@B2025.parquet` |

#### Exceptions and Edge Cases

1. **Raw files**: Raw data files retain their original naming from the source (HUD, Census, Zillow).

2. **Registry files**: Registry files (e.g., `pit_registry.parquet`, `source_registry.parquet`) do not contain temporal notation as they are metadata indexes.

3. **Diagnostic files**: Diagnostic outputs may use simplified names with the parent dataset's notation (e.g., `diagnostics__A2023@B2025.csv`).

4. **Export bundles**: Export bundles use sequential numbering (`export-1/`, `export-2/`) and preserve internal file naming.

### Provenance Metadata

```json
{
  "boundary_vintage": "2025",
  "tract_vintage": "2023", 
  "acs_vintage": "2022",
  "notation": "A2022@B2025×T2023"
}
```

---

**Previous:** [[06-Data-Model]] | **Next:** [[08-...]]
