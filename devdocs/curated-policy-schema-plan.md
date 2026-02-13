# Curated Data Policy + Recipe Vintage-Set Plan (Option 1)

Status: Mostly Implemented (Feb 2026)
Version: 0.2
Applies to: `data/curated/**`, recipe schema/planner, export selection, and migration utilities

## Goal

Standardize curated artifact naming and directory conventions so:
1. Filenames encode temporal identity deterministically.
2. Recipes can stay terse (range-based) while still selecting exact vintage tuples.
3. Legacy naming remains readable during transition, but new writes are canonical-only.

This plan uses **Option 1 semantics**: vintage selection is based on explicit **tuple sets**.
Authoring remains compact through range rules that expand into tuple sets at compile time.

## Canonical Curated Policy

### 1) Directory policy

Curated data remains flat by artifact class:
- `data/curated/coc_boundaries/`
- `data/curated/tiger/`
- `data/curated/xwalks/`
- `data/curated/acs/`
- `data/curated/measures/`
- `data/curated/zori/`
- `data/curated/pep/`
- `data/curated/pit/`
- `data/curated/panel/`

No nested `data/` trees are allowed under these folders.

### 2) Filename policy

Canonical filenames encode vintage tokens using temporal shorthand:
- Boundary: `coc__B{year}.parquet`
- Tracts/counties: `tracts__T{year}.parquet`, `counties__C{year}.parquet`
- Xwalks: `xwalk__B{b}xT{t}.parquet`, `xwalk__B{b}xC{c}.parquet`
- ACS tract ingest: `acs5_tracts__A{a}xT{t}.parquet`
- Measures: `measures__A{a}@B{b}xT{t}.parquet`
- ZORI aggregated: `zori__A{a}@B{b}xC{c}__w{w}.parquet`
- ZORI yearly: `zori_yearly__A{a}@B{b}xC{c}__w{w}__m{m}.parquet`
- PIT year: `pit__P{p}.parquet`
- PIT vintage: `pit_vintage__P{p}.parquet`
- Panel: `panel__Y{s}-{e}@B{b}.parquet`

Canonical writers must not emit legacy names.

### 3) Transition policy

- Readers may continue fallback support for legacy filenames during migration.
- Export and listing tools should prefer canonical files when duplicates exist.
- A validation command should report:
  - non-canonical filenames
  - duplicate temporal identity across multiple names
  - nested-path violations

## Recipe Vintage-Set Plan (Option 1)

### 1) Semantics

Recipes resolve dataset-year inputs by selecting a single tuple from a vintage set:
- Example tuple axes: `A`, `B`, `T`, `C`, `P`, `Z` (plus optional source/method dims).
- A dataset-year is valid only if exactly one tuple applies.

### 2) Compact authoring (already aligned with current extension)

Use `file_set` segment rules to generate tuple members tersely:
- `years` defines band.
- `geometry.vintage` pins geometry vintage.
- `constants` pins tuple dimensions (for example `tract=2010`).
- `year_offsets` derives dimensions from analysis year (for example `acs_end=-1`).
- `overrides` remain exact path escapes.

Result: small YAML, deterministic tuple expansion.

### 3) Schema additions (next)

Add optional top-level tuple set declarations:

```yaml
vintage_sets:
  acs_measures_default:
    dimensions: [analysis_year, acs_end, boundary, tract]
    rules:
      - years: "2015-2019"
        constants: { tract: 2010 }
        year_offsets: { analysis_year: 0, acs_end: -1, boundary: 0 }
      - years: "2020-2024"
        constants: { tract: 2020 }
        year_offsets: { analysis_year: 0, acs_end: -1, boundary: 0 }
```

Then allow dataset file_sets to reference that set (or inline equivalent) and map template variables.

### 4) Planner requirements -- IMPLEMENTED

Functions in `coclab/recipe/planner.py`:
- `expand_vintage_set(spec)` — expands rules into `{year: {dim: value}}` mapping.
- `resolve_vintage_tuple(name, year, recipe)` — resolves one year from a named set.

Diagnostics for:
- Overlapping rules (multiple tuples for year)
- Missing dimension coverage
- Uncovered year (no rule applies)
- Missing template variables in file_set path expansion

## Enforcement and Migration

### Phase 1: Policy + validation -- DONE
- Curated compliance tests in `tests/test_curated_compliance.py`.
- `coclab validate curated-layout` command in `coclab/cli/validate_curated.py`.
- Policy module at `coclab/curated_policy.py`.

### Phase 2: Canonical writes -- DONE
- All curated writers now use `coclab/naming.py` helpers.
- PEP was the last holdout; `coc_pep_filename()` added and wired in.
- Readers retain legacy fallback during transition.

### Phase 3: Backfill + dedupe -- DONE (tooling)
- Migration utility at `coclab/curated_migrate.py`.
- CLI command: `coclab migrate curated-layout [--apply]`.
- Dry-run mode (default) shows planned renames and conflicts.
- Apply mode renames deterministically.
- Execution on actual repository data is a manual step (coclab-oizg.7).

### Phase 4: Tighten defaults -- PLANNED
- Deprecation warnings for legacy reads.
- Remove legacy fallback in high-confidence modules once migration target met.
- Target: after migration has been executed and validated.

## Acceptance Criteria

1. All new curated writes are canonical by naming policy.
2. Recipe authors can express mixed-vintage multi-year inputs without per-year file lists.
3. Planner resolves one tuple per dataset-year with explicit errors on ambiguity/gaps.
4. Export selection and list commands choose canonical artifacts first and consistently.
5. Validation tooling reports non-canonical files and nested-path anomalies.

## Risks / Open Questions

1. Duration of legacy fallback support and cutover date.
2. Whether top-level `vintage_sets` should be optional sugar over current `file_set` model or a required abstraction.
3. Whether export config should accept tuple filters directly (beyond substring vintage matching).
