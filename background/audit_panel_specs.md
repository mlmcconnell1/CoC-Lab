# Audit Panel Specifications for the Glynn-Fox Claims Audit

## Purpose

This document defines the panel contracts for the Glynn-Fox claims audit
described in `devdocs/claims_audit_plan.md`.

The goal is not to recreate the paper's original historical Zillow panel.
The goal is to define a small number of frozen, auditable panels that support
claim-level evaluation under the repository's current data pipeline, validation
rules, and diagnostics standards.

These specifications are intended to be:

- human-readable enough to review before runs;
- machine-implementable without ad hoc interpretation;
- strict enough to prevent silent panel shrinkage or undocumented filtering.

## Scope

This document covers:

- the raw audit panel contract;
- the modeling-ready panel contract;
- workload-specific panel variants for the claims audit;
- required panel metadata and manifests;
- structural acceptance criteria before inference begins.

This document does not define:

- sampler settings;
- posterior diagnostics thresholds beyond the structural gate;
- claim-classification rules after fitting.

Those remain governed by `devdocs/claims_audit_plan.md` and run-specific
diagnostics outputs.

## Design Principles

All audit panels should follow these principles:

1. Freeze the unit set and year window before running inference.
2. Prefer metros as the primary claims-audit unit whenever a metro-capable
   panel is available.
3. Use CoC panels as a robustness layer, not as the default claims matrix.
4. Make all exclusions explicit and reproducible.
5. Use deterministic file paths and stable schemas so panels can be referenced
   from manifests and downstream reports.
6. Treat missing-data handling as part of the panel definition, not as an
   incidental preprocessing detail.

## Raw Audit Panel Contract

The raw audit panel is the canonical long-format annual input table for a given
workload. It must contain exactly one row per `(geo_id, year)`.

### Required columns

The raw audit panel must contain at least these columns:

- `geo_id`
- `year`
- `pit_total`
- `pit_sheltered`
- `pit_unsheltered`
- `total_population`
- `median_household_income`
- `zori`

These match the repository's current required panel columns in
`src/coc_glynnfox/validate.py`.

### Column semantics

- `geo_id`: stable unit identifier for the audit unit. For metro panels this
  must be the frozen metro identifier used throughout the workload. For CoC
  panels this is the CoC identifier.
- `year`: integer annual time index.
- `pit_total`: observed PIT homeless count used as the modeled count `C_it`.
- `pit_sheltered`: sheltered component of the PIT count used in count-accuracy
  prior construction.
- `pit_unsheltered`: unsheltered component of the PIT count used in
  count-accuracy prior construction.
- `total_population`: observed total population used as `N_it`.
- `median_household_income`: retained for diagnostics and context; not required
  by the core Glynn-Fox state-space model.
- `zori`: rent proxy series used to derive year-over-year rent change.

### Structural requirements

Every raw audit panel must satisfy all of the following:

1. Exactly one row per `(geo_id, year)`.
2. Sorted by `geo_id`, then `year`.
3. A frozen annual modeling window with explicit `start_year` and `end_year`.
4. Contiguous annual coverage for every included `geo_id` across the declared
   audit window.
5. Numeric and nonnegative values for:
   - `pit_total`
   - `pit_sheltered`
   - `pit_unsheltered`
   - `total_population`
   - `zori`
6. PIT identity must hold for every row:
   - `pit_total = pit_sheltered + pit_unsheltered`
7. Population bound must hold for every row:
   - `pit_total <= total_population`
8. Unit membership must be fixed by an explicit selection rule documented in
   the workload manifest.
9. Missing values in model-required fields must be handled by a declared policy
   before modeling. Silent dropping is not allowed.

### Required missing-value policy

Each audit panel must declare one of these missing-value policies:

- `drop`: drop rows with missing values in model-required columns, then drop
  any `geo_id` that no longer has full year coverage.
- `flag`: retain rows and add a `_has_missing` indicator, but the panel is not
  considered modeling-ready until a later documented step resolves all flagged
  rows.

For claims-audit workloads, `drop` is preferred only when the resulting unit
loss is explicitly reported in the manifest and remains substantively
defensible. If dropping materially changes the intended workload, the panel
should be rebuilt rather than silently narrowed.

## Modeling-Ready Panel Contract

The modeling-ready panel is the derived table used to prepare arrays for the
state-space model. It must be balanced across all included units and years.

### Required columns

The modeling-ready panel must contain these columns:

- `geo_id`
- `year`
- `pit_sheltered`
- `pit_unsheltered`
- `C_it`
- `N_it`
- `ZRI_it`
- `d_zori`
- `expected_pi`
- `a_it`
- `b_it`

### Derived-field definitions

- `C_it = pit_total`
- `N_it = total_population`
- `ZRI_it = zori`
- `d_zori`: within-unit year-over-year percent change in `zori`
- `expected_pi`: expected count accuracy for the unit-year under the chosen
  count-accuracy trajectory
- `a_it`, `b_it`: beta prior hyperparameters derived from `expected_pi` and the
  configured variance

### Modeling-ready requirements

Every modeling-ready panel must satisfy all of the following:

1. Balanced coverage across the full workload year window for every `geo_id`.
2. No missing values in:
   - `C_it`
   - `N_it`
   - `ZRI_it`
   - `a_it`
   - `b_it`
3. `d_zori` must be computable from the retained `ZRI_it` panel.
4. The first year for each `geo_id` may have `d_zori = NaN` in the table, but
   model preparation must deterministically convert that first-year value to
   `0.0`.
5. The final table must pivot cleanly into arrays of shape
   `(n_geos, n_years)` without introducing missing cells.

## Workload-Specific Panel Variants

The claims audit should use a small number of frozen workload panels.

### Workload A: Broad Metro Audit Panel

Purpose:
Primary claims-audit workload.

Unit type:
Metro.

Panel requirements:

- Broad current metro panel aligned to a single fixed year window.
- Balanced annual coverage after all panel filtering.
- Clear and reproducible metro selection rule.
- No cherry-picked exclusions beyond documented data-quality or panel-validity
  criteria.
- Large enough to exercise hierarchical borrowing across units.

Recommended manifest fields:

- `unit_type = "metro"`
- `selection_rule`
- `excluded_units`
- `exclusion_reasons`
- `n_units_pre_filter`
- `n_units_model_ready`

Primary uses:

- trend claim
- rent association claim
- latent total claim

### Workload B: Headline Metro Panel

Purpose:
Audit the paper's headline narrative claims for emphasized metros.

Unit type:
Metro.

Panel requirements:

- Include metros corresponding as closely as possible to New York,
  Los Angeles, Washington, D.C., Seattle, and optionally San Francisco and
  Boston.
- Use a frozen metro mapping document or machine-readable mapping file when a
  metro must be assembled from lower-level units.
- Match the main audit year window when feasible.
- Record any deviations from Workload A's rent proxy or aggregation logic.

Primary uses:

- high-concern metro claim
- headline trend comparisons
- rent-shock interpretation for emphasized places

### Workload C: CoC Robustness Panel

Purpose:
Robustness and external-validity layer for the metro-level claims.

Unit type:
CoC.

Panel requirements:

- Broad CoC panel covering the same or clearly aligned year window as the metro
  audit.
- Balanced annual coverage after missing-value handling.
- No claim that CoC and metro estimates are numerically interchangeable.
- Where a metro conclusion depends on constituent CoCs, the mapping between the
  metro and its CoCs must be documented.

Primary uses:

- trend-claim robustness
- rent-association robustness
- within-metro heterogeneity checks

### Workload D: Cross-Method Reuse Panel

Purpose:
Separate data-driven instability from backend-driven instability.

Unit type:
Same as the reused source workload.

Panel requirements:

- Must be exactly the same panel as Workload A, B, or C.
- No unit or year differences are allowed between backends being compared.
- Any preprocessing differences across methods must be documented and justified.

Primary uses:

- backend sensitivity comparisons
- top-unit rank stability checks
- sign and magnitude stability for `phi_i` and `phi_bar`

## Required Panel Metadata

Each frozen audit panel must publish a machine-readable manifest with at least
these fields:

- `panel_name`
- `workload_id`
- `unit_type`
- `panel_version`
- `selection_rule`
- `source_panel_path`
- `derived_panel_path`
- `start_year`
- `end_year`
- `missing_policy`
- `balanced_required`
- `rent_proxy`
- `build_timestamp`
- `git_commit`
- `n_rows_raw`
- `n_rows_model_ready`
- `n_units_raw`
- `n_units_model_ready`
- `dropped_units`
- `drop_reasons`

Recommended additional fields:

- `aggregation_logic_ref`
- `run_manifest_path`
- `validation_report_path`
- `notes`

The manifest must be emitted as a single JSON object so downstream tooling can
consume it without text stripping.

## Structural Gate Acceptance Criteria

Before any inference run is considered valid for audit use, the associated panel
must pass the structural gate.

### Required checks

The structural gate must verify:

1. Required columns are present.
2. `(geo_id, year)` is unique.
3. Panel is sorted by `geo_id`, then `year`.
4. Full-year contiguity across the declared audit window.
5. Numeric and nonnegative checks on modeled numeric inputs.
6. PIT identity holds.
7. Population bound holds.
8. Missing-value handling matches the declared policy.
9. The modeling-ready table remains balanced after missing-value handling.
10. All expected artifacts are present:
    - raw panel file
    - modeling-ready panel file
    - validation report
    - panel manifest

### Failure handling

If a panel fails the structural gate:

- do not use it for claim judgments;
- classify the run as structurally invalid;
- record the failure in the manifest or paired validation report;
- either repair the panel definition or create a narrower, newly versioned
  panel with explicit justification.

## Output File Conventions

Each frozen panel should use deterministic paths and filenames. A recommended
layout is:

```text
outputs/audit_panels/<panel_name>/
  raw_panel.parquet
  modeling_input.parquet
  validation_report.json
  panel_manifest.json
```

If multiple versions of the same panel are retained, versioning should appear in
the directory name or manifest field, not only in prose.

## Interpretation Limits

These panel specifications do not eliminate the audit's main external caveat:
the rent series is a current proxy centered on `zori` or related derived inputs,
not the paper's original historical Zillow series.

Accordingly:

- do not claim exact reproduction of the paper's published numerical results;
- do claim that these panels support a structured audit of the paper's
  substantive claims under the closest currently available panel data;
- treat panel-definition caveats as part of the scientific interpretation, not
  as implementation trivia.

## Minimal Acceptance Checklist

A panel is ready for audit use only if the answer to each question is "yes":

1. Is the workload name fixed and versioned?
2. Is the unit-selection rule explicit?
3. Is the year window explicit?
4. Does the raw panel satisfy uniqueness, contiguity, PIT identity, and
   population-bound checks?
5. Is the missing-value policy explicit and reported?
6. Is the modeling-ready table balanced after all filtering?
7. Are the manifest and validation report written to deterministic paths?
8. Can the table be converted into `(n_geos, n_years)` arrays without missing
   cells?

If any answer is "no", the panel is not ready for claims-audit inference.
