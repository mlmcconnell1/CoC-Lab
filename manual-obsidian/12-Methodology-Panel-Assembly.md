# Methodology: Panel Assembly

CoC Lab currently supports two panel assembly paths:

- **Imperative path:** `coclab build panel`
- **Recipe path:** `coclab build recipe` (recommended)

Both follow the same conceptual model: align heterogeneous inputs to a CoC-year frame, then join.

## Shared Assembly Pattern

1. Resolve analysis year universe.
2. Resolve dataset/year paths and effective geometries.
3. Resample to target geometry (`identity`, `aggregate`, `allocate`).
4. Join resampled datasets on common keys (typically `geo_id`, `year`).
5. Persist panel and provenance metadata.

## Imperative Panel Characteristics

- Inputs: PIT + ACS, optional yearly ZORI integration
- Uses policy helpers for boundary and ACS vintage alignment
- Writes panel under build-local `data/curated/panel/` when `--build` is used

## Recipe Panel Characteristics

- Uses explicit YAML declarations for datasets/transforms/pipelines
- Planner resolves dataset-year tasks deterministically
- Executor runs `materialize -> resample -> join -> persist`
- Current persist target is canonical `data/curated/panel/...`
- Writes `*.manifest.json` sidecar listing consumed assets

## Quality Signals

Across both paths, users should monitor:

- `coverage_ratio` and related diagnostics fields
- boundary change indicators in panel outputs
- missingness after joins

---

**Previous:** [[11-Methodology-ZORI-Aggregation]] | **Next:** [[13-Bundle-Layout]]
