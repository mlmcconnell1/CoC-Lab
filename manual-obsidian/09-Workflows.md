# Workflows

## Recommended Workflow: Recipe-Driven Build

1. Ingest required global assets (`boundaries`, `tiger`, `acs5-tract`, `pit`, `zori`, `pep`).
2. Create a named build and pin base assets.
3. Generate required crosswalks into the build.
4. Aggregate source datasets into build-local curated folders.
5. Run a YAML recipe for deterministic panel construction.
6. Export a bundle for downstream analysis.

Example command sequence:

```bash
# 1) Ingest core sources
coclab ingest boundaries --source hud_exchange --vintage 2025
coclab ingest tiger --year 2023 --type all
coclab ingest acs5-tract --acs 2019-2023 --tracts 2023
coclab ingest pit-vintage --vintage 2024
coclab ingest zori --geography county
coclab ingest pep --series auto

# 2) Build scaffold
coclab build create --name demo --years 2018-2024

# 3) Crosswalks
coclab generate xwalks --build demo --boundary 2025 --tracts 2023 --counties 2023

# 4) Aggregates
coclab aggregate acs --build demo
coclab aggregate zori --build demo --align pit_january
coclab aggregate pep --build demo
coclab aggregate pit --build demo

# 5) Recipe execution
coclab build recipe --recipe recipes/glynn_fox_v1.yaml

# 6) Export bundle
coclab build export --name demo_bundle --build demo
```

## Alternate Workflow: Imperative Panel Build

For existing pipelines/tests that still use the panel assembler directly:

```bash
coclab build panel --build demo --start 2018 --end 2024 --weighting population
```

Use this path when you want the legacy panel contract; use recipe execution when you need explicit multi-step composition and recipe manifests.

## Workflow Principles

- Pin and record boundary assets via named builds.
- Keep heavy transformations build-scoped.
- Treat recipe files as auditable execution plans, not ad-hoc scripts.

---

**Previous:** [[08-Temporal-Terminology]] | **Next:** [[10-Methodology-ACS-Aggregation]]
