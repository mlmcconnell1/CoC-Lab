# Overview

CoC Lab is a data engineering and reproducibility toolkit for building **CoC-centered analysis datasets** from heterogeneous public sources.

Its core design choice is deliberate:

- **Hub geography:** CoC boundaries by vintage (`B{year}`)
- **Spoke datasets:** tract- and county-native inputs (ACS, ZORI, PEP) mapped into the CoC hub via crosswalks
- **Execution style:** explicit build scaffolds and declarative YAML recipes

## What CoC Lab Does

- Ingests boundary, census geometry, PIT, ACS, PEP, and ZORI inputs
- Builds tract↔CoC and county↔CoC crosswalks
- Aggregates source datasets to CoC geography inside named builds
- Assembles CoC-year panels (imperative and recipe-driven paths)
- Writes provenance metadata and recipe manifests for reproducibility
- Exports analysis bundles with a machine-readable `MANIFEST.json`

## Philosophy

### 1. Reproducibility over convenience
A named build pins base assets (especially boundaries) and records aggregate runs. Recipe execution additionally emits consumed-asset manifests.

### 2. Declarative where possible
The recipe system separates:
- **Structural validation:** schema and referential integrity
- **Semantic validation:** adapter compatibility and runtime checks

### 3. Transparent temporal alignment
Vintages are explicit in file names, metadata, and docs. The system avoids hiding lag or mismatch decisions.

### 4. CoC-first analytical intent
The project is optimized for CoC-level inference. County-native and tract-native inputs are transformed into that analysis frame, not vice versa.

## Key Surfaces

- **CLI:** `coclab ...`
- **Recipe execution:** `coclab build recipe --recipe <file.yaml>`
- **Named builds:** `builds/<name>/...`
- **Global curated store:** `data/curated/...`

## Notes on Legacy vs Current Usage

CoC Lab still supports an imperative panel path (`build panel`) used by existing tests and workflows. The long-term direction is recipe-driven composition for multi-dataset panel construction.

---

**Next:** [[02-Installation]]
