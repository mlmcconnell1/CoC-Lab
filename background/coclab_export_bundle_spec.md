# Spec: `coclab export-bundle` (Analysis-Ready Data Bundle + `MANIFEST.json`)

## Objective

Add a `coclab export-bundle` CLI command that produces a **self-contained, analysis-ready bundle directory** suitable for ingestion into downstream analysis repositories (e.g., DVC-tracked repos). The bundle captures:

- the **panel(s)** required for statistical modeling,
- selected **inputs** (crosswalks, boundaries, ZORI, ACS, PIT, etc.),
- **diagnostics**,
- and a machine-readable **`MANIFEST.json`** that pins exact content by hash and records provenance.

### Non-negotiable behavior
- Every invocation creates a **new export folder**: `export-1`, `export-2`, … under the chosen output root so prior bundles are not overwritten.
- The manifest is created **as part of bundle generation** and includes file hashes and key metadata.

---

## CLI Design

### Command
```bash
coclab export-bundle [OPTIONS]
```

### Required options (recommended)
- `--name TEXT`  
  Logical bundle name, used for metadata and optional directory naming (does not replace `export-n` sequencing).

- `--out-dir PATH` (default: `exports/`)  
  Root directory where `export-n/` folders are created.

### Optional selection options
- `--panel PATH`  
  Explicit panel parquet to include (e.g., CoC×year panel with ZORI fields).  
  If omitted, the command may infer the “latest” panel based on other flags, but explicit is preferred.

- `--include TEXT` (repeatable or comma-separated; default: `panel,manifest,codebook,diagnostics`)  
  Components to include:  
  - `panel` (primary panel parquet(s))  
  - `inputs` (boundaries, crosswalks, raw-ish curated sources required to regenerate)  
  - `derived` (derived intermediate artifacts beyond the panel)  
  - `diagnostics` (diagnostic outputs)  
  - `codebook` (variable descriptions, schema)  
  - `manifest` (always created; this flag controls whether it is also copied to a `provenance/` folder)

- `--boundary-vintage TEXT` (e.g., `2025`)
- `--tracts-vintage TEXT` (e.g., `2023`)
- `--counties-vintage TEXT` (e.g., `2023`)
- `--acs-vintage TEXT` (e.g., `2019-2023`)
- `--years TEXT` (e.g., `2011-2024`)  
  Used for validation and for selecting artifacts when inference is used.

### Content control options
- `--copy-mode TEXT` (default: `copy`)  
  One of:  
  - `copy` (physical copy into bundle; safest)  
  - `hardlink` (fast but same filesystem)  
  - `symlink` (portable only if paths preserved; generally not recommended)

- `--compress` (flag; default off)  
  If on, also produce a tarball `export-n.tar.gz` alongside the folder.

- `--force` (flag; default off)  
  Regenerate bundle even if an identical manifest already exists (still creates a new export-n folder).

### Output
Creates a directory:
```
{out-dir}/export-{n}/
```

---

## Bundle Layout

**Required:**
```
export-{n}/
  MANIFEST.json
  README.md
  data/
    panels/
      <panel files>
```

**If `--include inputs`:**
```
  data/inputs/
    boundaries/
    xwalks/
    pit/
    rents/
    acs/
```

**If `--include diagnostics`:**
```
  diagnostics/
    <diagnostic artifacts>
```

**If `--include codebook`:**
```
  codebook/
    schema.md
    variables.csv
```

**Optional:**
```
  provenance/
    parquet_metadata_dump.json
```

### Naming conventions inside bundle
- Keep original filenames to avoid ambiguity.
- Add a short top-level README that describes:
  - what’s included,
  - how to use it in analysis repos (including DVC suggestions),
  - high-level parameterization (vintages, years, thresholds).

---

## Functional Requirements

### FR1: Export folder sequencing (`export-n`)
- In `out-dir`, find existing folders matching `export-<int>`.
- Choose `n = max(existing) + 1` (or 1 if none).
- Create `export-n/` and write all outputs there.
- This happens **even if** the content would be identical to a previous export.

### FR2: Deterministic file hashing
- Compute `sha256` for each included file **after** it is placed in the bundle directory.
- Hashing must read bytes from disk in binary mode.
- Store per-file:
  - relative path
  - sha256
  - size in bytes
  - modified time (optional)

### FR3: `MANIFEST.json` generation
- `MANIFEST.json` is created at bundle root.
- It must include:
  - bundle metadata (name, created_at, coclab_version, git commit if available)
  - declared parameters (boundary/tract/county/acs vintages; years)
  - list of included artifacts with hashes and roles
  - schema versions for key artifacts (panel schema version, zori schema version if present)
  - attribution snippets for external sources (e.g., Zillow attribution text) when relevant

### FR4: Validation checks before export completes
- Ensure panel exists and is readable.
- Ensure required columns exist (if `--panel` points to a panel requiring ZORI, validate expected columns such as `rent_to_income`, `zori_is_eligible`, etc.).
- Ensure that the panel’s vintages are compatible with the flags if those flags are provided (best-effort: check metadata columns).
- Fail with non-zero exit code if validation fails.

### FR5: Optional content inference (best-effort)
If explicit paths aren’t provided:
- Infer “latest” artifacts from standard curated directories using:
  - vintages flags
  - most recent modified time
  - filename patterns
- **Inference must be transparent**:
  - record inferred selections in the manifest
  - print to console what was inferred

---

## `MANIFEST.json` Schema (Proposed)

### Top-level fields
```json
{
  "bundle_name": "gbc_replication",
  "export_id": "export-7",
  "created_at_utc": "2026-01-07T21:15:03Z",
  "coclab": {
    "version": "0.9.3",
    "git_commit": "abc1234",
    "python": "3.11.6"
  },
  "parameters": {
    "boundary_vintage": "2025",
    "tracts_vintage": "2023",
    "counties_vintage": "2023",
    "acs_vintage": "2019-2023",
    "years": "2011-2024",
    "copy_mode": "copy"
  },
  "artifacts": [
    {
      "role": "panel",
      "path": "data/panels/coc_panel__2011_2024__zori.parquet",
      "sha256": "...",
      "bytes": 12345678,
      "rows": 99999,
      "columns": 87,
      "key_columns": ["coc_id","year","pit_total","population","zori_is_eligible","rent_to_income"]
    }
  ],
  "sources": [
    {
      "name": "Zillow Economic Research",
      "metric": "ZORI",
      "attribution": "<full Zillow attribution text>",
      "license_notes": "Public use with required attribution (see Terms of Use)."
    }
  ],
  "notes": "Bundle exported for GBC-like replication."
}
```

### Artifact entry fields (minimum)
- `role`: `panel` | `input` | `derived` | `diagnostic` | `codebook`
- `path`: relative bundle path
- `sha256`
- `bytes`
- `rows` (if parquet/csv readable)
- `columns` and optionally `key_columns`
- `provenance` (optional object copied from parquet metadata or provenance column)

---

## Implementation Plan (Parallel Agents)

### Agent A: CLI + Export Orchestration
**Deliverables**
- Typer command `export-bundle`
- Folder sequencing logic (`export-n`)
- Copy/link logic based on `--copy-mode`
- Console reporting

**Interfaces expected**
- `bundle_manifest.build_manifest(bundle_root, selections, params) -> dict`
- `bundle_copy.copy_artifacts(selections, bundle_root, copy_mode) -> list[ArtifactRecord]`

### Agent B: Artifact Selection & Inference
**Deliverables**
- Logic that maps flags to concrete file paths:
  - panel selection (explicit or inferred)
  - related inputs (boundaries, crosswalks, rents, ACS, PIT)
  - diagnostics and codebook
- Standard filename patterns and directory conventions
- Returns a structured “selection plan”:
  - list of (source_path, dest_relative_path, role)

**Acceptance**
- Inference is deterministic given the filesystem state and flags.
- Prints and records what was selected.

### Agent C: Manifest Generation (`MANIFEST.json`)
**Deliverables**
- `MANIFEST.json` schema implementation and writer
- File hashing function
- Metadata extractors:
  - parquet row counts, column lists
  - selected key columns
  - optional provenance extraction from parquet metadata or a column
- Include Zillow attribution when ZORI artifacts are present.

**Acceptance**
- Manifest matches included files and their hashes.
- Manifest records inferred vs explicit selections.
- Manifest can be parsed and validated by a simple checker.

### Agent D: Validators
**Deliverables**
- Validation functions run before finalizing export:
  - file existence/readability
  - panel schema expectations
  - vintage compatibility (best-effort)
- Error messages must be actionable.

**Acceptance**
- Fails fast with clear messaging.
- No partial bundles left behind unless `--force-partial` is added (not required for v1).

### Agent E: Codebook Generation (Optional but recommended)
**Deliverables**
- `codebook/schema.md`: brief schema and interpretation notes
- `codebook/variables.csv`: name, type, source, description
- Reuse existing schema docs if available; otherwise generate from panel columns and known mappings.

**Acceptance**
- Codebook is stable and useful for analysis repo consumers.

### Agent F: Tests
**Deliverables**
- Unit tests for:
  - export-n sequencing
  - copy/link behavior (mock filesystem)
  - hashing correctness
  - manifest creation
- Integration test:
  - create a tiny fake curated directory structure and a fake panel parquet
  - run export-bundle and assert layout + manifest contents

---

## Suggested Console Output (Minimum)

On success:
- export directory path
- number of files exported per role
- manifest path + summary (total bytes, sha256 of manifest)

Example:
```
Export created: exports/export-3/
  panel: 1 file
  inputs: 7 files
  diagnostics: 3 files
MANIFEST.json: exports/export-3/MANIFEST.json
Total: 11 files, 482 MB
```

---

## Error Handling & Exit Codes
- `0` success
- `2` validation failure (missing panel, incompatible vintages, unreadable files)
- `3` filesystem failure (cannot create export directory, copy failure)
- `4` manifest failure (hashing/metadata extraction failure)

---

## Analysis Repo Incorporation (Documentation Snippet for README)

Include in bundle `README.md`:

```markdown
## Using this bundle in an analysis repository

Recommended: track this folder with DVC.

```bash
dvc init
dvc add data/bundles/export-3
git add data/bundles/export-3.dvc .gitignore
git commit -m "Pin CoC Lab export bundle export-3"
```

All analysis code should refer only to paths inside this bundle.
The `MANIFEST.json` file pins exact file hashes for provenance.
```

---

## Open Decisions (Default Choices)
- Default `--copy-mode`: `copy`
- Whether to always include inputs: recommended default is **panel + manifest + codebook + diagnostics**, with `--include inputs` required explicitly to avoid large exports by surprise.
- Whether to include a compressed archive by default: recommended **off**.

---

## Definition of Done
- `coclab export-bundle` creates `export-n/` reliably without overwriting.
- Bundle contains the requested components and always includes a correct `MANIFEST.json`.
- Manifest includes hashes and minimum provenance metadata.
- Command is test-covered and documented in the manual.
