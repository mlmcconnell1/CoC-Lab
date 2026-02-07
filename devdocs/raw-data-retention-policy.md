# Raw Data Retention Policy

Status: Implemented
Version: 1.1
Applies to: All ingestion pipelines that fetch external data

## Purpose

Define a consistent, reproducible policy for handling raw external inputs across all data sources (file downloads and APIs).

## Policy Statement

All external ingests MUST persist a local raw snapshot by default.

The current distinction between "file-based source" and "API source" is not sufficient as a retention policy. Reproducibility requirements apply to both.

## Requirements

1. Every ingest writes a raw snapshot under `data/raw/<source_type>/...` before or alongside curated output generation.
2. Every ingest computes and records `raw_sha256` and `file_size` from the persisted raw snapshot.
3. Curated outputs must be derivable from the retained raw snapshot plus code/versioned transforms.
4. `source_registry.local_path` should reference the persisted raw snapshot path (not only curated output paths).
5. Raw snapshots should be immutable after write. Re-runs create a new dated snapshot or replace only with `--force` and an updated hash entry.

## What Counts as a Raw Snapshot

### File-based sources

Persist the original downloaded artifact(s), such as:
- CSV/XLSX/XLSB/ZIP/GDB/SHP payloads

### API-based sources

Persist a canonical API snapshot artifact, for example:
- `response.ndjson` or `response.jsonl` with deterministic ordering
- `request.json` capturing URL, params, headers used for retrieval
- `manifest.json` capturing pagination, retrieved timestamp, row/feature counts, and content hash

Canonicalization must be deterministic so equal upstream data produces equal hashes.

## Storage Conventions

1. Root path: `data/raw/<source_type>/`
2. Include temporal identity in path or filename (date and/or vintage/year).
3. Keep source-specific naming stable and machine-parseable.

Examples:
- `data/raw/pit/2024/2007-2024-PIT-Counts-by-CoC.xlsb`
- `data/raw/zori/zori__county__2026-02-07.csv`
- `data/raw/hud_opendata/2026-02-07/response.ndjson`
- `data/raw/hud_opendata/2026-02-07/manifest.json`

## Exceptions

Exceptions are allowed only when raw persistence is impractical (for example licensing, legal restrictions, or extreme storage constraints).

Exception requirements:
1. Must be explicitly documented in code and docs for that ingest path.
2. Must still record reproducibility metadata (request parameters, source URL, timestamps, hashes, counts).
3. Must include a justification and owner in the exception note.

Implicit exceptions based only on source type are not allowed.

## Operational Guidance

1. Prefer writing raw snapshots first, then parsing/normalizing.
2. If temporary directories are used for intermediate extraction, copy final raw snapshot artifacts into `data/raw/...` before cleanup.
3. Validation and QA should run against parsed data derived from the persisted raw snapshot.

## Implementation Status

All ingest modules now comply with this policy via `coclab.raw_snapshot`:

| Module | Source Type | Snapshot Format | Raw Path |
|--------|-----------|----------------|----------|
| `hud_opendata_arcgis` | API | `response.ndjson` + manifest | `data/raw/hud_opendata/<date>/` |
| `tiger_tracts` | File | Per-state ZIP | `data/raw/census/<year>/tracts/` |
| `tiger_counties` | File | National ZIP | `data/raw/census/<year>/counties/` |
| `nhgis/ingest` (tracts & counties) | File | NHGIS ZIP | `data/raw/nhgis/<year>/<geo>/` |
| `acs/ingest/tract_population` | API | `response.ndjson` + manifest | `data/raw/acs_tract/<snapshot>/` |
| `rents/weights` | API | `response.ndjson` + manifest | `data/raw/acs_county/<snapshot>/` |
| `census/ingest/tract_relationship` | File | Downloaded text file | `data/raw/census/tract_relationship/` |

`source_registry.local_path` points to the raw artifact in all cases.
Curated output paths are stored in `metadata["curated_path"]`.

Compliance is enforced by `tests/test_retention_compliance.py`.

## Compliance Checklist for New Ingesters

1. Writes raw snapshot to `data/raw/<source_type>/...`
2. Computes hash/size from persisted raw artifact
3. Registers source with raw snapshot `local_path`
4. Produces curated outputs from retained raw snapshot
5. Includes tests for raw retention and reproducibility metadata

