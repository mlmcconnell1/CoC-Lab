# Storage Roots Configuration Plan

Status: Planned
Version: 0.1
Applies to: runtime configuration, path resolution, recipe execution, manifests, export CLI

## Goal

Introduce first-class, configurable storage roots so CoC-Lab stops assuming that all artifacts live under the repository working tree.

This design separates three concerns that are currently conflated:

1. `asset_store_root`: reusable CoC-Lab internal assets shared across recipes
2. `output_root`: final products intended for downstream packages
3. `export destination`: explicit replication/share bundles produced on demand

The key intent is to reserve the term "output" for deliverables consumed by downstream packages, while keeping internal ingest/aggregate assets conceptually separate.

## Why This Change Is Needed

The current implementation mixes two different classes of files under `data/curated/`:

- reusable internal assets such as raw snapshots, curated ingests, crosswalks, registries, and aggregated source artifacts;
- recipe-built panels and diagnostics that are better treated as downstream-consumable outputs.

Export bundles are already modeled separately under an explicit export workflow, but the code still assumes project-root-relative artifact paths in many places.

That causes three architectural problems:

1. repo layout is treated as runtime storage policy;
2. manifests and provenance assume a single filesystem root;
3. downstream packages cannot cleanly consume CoC-Lab outputs from a shared location without also inheriting CoC-Lab's internal asset layout.

## Proposed Storage Model

### 1. Asset store

`asset_store_root` is the reusable CoC-Lab asset store.

It contains:

- raw snapshots;
- curated ingests;
- crosswalks;
- registries;
- reusable aggregated artifacts used as inputs to multiple recipes;
- other CoC-Lab-internal persisted assets.

Recommended internal layout beneath `asset_store_root`:

- `raw/`
- `curated/`

Examples:

- `<asset_store_root>/raw/pit/...`
- `<asset_store_root>/curated/acs/...`
- `<asset_store_root>/curated/xwalks/...`

### 2. Outputs

`output_root` is the location for products consumed by downstream packages.

It contains:

- recipe-built panels;
- recipe-built diagnostics;
- future downstream-facing deliverables that are not part of the replication export workflow.

Recommended internal layout beneath `output_root`:

- `panel/`
- `diagnostics/` when a distinct output family is introduced later

For the current recipe system, panel outputs and their sidecars should resolve under `output_root`, not under the curated asset store.

### 3. Export destination

Export remains a separate action and not a root category.

`coclab build recipe-export` should copy manifest-declared assets into a caller-provided destination. The CLI option should be named `--destination`.

This keeps replication bundles distinct from both the internal asset store and downstream-consumable outputs.

## Configuration Surfaces

These roots are runtime configuration, not recipe semantics.

They should be configurable through the following surfaces, in this precedence order:

1. CLI flags
2. environment variables
3. repo-local config file
4. user config file
5. built-in defaults

### CLI flags

- `--asset-store-root`
- `--output-root`

Flags should be added first to commands that read or write persisted assets or outputs. Commands that do not touch the filesystem do not need these options.

### Environment variables

- `COCLAB_ASSET_STORE_ROOT`
- `COCLAB_OUTPUT_ROOT`

### Repo-local config

Path: `<repo>/coclab.yaml`

This file expresses the storage policy for a particular checkout.

Suggested shape:

```yaml
asset_store_root: /path/to/coclab-assets
output_root: /path/to/coclab-outputs
```

### User config

Path: `~/.config/coclab/config.yaml`

This supports local defaults across multiple repositories or workspaces.

### Built-in defaults

For backward compatibility, the built-in defaults should preserve current behavior:

- `asset_store_root = <project_root>/data`
- `output_root = <project_root>/data/curated/panel`

These defaults are transitional and preserve existing tests and developer expectations while the architecture is decoupled.

## Path Semantics

Naming and placement should be separate concerns.

### Naming

Canonical filenames remain defined in `coclab/naming.py`.

### Placement

Placement should be centralized in a new path/config layer rather than hardcoded throughout modules.

Recommended module split:

- `coclab/config.py`: load and merge configuration sources
- `coclab/paths.py`: resolve typed directories and files from the active configuration

Examples of helper functions:

- `asset_store_root()`
- `output_root()`
- `raw_root()`
- `curated_root()`
- `curated_dir(kind)`
- `output_dir(kind)`
- `panel_output_path(...)`

The rest of the codebase should call these helpers instead of constructing `Path("data/...")` directly.

## Manifest And Provenance Changes

Current recipe manifests assume project-root-relative asset paths. That assumption breaks once assets and outputs can live outside the repository tree.

Manifests should move to logical-root-relative records.

### Asset records

Each recorded path should identify both:

- the logical root it is relative to;
- the relative path within that root.

Suggested shape:

```json
{
  "root": "asset_store",
  "path": "curated/xwalks/xwalk__B2025xT2020.parquet"
}
```

### Output records

Produced outputs should be recorded similarly:

```json
{
  "root": "output",
  "path": "panel/panel__Y2015-2024@B2025.parquet"
}
```

### Export resolution

The export implementation should resolve manifest records by logical root:

- `asset_store` paths from `asset_store_root`
- `output` paths from `output_root`

The export bundle should preserve logical-relative paths inside the bundle instead of silently flattening different root spaces into one undifferentiated project-relative namespace.

## Command Behavior

### Recipe build commands

Recipe execution should:

- read dataset and transform inputs from the asset store;
- write built outputs to `output_root`;
- write output manifests next to the built outputs.

### Aggregate and ingest commands

These commands should continue writing to the asset store, because their products are reusable CoC-Lab assets rather than downstream-facing outputs.

### Export command

`coclab build recipe-export` should:

- rename `--output` to `--destination`;
- accept the same root configuration surfaces used elsewhere;
- resolve manifest entries through logical roots rather than project-relative paths.

## Backward Compatibility

The change should be staged.

### Phase 1: introduce central config and path helpers

- add config loading and precedence handling;
- keep current defaults;
- do not change path semantics yet beyond routing writes and reads through helpers.

### Phase 2: migrate core writers and readers

- recipe outputs move to `output_root`;
- ingest, aggregate, crosswalk, registry, and listing commands move to `asset_store_root`;
- legacy direct `data/...` path construction is removed from high-traffic modules.

### Phase 3: migrate manifests and export

- add logical root metadata to manifest records;
- update export to resolve from configured roots;
- support reading legacy manifests during transition if feasible.

### Phase 4: docs and cleanup

- update README and manual storage model language;
- update examples from repo-relative `data/curated/...` assumptions to root-aware examples;
- remove obsolete path assumptions from tests and docs.

## Testing Plan

Add coverage for:

1. config precedence across CLI, env, repo config, user config, and defaults;
2. default behavior preserving current repository-local paths;
3. custom `asset_store_root` for ingest, aggregate, crosswalk, registry, and discovery code;
4. custom `output_root` for recipe panels, diagnostics, and manifests;
5. export bundle creation with `--destination`;
6. manifest resolution for both `asset_store` and `output` records;
7. backward compatibility for legacy path assumptions where transitional support is retained.

## Acceptance Criteria

1. CoC-Lab can run with assets stored outside the repository.
2. Recipe outputs can be written to a downstream-consumable location independent of the asset store.
3. Export bundles remain a separate explicit workflow and use `--destination`.
4. The codebase no longer depends on project-root-relative `data/raw` and `data/curated` assumptions in core path resolution.
5. Filenames remain canonical and deterministic regardless of root placement.

## Non-Goals

This plan does not attempt to:

- change recipe semantics;
- redesign the recipe schema around storage roots;
- flatten raw, curated, outputs, and exports into one generic artifact namespace;
- solve every historical absolute/relative path quirk in one pass without staged compatibility.
