# Executor Panel-Assembly & Persistence Extraction Design

Status: design only, no code changes
Parent bead: `coclab-i6qh` (executor decomposition)
Scope bead: `coclab-wxl9` (panel assembly + persistence path)
Source under discussion: `coclab/recipe/executor.py` (2455 LOC)
Author/agent date: 2026-04-10

---

## 1. Current state

The panel-assembly and persistence path inside `coclab/recipe/executor.py` is
four top-level helpers plus one private dataclass, all living in the same
file as materialize/resample/join orchestration, crosswalk probing, CT
alignment, and the `execute_recipe` entry point. The persistence bundle is
roughly 450 LOC (~1786–2266).

### Line map (from current file)

| Symbol | Lines | LOC | Role |
|---|---|---|---|
| `_canonicalize_panel_for_target` | 1786–1817 | 32 | Stamps target-geometry metadata (`geo_type`, `coc_id`, `metro_id`, `metro_name`, `boundary_vintage_used`, `definition_version_used`) onto a joined panel. |
| `_AssembledPanel` (dataclass) | 1820–1830 | 11 | Struct returned by `_assemble_panel`. Carries `panel`, `frames`, `target`, `target_geo_type`, `boundary_vintage`, `definition_version`, `zori_provenance`. |
| `_resolve_panel_aliases` | 1833–1844 | 12 | Reads `target.panel_policy.column_aliases` and returns a rename map. |
| `_assemble_panel` | 1847–2031 | 185 | Gathers per-year `("__joined__", year)` frames, concatenates, canonicalizes, applies ZORI/ACS1/LAUS policy branches, calls `finalize_panel`, applies cohort selector. |
| `_persist_outputs` | 2034–2213 | 180 | Calls `_assemble_panel`, resolves output file, runs conformance checks, builds provenance dict, writes parquet with embedded metadata, writes manifest sidecar. |
| `_persist_diagnostics` | 2216–2266 | 51 | Calls `_assemble_panel`, runs `generate_diagnostics_report`, writes JSON sidecar. |

Total: ~470 LOC of code that can be moved, with `_execute_plan` in
executor.py being the only orchestrator that calls `_persist_outputs` and
`_persist_diagnostics`.

### Concerns that are tangled inside `_assemble_panel`

1. **Year-frame gathering and concat** — pure orchestration that reads from
   `ctx.intermediates` and concatenates. Not policy-specific.
2. **Canonical stamping** — already factored into
   `_canonicalize_panel_for_target`, called once.
3. **Target-geometry metadata** — derived via `_target_geometry_metadata`.
4. **Policy resolution** — `policy = getattr(target, "panel_policy", None)`.
5. **ZORI branch** (~40 LOC): local import of
   `coclab.panel.zori_eligibility` symbols, rename `zori → zori_coc`, call
   `apply_zori_eligibility`, `compute_rent_to_income`,
   `add_provenance_columns`, drop leak columns (`method`, `geo_count`),
   compute `extra_columns`, and construct a `ZoriProvenance` that needs to
   flow out of assembly into `_persist_outputs`.
6. **ACS1 branch** (~25 LOC): metro-only gating, adds
   `acs1_vintage_used` and `acs_products_used`, fills NA on rows missing
   data.
7. **LAUS branch** (~20 LOC): metro-only gating, adds `laus_vintage_used`,
   ensures all four LAUS measure columns exist.
8. **`finalize_panel` call** — the shared canonical shaping from
   `coclab.panel.finalize`, parameterised by `include_zori`,
   `source_label`, `column_aliases`, `extra_columns`.
9. **Cohort selector** — `_apply_cohort_selector` + `_echo` for progress
   output.

### Concerns that are tangled inside `_persist_outputs`

1. **Output-file resolution and collision detection** against
   `ctx._written_outputs`.
2. **Conformance configuration** — re-derives `persist_target`,
   `persist_policy`, `include_laus`, `acs_products`, `include_zori`,
   translates `measure_columns` through panel aliases. This is effectively
   a second read of the same policy that was already inspected in
   `_assemble_panel`.
3. **Conformance execution** — calls `run_conformance` and prints a
   summary.
4. **Provenance assembly** — calls `_build_provenance`, attaches
   `target_geometry`, `conformance`, and (conditionally) `zori` and
   `zori_summary` from the ZoriProvenance carried on `_AssembledPanel`.
5. **Parquet write with embedded metadata** — direct `pyarrow` usage,
   then `ctx._written_outputs.add(output_file)`.
6. **Manifest sidecar write** — calls `_build_manifest` and
   `write_manifest`.

### Why this is the risky extraction

- The ZORI/ACS1/LAUS branches each read private attributes of `PanelPolicy`
  and each mutates `panel` in place; their ordering matters because
  `finalize_panel` depends on which extra columns exist and which renames
  have already happened.
- Two policy reads (assembly vs persistence) already drifted to compute
  slightly different things (conformance wants measure-column alias
  translation; assembly wants `include_zori` and `source_label`). Extraction
  must not silently re-order or de-duplicate these without a behavioural
  audit.
- `_AssembledPanel.zori_provenance` is the only policy-specific field on
  the boundary today. If ACS1 or LAUS grow summary blocks in provenance
  later (they do not yet, but the pattern is implied), the boundary will
  have to widen; the new design should anticipate that.

---

## 2. Target modules

Create three new modules under `coclab/recipe/` and reduce `executor.py`
to re-exports. Keeping the new modules under `coclab.recipe.*` (not
`coclab.panel.*`) keeps them close to the orchestration context they
serve; `coclab.panel.*` is reserved for schema and shaping primitives
shared with the legacy `build_panel` path.

### 2.1 `coclab/recipe/executor_panel_policies.py`

Responsibility: translate `target.panel_policy` into concrete
mutations on a partially assembled panel DataFrame, and surface any
policy-specific metadata needed downstream.

- Defines a `PanelPolicyApplier` protocol (or abstract base) with:
  - `applies_to(target_geo_type: str, policy: PanelPolicy) -> bool`
  - `apply(panel: pd.DataFrame, *, policy: PanelPolicy, target_geo_type: str) -> PolicyApplication`
- Defines `PolicyApplication` (frozen dataclass) as the per-policy return
  value — see Section 3.
- Implements three concrete appliers:
  - `ZoriPolicyApplier` — houses the local import of `zori_eligibility`,
    the `zori → zori_coc` rename, eligibility, rent_to_income, provenance,
    leak-column drop, and `extra_columns`. Emits a
    `ZoriProvenance`-carrying `PolicyApplication`.
  - `Acs1PolicyApplier` — metro-only gate, `acs1_vintage_used` and
    `acs_products_used` stamping, NA fill behaviour.
  - `LausPolicyApplier` — metro-only gate, `laus_vintage_used`,
    LAUS measure-column backfill.
- Exposes a module-level ordered tuple `DEFAULT_APPLIERS` used by
  `executor_panel.assemble_panel` so policy application order is a
  single source of truth and unit-testable.
- Exposes a small helper `collect_conformance_flags(target, panel) ->
  ConformanceFlags` that reads the same policy surface and returns the
  `include_zori`, `include_laus`, `acs_products`, and alias-translated
  `measure_columns` needed by `_persist_outputs`. This removes the
  second policy-read drift described above.

### 2.2 `coclab/recipe/executor_panel.py`

Responsibility: pure panel assembly from join intermediates. No
parquet, no JSON, no manifest, no conformance.

- Hosts (moved verbatim from executor.py):
  - `_canonicalize_panel_for_target`
  - `_resolve_panel_aliases`
  - `_AssembledPanel` (renamed to public `AssembledPanel` — see below)
  - `_apply_cohort_selector` stays in executor.py for now (it is
    imported by tests) but `assemble_panel` calls it via a re-export.
- Hosts a new public function:

  ```python
  def assemble_panel(
      plan: ExecutionPlan,
      ctx: ExecutionContext,
      *,
      step_kind: str = "persist",
      appliers: Sequence[PanelPolicyApplier] = DEFAULT_APPLIERS,
  ) -> AssembledPanel | StepResult: ...
  ```

  This is the extracted `_assemble_panel`, with the three policy
  branches replaced by a loop over `appliers`. The `finalize_panel`
  call and cohort-selector step remain here because they are not
  policy-specific.

### 2.3 `coclab/recipe/executor_persistence.py`

Responsibility: write parquet, write manifest sidecar, write diagnostics
sidecar. Consumes only an `AssembledPanel` plus `plan` and `ctx`.

- Hosts (moved from executor.py):
  - `persist_outputs(plan, ctx, assembled) -> StepResult`
  - `persist_diagnostics(plan, ctx, assembled) -> StepResult`
- Imports `run_conformance`, `PanelRequest`, `ACS_MEASURE_COLUMNS`,
  `LAUS_MEASURE_COLUMNS` at module top (they are module-local imports
  today — moving them to module top is safe and reduces per-call cost).
- Imports `collect_conformance_flags` from
  `executor_panel_policies` so the same policy-read path is used.
- Uses `ctx._written_outputs` (the collision-detection set). This is the
  single mutable-ctx side effect that crosses the boundary.

### 2.4 What stays in `executor.py`

- All orchestrator functions: `execute_recipe`, `_execute_plan`,
  `_execute_materialize`, `_execute_resample`, `_execute_join`.
- `ExecutionContext`, `ExecutorError`, `StepResult`, `PipelineResult`.
- `_classify_path`, `_build_provenance`, `_build_manifest`,
  `_target_geometry_metadata`, `_resolve_pipeline_target`,
  `_resolve_panel_output_file`, `_recipe_output_dirname`,
  `resolve_pipeline_artifacts`.
- `_apply_cohort_selector` (tests import it directly).
- `_detect_xwalk_target_col` and everything currently already outside
  the 1786–2266 range.
- Re-exports for backward compatibility (see Section 4).

`_execute_plan` becomes a two-line change: call
`executor_persistence.persist_outputs(plan, ctx, assembled)` with an
assembled panel built by `executor_panel.assemble_panel`. To avoid
assembling the panel twice when both `panel` and `diagnostics` outputs
are declared, the orchestrator should call `assemble_panel` once per
plan execution and pass the result to both step functions.

---

## 3. Data contracts

Two frozen dataclasses define the extraction boundary. Both should live
next to the code that produces them; downstream consumers import by
name.

### 3.1 `AssembledPanel` (in `executor_panel.py`)

Rename `_AssembledPanel` to `AssembledPanel` (remove the private
underscore because it now crosses a module boundary). The shape stays
identical for step one; we widen it only if needed later.

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class AssembledPanel:
    """The panel DataFrame and metadata produced by assemble_panel.

    This is the contract between assembly and persistence. Persistence
    must not inspect `target.panel_policy` directly; anything it needs
    from the policy must flow through `policy_artifacts` or the
    conformance flags helper.
    """
    panel: pd.DataFrame
    frames: list[pd.DataFrame]                   # per-year frames, for row counts
    target: object                                # TargetSpec
    target_geo_type: str
    boundary_vintage: str | None
    definition_version: str | None
    policy_artifacts: dict[str, PolicyApplication] = field(default_factory=dict)

    @property
    def zori_provenance(self) -> ZoriProvenance | None:
        """Backward-compatible accessor used by _persist_outputs."""
        app = self.policy_artifacts.get("zori")
        return app.provenance if app is not None else None
```

The `zori_provenance` property preserves the attribute name today's
`_persist_outputs` references, so step one of the extraction does not
have to touch persistence's provenance-assembly code.

### 3.2 `PolicyApplication` (in `executor_panel_policies.py`)

```python
@dataclass(frozen=True)
class PolicyApplication:
    """The result of applying one PanelPolicyApplier to a panel."""
    name: str                                      # "zori" | "acs1" | "laus"
    panel: pd.DataFrame                            # the possibly-mutated frame
    extra_columns: tuple[str, ...] = ()            # appended to finalize_panel extras
    provenance: object | None = None               # ZoriProvenance today; None for acs1/laus
    notes: tuple[str, ...] = ()                    # progress messages for _echo
```

The applier loop inside `assemble_panel` accumulates `extra_columns`
and hands the union to `finalize_panel`. `notes` are emitted via
`_echo` by the caller.

### 3.3 `ConformanceFlags` (in `executor_panel_policies.py`)

```python
@dataclass(frozen=True)
class ConformanceFlags:
    include_zori: bool
    include_laus: bool
    acs_products: tuple[str, ...]                  # ("acs5",) or ("acs5", "acs1")
    measure_columns: list[str] | None              # after alias translation
```

This is the one-stop translation of `target.panel_policy` for
conformance, replacing the 20+ lines currently inlined inside
`_persist_outputs` (lines ~2098–2142).

---

## 4. Compatibility boundaries

Tests and one production module reach into executor internals. The
extraction must keep these symbols importable from
`coclab.recipe.executor` via re-export shims — do not change callers in
step one.

### Symbols that must remain importable from `coclab.recipe.executor`

From `tests/test_recipe.py` top-level import (lines 23–36):

- `ExecutionContext`
- `ExecutorError`
- `PipelineResult`
- `StepResult`
- `_apply_temporal_filter`
- `_canonicalize_panel_for_target`       <- moves to `executor_panel`
- `_execute_materialize`
- `_execute_resample`
- `_persist_diagnostics`                 <- moves to `executor_persistence`
- `_recipe_output_dirname`
- `_resolve_transform_path`
- `execute_recipe`

From `tests/test_recipe.py` inline imports:

- `_persist_diagnostics` (line 1720)
- `_apply_cohort_selector` (lines 5121, 5135, 5148, 5166, 5177)

From other tests:

- `tests/test_storage_roots.py`: `ExecutionContext`, `_classify_path`
- `tests/test_crosswalk_generalization.py`: `_detect_xwalk_target_col`,
  `ExecutorError`
- `tests/test_recipe_acs1.py`: `_resample_identity`
- `tests/test_recipe_panel_policies.py`: `execute_recipe` only (the
  panel-policy tests go through the public entry point, so the
  extraction can change internals freely as long as the behaviour is
  preserved)

From `coclab/cli/recipe.py`:

- `ExecutorError`, `execute_recipe`, `resolve_pipeline_artifacts`

From `coclab/recipe/probes.py` (lazy import):

- `_identify_metro_and_base`, `_resolve_transform_path`, `ExecutorError`

### Shim strategy

At the bottom of `coclab/recipe/executor.py`, after all remaining
definitions, add:

```python
# Back-compat shims for symbols that moved during the coclab-wxl9 split.
# Tests and external callers import these from coclab.recipe.executor.
from coclab.recipe.executor_panel import (           # noqa: E402,F401
    AssembledPanel as _AssembledPanel,
    assemble_panel as _assemble_panel,
    canonicalize_panel_for_target as _canonicalize_panel_for_target,
)
from coclab.recipe.executor_persistence import (      # noqa: E402,F401
    persist_outputs as _persist_outputs,
    persist_diagnostics as _persist_diagnostics,
)
```

Note: the shim re-exports under the original underscored names, and the
extracted modules define public names without underscores. The alias
preserves `from coclab.recipe.executor import _persist_diagnostics`.

`_apply_cohort_selector`, `_canonicalize_panel_for_target` used inline
in tests must be verified for call-signature parity — they currently
take no special state, so a simple function move + re-export suffices.

### No new public API (yet)

Do not add `coclab.recipe.executor_panel` / `executor_persistence` to
any `__init__.py`. They are internal. Callers continue to import from
`coclab.recipe.executor` so the surface area is unchanged.

---

## 5. Policy translation pattern

The three policy branches inside `_assemble_panel` today are
conditional `if policy.zori is not None`, `if policy.acs1 ... include`,
`if policy.laus ... include`. Each performs a local import, mutates
`panel`, and (in ZORI's case) produces metadata that flows back to
`_persist_outputs` via the `_AssembledPanel` dataclass.

### Proposed pattern: registry of strategy objects

Strategy objects, not strategy functions, because ZORI needs to carry
provenance back to the caller and the protocol needs to express "this
applier returns side-band data". A small protocol plus an ordered
tuple of concrete appliers keeps the loop linear and easy to test in
isolation.

```python
# coclab/recipe/executor_panel_policies.py
from typing import Protocol

class PanelPolicyApplier(Protocol):
    name: str

    def applies_to(
        self,
        *,
        target_geo_type: str,
        policy: PanelPolicy | None,
    ) -> bool: ...

    def apply(
        self,
        panel: pd.DataFrame,
        *,
        policy: PanelPolicy,
        target_geo_type: str,
    ) -> PolicyApplication: ...


class ZoriPolicyApplier:
    name = "zori"

    def applies_to(self, *, target_geo_type, policy):
        return policy is not None and policy.zori is not None

    def apply(self, panel, *, policy, target_geo_type):
        # Local import stays local to the applier, not to _assemble_panel.
        from coclab.panel.zori_eligibility import (
            ZoriProvenance,
            add_provenance_columns,
            apply_zori_eligibility,
            compute_rent_to_income,
        )

        if "zori" in panel.columns and "zori_coc" not in panel.columns:
            panel = panel.rename(columns={"zori": "zori_coc"})
        if "zori_coc" not in panel.columns:
            # Policy declared but no data arrived; emit a skipped application.
            return PolicyApplication(name=self.name, panel=panel)

        rent_alignment = "pit_january"
        if "method" in panel.columns:
            methods = panel["method"].dropna().unique()
            if len(methods) == 1:
                rent_alignment = str(methods[0])

        panel = apply_zori_eligibility(
            panel, min_coverage=policy.zori.min_coverage,
        )
        panel = compute_rent_to_income(panel)
        prov = ZoriProvenance(
            rent_alignment=rent_alignment,
            zori_min_coverage=policy.zori.min_coverage,
        )
        panel = add_provenance_columns(panel, prov)
        for leak in ("method", "geo_count"):
            if leak in panel.columns:
                panel = panel.drop(columns=[leak])

        extras: tuple[str, ...] = ()
        if "zori_max_geo_contribution" in panel.columns:
            extras = ("zori_max_geo_contribution",)

        return PolicyApplication(
            name=self.name,
            panel=panel,
            extra_columns=extras,
            provenance=prov,
        )


class Acs1PolicyApplier:
    name = "acs1"

    def applies_to(self, *, target_geo_type, policy):
        return (
            target_geo_type == "metro"
            and policy is not None
            and policy.acs1 is not None
            and policy.acs1.include
        )

    def apply(self, panel, *, policy, target_geo_type):
        has_acs1 = (
            "unemployment_rate_acs1" in panel.columns
            and panel["unemployment_rate_acs1"].notna().any()
        )
        if has_acs1:
            panel = panel.copy()
            panel["acs1_vintage_used"] = panel["year"].astype(str)
            panel["acs_products_used"] = "acs5,acs1"
            missing = panel["unemployment_rate_acs1"].isna()
            if missing.any():
                panel.loc[missing, "acs1_vintage_used"] = pd.NA
        else:
            panel = panel.copy()
            panel["acs1_vintage_used"] = pd.NA
            panel["acs_products_used"] = "acs5"
            if "unemployment_rate_acs1" not in panel.columns:
                panel["unemployment_rate_acs1"] = np.nan
        return PolicyApplication(name=self.name, panel=panel)


class LausPolicyApplier:
    name = "laus"

    def applies_to(self, *, target_geo_type, policy):
        return (
            target_geo_type == "metro"
            and policy is not None
            and policy.laus is not None
            and policy.laus.include
        )

    def apply(self, panel, *, policy, target_geo_type):
        panel = panel.copy()
        has_laus = (
            "unemployment_rate" in panel.columns
            and panel["unemployment_rate"].notna().any()
        )
        if has_laus:
            panel["laus_vintage_used"] = panel["year"].astype(str)
            missing = panel["unemployment_rate"].isna()
            if missing.any():
                panel.loc[missing, "laus_vintage_used"] = pd.NA
        else:
            panel["laus_vintage_used"] = pd.NA
            for col in ("labor_force", "employed", "unemployed", "unemployment_rate"):
                if col not in panel.columns:
                    panel[col] = np.nan
        return PolicyApplication(name=self.name, panel=panel)


DEFAULT_APPLIERS: tuple[PanelPolicyApplier, ...] = (
    ZoriPolicyApplier(),
    Acs1PolicyApplier(),
    LausPolicyApplier(),
)
```

### Caller inside `assemble_panel`

```python
policy = getattr(target, "panel_policy", None)
source_label = policy.source_label if policy else None
aliases = _resolve_panel_aliases(target)

extras: list[str] = []
policy_artifacts: dict[str, PolicyApplication] = {}

for applier in appliers:
    if not applier.applies_to(target_geo_type=target_geo_type, policy=policy):
        continue
    application = applier.apply(
        panel, policy=policy, target_geo_type=target_geo_type,
    )
    panel = application.panel
    extras.extend(application.extra_columns)
    policy_artifacts[applier.name] = application
    for note in application.notes:
        _echo(ctx, f"  [{applier.name}] {note}")

include_zori = "zori" in policy_artifacts
panel = finalize_panel(
    panel,
    geo_type=target_geo_type,
    include_zori=include_zori,
    source_label=source_label,
    column_aliases=aliases,
    extra_columns=extras or None,
)
```

### Why this shape

- **Order-explicit**: `DEFAULT_APPLIERS` makes the ZORI-before-ACS1-before-LAUS
  ordering visible at module top, rather than buried inside a 180-line
  function. Today's ordering matters for column-existence checks; the
  tuple captures that.
- **Testable in isolation**: each applier takes a DataFrame and a
  `PanelPolicy` — no `ExecutionContext`, no `plan`, no I/O. Unit tests
  can hand in fabricated inputs.
- **Extensible**: adding a fourth policy (e.g. a future "housing"
  applier) is one class plus one tuple entry. No new conditional
  branch inside `assemble_panel`.
- **Imports stay lazy where they need to**: the `zori_eligibility`
  import lives inside `ZoriPolicyApplier.apply`, preserving the
  current behaviour of not paying the import cost when ZORI is not in
  the policy.
- **Provenance flow is explicit**: `AssembledPanel.policy_artifacts`
  is a dict keyed by applier name. `_persist_outputs` reads
  `policy_artifacts["zori"].provenance` instead of the current
  special-case `zori_provenance` field.

---

## 6. Risk assessment

### High-risk items and mitigation

| Risk | Mitigation |
|---|---|
| ZORI/ACS1/LAUS ordering subtly matters — today's sequence is ZORI first (rename + provenance), ACS1 second (metro provenance columns), LAUS third (metro provenance columns). Reordering would change which columns exist at `finalize_panel` time. | Capture order in `DEFAULT_APPLIERS`. Do not change the tuple order in step one. Document the invariant at the tuple definition. |
| `finalize_panel` is sensitive to `extra_columns` and `include_zori`. If the applier loop computes these differently from today, the final schema drifts. | The loop computes `extras` as a union exactly like the current code's single `extra_columns` assignment; `include_zori` is still derived from "did the ZORI applier run successfully". Add an assertion or test that the extracted path produces the same column list as the original for a representative ZORI recipe. |
| `_persist_outputs` currently re-reads policy to configure conformance. Extracting both assembly and persistence in the same step means two call sites could drift if we refactor one but not the other. | Introduce `collect_conformance_flags` in step one but keep it byte-identical to the current inline logic. Verify with a golden-output test of the produced provenance dict before and after. |
| Tests reach in via `from coclab.recipe.executor import _persist_diagnostics` and similar private names. Moving the definition breaks the import if the shim is forgotten. | Add the re-export shim in the same commit as the move; run the test suite to verify imports resolve. |
| `ctx._written_outputs` is a private attribute attached dynamically. The persistence module must still mutate it. | Move the `hasattr(ctx, "_written_outputs")` check into `persist_outputs`; it is already defensive today. Consider promoting it to a real field on `ExecutionContext` in a follow-up bead, not step one. |
| `_assemble_panel` is called from two places (panel and diagnostics persistence) and the current code re-assembles the panel twice when both outputs are requested. The extraction is a good opportunity to fix this, but changing the call pattern is out of scope for step one. | Preserve the double-assembly behaviour in step one to isolate risk. Track single-assembly as a follow-up optimisation. (Alternatively, do it in the orchestrator as noted in 2.4; if you do, the test at `test_diagnostics_no_joined_outputs_fails` still needs to pass because it constructs an empty-intermediates ctx and calls `_persist_diagnostics` directly.) |
| `tests/test_recipe.py::test_diagnostics_no_joined_outputs_fails` constructs an `ExecutionContext` with empty intermediates and calls `_persist_diagnostics(plan, ctx)` directly. The shim must accept this signature unchanged. | `persist_diagnostics(plan, ctx)` must keep the 2-arg signature; internally it calls `assemble_panel(plan, ctx, step_kind="persist_diagnostics")` just like today. The single-assembly optimisation therefore cannot be applied to the diagnostics path without breaking this test; keep `persist_diagnostics` self-contained. |
| Golden-output comparison: the parquet embeds provenance JSON. Bit-for-bit equality is not guaranteed because provenance includes timestamps and dict ordering. | For the refactor validation, compare `table.column_names`, `len(panel)`, and `json.loads(provenance_bytes)` with a ctx whose executed_at is pinned via a fixture. Diagnostics JSON can be compared as parsed dicts minus timestamps. |

### Low-risk items

- `_canonicalize_panel_for_target`: self-contained, no policy logic.
- `_resolve_panel_aliases`: pure read of `target.panel_policy.column_aliases`.
- `_apply_cohort_selector`: test-imported, leave in `executor.py`.

---

## 7. Step sequence

Each step leaves the tree with `pytest` passing. Do not combine steps.

### Step 1 — Scaffold the new modules with no behaviour change

- Create `coclab/recipe/executor_panel.py` containing only the module
  docstring and imports.
- Create `coclab/recipe/executor_persistence.py` containing only the
  module docstring and imports.
- Create `coclab/recipe/executor_panel_policies.py` containing only
  the module docstring and imports.
- Run `pytest -x`. Nothing references the new modules yet; tests must
  still pass.
- Commit: `scaffold panel/persistence split modules`.

### Step 2 — Move `_canonicalize_panel_for_target` and `_resolve_panel_aliases`

- Cut both functions from `executor.py` into `executor_panel.py`.
- Rename to public names in the new module:
  `canonicalize_panel_for_target`, `resolve_panel_aliases`.
- Add a back-compat re-export block at the bottom of `executor.py`
  aliasing the underscored names to the new public names.
- Run `pytest tests/test_recipe.py -x`. The test file imports
  `_canonicalize_panel_for_target` at top level and uses it inline.
- Commit: `move canonicalize helpers into executor_panel`.

### Step 3 — Move `_AssembledPanel` and `_assemble_panel` with no policy extraction

- Cut `_AssembledPanel` (renamed `AssembledPanel`) and `_assemble_panel`
  (renamed `assemble_panel`) into `executor_panel.py`.
- Keep the ZORI/ACS1/LAUS branches inlined verbatim for now.
- Update the re-export shim at the bottom of `executor.py` so
  `_AssembledPanel` and `_assemble_panel` still import cleanly.
- Update `_persist_outputs` and `_persist_diagnostics` in `executor.py`
  to import `assemble_panel` from the new module.
- Run `pytest tests/test_recipe.py tests/test_recipe_panel_policies.py
  tests/test_recipe_acs1.py -x`.
- Commit: `move assemble_panel into executor_panel`.

### Step 4 — Move `_persist_outputs` and `_persist_diagnostics`

- Cut both functions into `executor_persistence.py` as
  `persist_outputs` and `persist_diagnostics`.
- Resolve imports: `PanelRequest`, `run_conformance`,
  `ACS_MEASURE_COLUMNS`, `LAUS_MEASURE_COLUMNS`,
  `summarize_zori_eligibility`, `_build_provenance`, `_build_manifest`,
  `_resolve_panel_output_file`, `_resolve_pipeline_target`,
  `_resolve_panel_aliases`, `_target_geometry_metadata`, `_echo`,
  `write_manifest`, `expand_year_spec`.
  - Several of these still live in `executor.py` after the split. Import
    them from `coclab.recipe.executor` at the top of
    `executor_persistence.py`. This creates a one-way dependency
    `executor_persistence → executor`, which is fine; the reverse would
    be a cycle.
- Update `_execute_plan` in `executor.py` to call
  `persist_outputs` / `persist_diagnostics` from the new module.
- Add the re-export shim aliasing `_persist_outputs` and
  `_persist_diagnostics`.
- Run the full `pytest` suite.
- Commit: `move persistence into executor_persistence`.

### Step 5 — Extract the ZORI policy branch into `ZoriPolicyApplier`

- In `executor_panel_policies.py`, define `PolicyApplication`,
  `PanelPolicyApplier` protocol, and `ZoriPolicyApplier`.
- In `assemble_panel`, replace the ZORI branch with a single applier
  call. The ACS1 and LAUS branches stay inlined for now.
- Store the `ZoriProvenance` on `AssembledPanel.policy_artifacts["zori"]`
  but keep the `zori_provenance` property on `AssembledPanel` so
  `persist_outputs` is unchanged.
- Run `pytest tests/test_recipe_panel_policies.py
  tests/test_zori_eligibility.py -x`, then the full suite.
- Commit: `extract ZoriPolicyApplier`.

### Step 6 — Extract the ACS1 and LAUS policy branches

- Add `Acs1PolicyApplier` and `LausPolicyApplier` to
  `executor_panel_policies.py`.
- Define `DEFAULT_APPLIERS = (ZoriPolicyApplier(), Acs1PolicyApplier(), LausPolicyApplier())`.
- Replace both remaining inline branches in `assemble_panel` with the
  applier loop described in Section 5.
- Run `pytest tests/test_recipe_panel_policies.py tests/test_recipe_acs1.py
  tests/test_recipe_laus*.py -x` (adjust glob to match actual files),
  then the full suite.
- Commit: `extract Acs1 and Laus policy appliers`.

### Step 7 — Introduce `ConformanceFlags` and `collect_conformance_flags`

- Add the helper in `executor_panel_policies.py`. Copy logic verbatim
  from `_persist_outputs` (lines ~2098–2142).
- Replace the inline conformance configuration in `persist_outputs`
  with a single call. Verify with a golden test that the built
  `PanelRequest` has identical fields before and after for a
  representative recipe.
- Run full `pytest` suite.
- Commit: `unify conformance flag resolution`.

### Step 8 — Tighten visibility and documentation

- Move the lazy local imports inside `persist_outputs` and
  `persist_diagnostics` to the top of `executor_persistence.py` where
  doing so does not introduce cycles.
- Add module docstrings that describe the extraction boundary and
  point to `coclab-wxl9`.
- Consider promoting `ExecutionContext._written_outputs` to a real
  field. If the scope is larger than a one-line change, defer to a
  follow-up bead.
- Run full `pytest` suite.
- Commit: `clean up panel/persistence module boundaries`.

### Post-step validation

- After step 8, `executor.py` should be about 470 LOC lighter (~1985
  LOC remaining). Verify `wc -l coclab/recipe/executor.py`.
- Verify that every symbol listed in Section 4 still resolves via
  `python -c "from coclab.recipe.executor import <name>"` for each.
- Run the recipe CLI end-to-end against a known recipe fixture and
  diff the produced `panel.parquet` schema and the
  `panel.manifest.json` file against a pre-refactor capture. Field
  ordering and dtype must match.

### Rollback note

Each step is a single commit. If a later step's test run fails, the
sequence can be rolled back one commit at a time without losing
earlier wins. Do not squash the step-commits until the whole sequence
lands and soaks for at least one CI cycle.
