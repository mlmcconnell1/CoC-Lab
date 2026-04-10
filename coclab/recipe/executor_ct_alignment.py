"""Connecticut planning-region → legacy-county alignment helpers.

Recipe inputs sometimes mix Connecticut's two county vintages: the
post-2022 planning regions (vintage 2024+) and the pre-2022 legacy
counties (vintage 2020).  When a crosswalk uses one vintage and a
dataset uses the other, the executor builds an authoritative bridge
crosswalk and translates measure values across the boundary.  These
helpers own the detection, caching, and translation logic so they can
be reused by both the temporal-filter path and the resample step.
"""

from __future__ import annotations

import pandas as pd

from coclab.geo.ct_planning_regions import (
    CT_PLANNING_REGION_VINTAGE,
    CtPlanningRegionCrosswalk,
    build_ct_county_planning_region_crosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
    translate_weights_planning_to_legacy,
)
from coclab.naming import county_path
from coclab.recipe.executor_core import ExecutionContext, ExecutorError


def _ct_county_alignment_cache_key(legacy_vintage: int) -> tuple[int, int]:
    """Return the canonical cache key for ``ExecutionContext.ct_county_alignment_cache``.

    Centralizing the (legacy_vintage, planning_vintage) tuple convention
    keeps callers from constructing inconsistent keys when reading or
    writing the cache.
    """
    return (legacy_vintage, CT_PLANNING_REGION_VINTAGE)


def _needs_ct_planning_to_legacy_alignment(
    *,
    xwalk: pd.DataFrame,
    source_values: pd.Series,
    source_key: str,
) -> bool:
    """Return True when CT planning-region inputs need a legacy-county bridge."""
    if source_key != "county_fips" or source_key not in xwalk.columns:
        return False

    xwalk_has_ct_legacy = xwalk[source_key].dropna().astype(str).map(
        is_ct_legacy_county_fips,
    ).any()
    source_has_ct_planning = source_values.dropna().astype(str).map(
        is_ct_planning_region_fips,
    ).any()
    return bool(xwalk_has_ct_legacy and source_has_ct_planning)


def _load_ct_county_alignment_crosswalk(
    *,
    ctx: ExecutionContext,
    legacy_vintage: int,
) -> CtPlanningRegionCrosswalk:
    """Load and cache the CT planning-region→legacy county bridge."""
    cache_key = _ct_county_alignment_cache_key(legacy_vintage)
    cached = ctx.ct_county_alignment_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        crosswalk = build_ct_county_planning_region_crosswalk(
            legacy_county_vintage=legacy_vintage,
            planning_region_vintage=CT_PLANNING_REGION_VINTAGE,
        )
    except (FileNotFoundError, ValueError) as exc:
        legacy_path = county_path(legacy_vintage)
        planning_path = county_path(CT_PLANNING_REGION_VINTAGE)
        raise ExecutorError(
            "Connecticut county alignment is required because the recipe "
            "crosswalk uses legacy county FIPS while the dataset uses "
            "planning-region FIPS. Failed to build the authoritative CT "
            f"county bridge from {legacy_path} and {planning_path}: {exc}"
        ) from exc

    ctx.ct_county_alignment_cache[cache_key] = crosswalk
    return crosswalk


def _translate_ct_planning_values_to_legacy(
    *,
    df: pd.DataFrame,
    geo_col: str,
    value_columns: list[str],
    crosswalk: CtPlanningRegionCrosswalk,
    year_value: int | None = None,
) -> pd.DataFrame:
    """Translate CT planning-region values to legacy counties column by column."""
    translated_parts: list[pd.DataFrame] = []
    for value_col in value_columns:
        translated = translate_weights_planning_to_legacy(
            df[[geo_col, value_col]].rename(
                columns={geo_col: "county_fips", value_col: "weight_value"},
            ),
            crosswalk,
        ).rename(columns={"county_fips": geo_col, "weight_value": value_col})
        translated_parts.append(translated)

    result = translated_parts[0]
    for part in translated_parts[1:]:
        result = result.merge(part, on=geo_col, how="outer")
    if year_value is not None:
        result["year"] = year_value
    return result
