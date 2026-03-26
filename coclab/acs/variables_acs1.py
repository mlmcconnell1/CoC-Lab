"""ACS 1-year variable definitions for metro-native unemployment data.

Single source of truth for ACS 1-year variables fetched at CBSA geography.
These are separate from the ACS 5-year tract-level variables in ``variables.py``
because ACS 1-year is a different Census product with different geographic
availability and temporal resolution.

Initial implementation covers Table B23025 (Employment Status for the
Population 16 Years and Over) for unemployment rate derivation.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Census table B23025: Employment Status for the Population 16 Years and Over
# ---------------------------------------------------------------------------

ACS1_UNEMPLOYMENT_TABLE: str = "B23025"

#: ACS 1-year variable codes to fetch from the Census API.
ACS1_UNEMPLOYMENT_VARIABLES: list[str] = [
    "B23025_001E",  # Total population 16 years and over
    "B23025_003E",  # Civilian labor force
    "B23025_005E",  # Unemployed (civilian)
]

#: Human-readable names for the raw Census variables.
ACS1_VARIABLE_NAMES: dict[str, str] = {
    "B23025_001E": "pop_16_plus",
    "B23025_003E": "civilian_labor_force",
    "B23025_005E": "unemployed_count",
}

#: Tables included (for provenance tracking).
ACS1_TABLES: list[str] = ["B23025"]

# ---------------------------------------------------------------------------
# Derived measures
# ---------------------------------------------------------------------------

#: Derived measure: unemployment_rate_acs1 = unemployed / civilian_labor_force
DERIVED_ACS1_MEASURES: dict[str, str] = {
    "unemployment_rate_acs1": (
        "Unemployment rate from ACS 1-year (B23025_005E / B23025_003E)"
    ),
}

# ---------------------------------------------------------------------------
# Output schema for metro-level ACS1 unemployment data
# ---------------------------------------------------------------------------

#: Canonical column order for metro ACS 1-year output files.
ACS1_METRO_OUTPUT_COLUMNS: list[str] = [
    "metro_id",
    "metro_name",
    "definition_version",
    "acs1_vintage",
    "cbsa_code",
    "pop_16_plus",
    "civilian_labor_force",
    "unemployed_count",
    "unemployment_rate_acs1",
    "data_source",
    "source_ref",
    "ingested_at",
]

# ---------------------------------------------------------------------------
# ACS 1-year availability
# ---------------------------------------------------------------------------

#: ACS 1-year estimates are reliable for metros starting ~2012.
ACS1_FIRST_RELIABLE_YEAR: int = 2012
