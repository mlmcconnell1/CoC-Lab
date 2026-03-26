"""ACS 1-year variable definitions for metro-native unemployment data."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Census table B23025: Employment Status for the Population 16 Years and Over
# ---------------------------------------------------------------------------

ACS1_UNEMPLOYMENT_TABLE: str = "B23025"

ACS1_UNEMPLOYMENT_VARIABLES: dict[str, str] = {
    "B23025_001E": "pop_16_plus",           # Total population 16+
    "B23025_002E": "in_labor_force",         # In labor force
    "B23025_003E": "civilian_labor_force",   # Civilian labor force
    "B23025_005E": "unemployed",             # Unemployed (civilian)
    "B23025_007E": "not_in_labor_force",     # Not in labor force
}

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

ACS1_METRO_OUTPUT_COLUMNS: list[str] = [
    "metro_id",
    "cbsa_code",
    "year",
    "acs1_vintage",
    "unemployment_rate_acs1",
    "civilian_labor_force",
    "unemployed_count",
    "pop_16_plus",
    "data_source",
    "source_ref",
    "ingested_at",
]

# ---------------------------------------------------------------------------
# ACS 1-year availability
# ---------------------------------------------------------------------------

#: ACS 1-year estimates are reliable for metros starting ~2012.
ACS1_FIRST_RELIABLE_YEAR: int = 2012
