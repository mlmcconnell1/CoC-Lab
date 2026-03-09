"""Tests for the temporal variation conformance check (coclab-1d2j).

Verifies that ``check_temporal_variation`` correctly identifies
suspiciously static year-over-year values that may indicate a data
broadcast bug.

Truth table
-----------

    scenario          | CoC count | years     | pattern                        | expected
    ------------------|-----------|-----------|--------------------------------|---------
    normal_variation  | 3         | 2020-2023 | all values differ              | no results
    high_unchanged    | 3         | 2020-2023 | >50% unchanged                 | warning
    broadcast         | 3         | 2020-2023 | >90% unchanged (all same)      | error
    single_year       | 3         | 2022 only | n/a                            | no results (skip)
    missing_measure   | 3         | 2020-2023 | column not in df               | no results
    nulls_excluded    | 3         | 2020-2023 | many nulls, non-null vary      | no results
    mixed_measures    | 3         | 2020-2023 | pop varies, pit static         | warning pit_total

Beads: coclab-1d2j
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.panel.conformance import (
    TEMPORAL_ERROR_THRESHOLD,
    TEMPORAL_WARN_THRESHOLD,
    PanelRequest,
    check_temporal_variation,
)

# ============================================================================
# Constants
# ============================================================================

COCS = ["CO-500", "CA-600", "NY-600"]
YEARS = [2020, 2021, 2022, 2023]
DEFAULT_REQUEST = PanelRequest(start_year=2020, end_year=2023)


# ============================================================================
# Fixture helpers
# ============================================================================


def _make_panel(
    cocs: list[str],
    years: list[int],
    total_population: list[list[float | None]] | None = None,
    pit_total: list[list[float | None]] | None = None,
) -> pd.DataFrame:
    """Build a panel DataFrame from per-CoC value lists.

    Parameters
    ----------
    cocs : list[str]
        CoC IDs (one row-group per CoC).
    years : list[int]
        Year values for every CoC.
    total_population : list[list[float | None]] | None
        Outer list = per CoC, inner list = per year.  ``None`` omits the column.
    pit_total : list[list[float | None]] | None
        Same shape as *total_population*.  ``None`` omits the column.

    Returns
    -------
    pd.DataFrame
    """
    rows: list[dict[str, object]] = []
    for i, coc in enumerate(cocs):
        for j, yr in enumerate(years):
            row: dict[str, object] = {"coc_id": coc, "year": yr}
            if total_population is not None:
                row["total_population"] = total_population[i][j]
            if pit_total is not None:
                row["pit_total"] = pit_total[i][j]
            rows.append(row)
    return pd.DataFrame(rows)


# ============================================================================
# Parametrised fixture data
# ============================================================================

# --- normal_variation: all values differ year-over-year --------------------
# Each CoC gets distinct values for each year in both measures.
_NORMAL_POP = [
    [100, 200, 300, 400],
    [500, 600, 700, 800],
    [900, 1000, 1100, 1200],
]
_NORMAL_PIT = [
    [10, 20, 30, 40],
    [50, 60, 70, 80],
    [90, 100, 110, 120],
]

# --- high_unchanged: >50% unchanged (6 out of 9 pairs = 67%) ---------------
# 3 CoCs x 3 consecutive-year pairs = 9 total pairs.
# For each CoC, years 0-1 unchanged, years 1-2 unchanged, year 2-3 changes.
# That gives 6/9 = 67% unchanged.
_HIGH_UNCHANGED_POP = [
    [100, 100, 100, 999],
    [200, 200, 200, 888],
    [300, 300, 300, 777],
]
_HIGH_UNCHANGED_PIT = [
    [10, 20, 30, 40],
    [50, 60, 70, 80],
    [90, 100, 110, 120],
]

# --- broadcast_detected: >90% unchanged (all same within each CoC) ---------
# Every year has the same value for each CoC = 9/9 = 100% unchanged.
_BROADCAST_POP = [
    [100, 100, 100, 100],
    [200, 200, 200, 200],
    [300, 300, 300, 300],
]
_BROADCAST_PIT = [
    [10, 10, 10, 10],
    [20, 20, 20, 20],
    [30, 30, 30, 30],
]

# --- single_year: only one year, no pairs to compare -----------------------
_SINGLE_YEAR_POP = [
    [100],
    [200],
    [300],
]

# --- nulls_excluded: heavy nulls but non-null values vary ------------------
# Many null entries; the few non-null pairs all differ, so unchanged rate = 0.
_NULLS_POP = [
    [None, 200, None, 400],
    [None, None, None, 800],
    [900, None, 1100, None],
]
_NULLS_PIT = [
    [None, 20, None, 40],
    [None, None, None, 80],
    [90, None, 110, None],
]

# --- mixed_measures: population varies, pit_total is static -----------------
_MIXED_POP = [
    [100, 200, 300, 400],
    [500, 600, 700, 800],
    [900, 1000, 1100, 1200],
]
_MIXED_PIT = [
    [10, 10, 10, 10],
    [20, 20, 20, 20],
    [30, 30, 30, 30],
]


# ============================================================================
# Truth table for parametrize
# ============================================================================

# Each entry: (scenario_id, df, expected_count, expected_severities, expected_measures)
#   expected_severities / expected_measures are lists aligned by result index.

_SCENARIOS: list[
    tuple[str, pd.DataFrame, int, list[str], list[str]]
] = [
    (
        "normal_variation",
        _make_panel(COCS, YEARS, _NORMAL_POP, _NORMAL_PIT),
        0,
        [],
        [],
    ),
    (
        "high_unchanged",
        _make_panel(COCS, YEARS, _HIGH_UNCHANGED_POP, _HIGH_UNCHANGED_PIT),
        1,  # only total_population triggers; pit_total varies
        ["warning"],
        ["total_population"],
    ),
    (
        "broadcast",
        _make_panel(COCS, YEARS, _BROADCAST_POP, _BROADCAST_PIT),
        2,  # both measures trigger
        ["error", "error"],
        ["total_population", "pit_total"],
    ),
    (
        "single_year",
        _make_panel(COCS, [2022], _SINGLE_YEAR_POP),
        0,
        [],
        [],
    ),
    (
        "missing_measure",
        pd.DataFrame({
            "coc_id": COCS * len(YEARS),
            "year": sorted(YEARS * len(COCS)),
            "some_other_col": range(len(COCS) * len(YEARS)),
        }),
        0,
        [],
        [],
    ),
    (
        "nulls_excluded",
        _make_panel(COCS, YEARS, _NULLS_POP, _NULLS_PIT),
        0,
        [],
        [],
    ),
    (
        "mixed_measures",
        _make_panel(COCS, YEARS, _MIXED_POP, _MIXED_PIT),
        1,  # only pit_total triggers
        ["error"],
        ["pit_total"],
    ),
]


# ============================================================================
# Tests
# ============================================================================


class TestCheckTemporalVariation:
    """Tests for check_temporal_variation conformance check."""

    @pytest.mark.parametrize(
        "scenario_id, df, expected_count, expected_severities, expected_measures",
        _SCENARIOS,
        ids=[s[0] for s in _SCENARIOS],
    )
    def test_scenario(
        self,
        scenario_id: str,
        df: pd.DataFrame,
        expected_count: int,
        expected_severities: list[str],
        expected_measures: list[str],
    ) -> None:
        results = check_temporal_variation(df, DEFAULT_REQUEST)

        assert len(results) == expected_count, (
            f"[{scenario_id}] expected {expected_count} result(s), "
            f"got {len(results)}: {[r.message for r in results]}"
        )
        for i, result in enumerate(results):
            assert result.check_name == "temporal_variation"
            assert result.severity == expected_severities[i], (
                f"[{scenario_id}] result {i}: expected severity "
                f"{expected_severities[i]!r}, got {result.severity!r}"
            )
            assert result.details["measure"] == expected_measures[i], (
                f"[{scenario_id}] result {i}: expected measure "
                f"{expected_measures[i]!r}, got {result.details['measure']!r}"
            )

    # -- Detailed checks on result content ----------------------------------

    def test_warning_details_structure(self) -> None:
        """Warning result contains all expected details keys."""
        df = _make_panel(COCS, YEARS, _HIGH_UNCHANGED_POP, _HIGH_UNCHANGED_PIT)
        results = check_temporal_variation(df, DEFAULT_REQUEST)

        # Should have exactly one warning for total_population.
        assert len(results) == 1
        r = results[0]
        assert r.severity == "warning"
        assert r.details["measure"] == "total_population"
        assert r.details["unchanged_rate"] == pytest.approx(6 / 9)
        assert r.details["unchanged_count"] == 6
        assert r.details["total_pairs"] == 9
        assert r.details["threshold_used"] == TEMPORAL_WARN_THRESHOLD

    def test_error_details_structure(self) -> None:
        """Error result contains all expected details keys and broadcast suffix."""
        df = _make_panel(COCS, YEARS, _BROADCAST_POP, _BROADCAST_PIT)
        results = check_temporal_variation(df, DEFAULT_REQUEST)

        # Both measures should trigger errors.
        assert len(results) == 2
        for r in results:
            assert r.severity == "error"
            assert r.details["unchanged_rate"] == pytest.approx(1.0)
            assert r.details["unchanged_count"] == 9
            assert r.details["total_pairs"] == 9
            assert r.details["threshold_used"] == TEMPORAL_ERROR_THRESHOLD
            assert "possible data broadcast" in r.message

    def test_warning_message_format(self) -> None:
        """Warning message follows the specified format (no broadcast suffix)."""
        df = _make_panel(COCS, YEARS, _HIGH_UNCHANGED_POP, _HIGH_UNCHANGED_PIT)
        results = check_temporal_variation(df, DEFAULT_REQUEST)

        assert len(results) == 1
        msg = results[0].message
        assert msg.startswith("total_population:")
        assert "67%" in msg  # 6/9 ≈ 66.7% rounds to 67%
        assert "(6/9)" in msg
        assert "possible data broadcast" not in msg

    def test_error_message_format(self) -> None:
        """Error message follows the specified format with broadcast suffix."""
        df = _make_panel(COCS, YEARS, _BROADCAST_POP, _BROADCAST_PIT)
        results = check_temporal_variation(df, DEFAULT_REQUEST)

        pop_result = next(r for r in results if r.details["measure"] == "total_population")
        msg = pop_result.message
        assert msg.startswith("total_population:")
        assert "100%" in msg
        assert "(9/9)" in msg
        assert "\u2014 possible data broadcast" in msg

    def test_null_null_not_counted_as_unchanged(self) -> None:
        """Two consecutive nulls should NOT count as an unchanged pair."""
        # Build a panel where all values are null.
        df = _make_panel(
            COCS,
            YEARS,
            total_population=[[None, None, None, None]] * 3,
        )
        results = check_temporal_variation(df, DEFAULT_REQUEST)
        # No comparable pairs exist, so no results.
        assert len(results) == 0

    def test_only_measures_in_constant_are_checked(self) -> None:
        """Columns not in TEMPORAL_VARIATION_MEASURES are ignored."""
        df = pd.DataFrame({
            "coc_id": COCS * len(YEARS),
            "year": sorted(YEARS * len(COCS)),
            # This column is static but should not be checked.
            "median_household_income": [50000] * (len(COCS) * len(YEARS)),
        })
        results = check_temporal_variation(df, DEFAULT_REQUEST)
        assert len(results) == 0
