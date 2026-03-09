"""CoC coverage conformance check tests.

Tests for the three coverage-related conformance checks defined in
coclab/panel/conformance.py:

1. check_coc_count — warns when fewer CoCs than expected
2. check_panel_balance — warns when not all CoCs appear in all years
3. check_coc_year_gaps — warns when CoCs have non-contiguous year coverage

Fixture design
--------------
All fixture data is declared as module-level constants. Each parametrized
test scenario is named and derives expectations from these constants.
Check functions are called directly (not via run_conformance) so each
test isolates one check.

CoC count truth table (check_coc_count):

    Scenario        | expected | actual | Result
    ----------------|----------|--------|------------------
    expected_matches|    3     |   3    | no warning
    expected_fewer  |    5     |   3    | 1 warning (deficit=2)
    expected_none   |   None   |   3    | no results (skip)

Panel balance truth table (check_panel_balance):

    Scenario         | Setup                          | Result
    -----------------|--------------------------------|-------------------
    balanced         | all CoCs in all years           | no warning
    unbalanced       | COC-C missing year 2022         | 1 warning
    single_year      | only 1 year in panel            | no warning

CoC year gaps truth table (check_coc_year_gaps):

    Scenario          | Setup                                    | Result
    ------------------|------------------------------------------|------------------
    no_gaps           | each CoC's years are contiguous           | no warning
    has_gaps          | COC-D present 2020,2022 (absent 2021)    | 1 warning
    edge_only_missing | COC-E present 2021-2023, panel 2020-2024 | no warning

Beads:
- coclab-2o8i: CoC coverage conformance checks
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.panel.conformance import (
    PanelRequest,
    check_coc_count,
    check_coc_year_gaps,
    check_panel_balance,
)

# ============================================================================
# Fixture constants — single source of truth
# ============================================================================

COC_IDS = ["COC-A", "COC-B", "COC-C"]
YEARS = [2020, 2021, 2022, 2023]

# Balanced panel: every CoC in every year.
BALANCED_ROWS = [
    {"coc_id": coc, "year": y, "value": 1.0}
    for coc in COC_IDS
    for y in YEARS
]

# Unbalanced panel: COC-C is missing year 2022.
UNBALANCED_MISSING_COC = "COC-C"
UNBALANCED_MISSING_YEAR = 2022
UNBALANCED_ROWS = [
    row for row in BALANCED_ROWS
    if not (row["coc_id"] == UNBALANCED_MISSING_COC
            and row["year"] == UNBALANCED_MISSING_YEAR)
]

# Single-year panel: only 2020.
SINGLE_YEAR_ROWS = [
    {"coc_id": coc, "year": 2020, "value": 1.0}
    for coc in COC_IDS
]

# Panel with internal gaps: COC-D is present in 2020 and 2022, absent in 2021.
GAP_COC = "COC-D"
GAP_MISSING_YEAR = 2021
GAP_PANEL_COCS = ["COC-A", "COC-B", GAP_COC]
GAP_ROWS = [
    {"coc_id": coc, "year": y, "value": 1.0}
    for coc in GAP_PANEL_COCS
    for y in YEARS
    if not (coc == GAP_COC and y == GAP_MISSING_YEAR)
]

# Panel with edge-only missing: COC-E present 2021-2023, panel spans 2020-2024.
EDGE_COC = "COC-E"
EDGE_PANEL_YEARS = [2020, 2021, 2022, 2023, 2024]
EDGE_COC_YEARS = [2021, 2022, 2023]
EDGE_PANEL_COCS = ["COC-A", "COC-B", EDGE_COC]
EDGE_ROWS = [
    {"coc_id": coc, "year": y, "value": 1.0}
    for coc in EDGE_PANEL_COCS
    for y in EDGE_PANEL_YEARS
    if not (coc == EDGE_COC and y not in EDGE_COC_YEARS)
]


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from row dicts."""
    return pd.DataFrame(rows)


def _default_request(**overrides) -> PanelRequest:
    """Build a PanelRequest with sensible defaults, allowing overrides."""
    kwargs = {
        "start_year": YEARS[0],
        "end_year": YEARS[-1],
    }
    kwargs.update(overrides)
    return PanelRequest(**kwargs)


# ============================================================================
# check_coc_count tests
# ============================================================================


class TestCheckCocCount:
    """Tests for check_coc_count conformance check."""

    @pytest.mark.parametrize(
        "scenario,expected_coc_count,expected_result_count",
        [
            ("expected_matches", len(COC_IDS), 0),
            ("expected_fewer", 5, 1),
            ("expected_none", None, 0),
        ],
        ids=["expected_matches", "expected_fewer", "expected_none"],
    )
    def test_coc_count(
        self, scenario, expected_coc_count, expected_result_count
    ):
        df = _make_df(BALANCED_ROWS)
        request = _default_request(expected_coc_count=expected_coc_count)
        results = check_coc_count(df, request)

        assert len(results) == expected_result_count

        if expected_result_count > 0:
            r = results[0]
            assert r.check_name == "coc_count_mismatch"
            assert r.severity == "warning"
            actual_count = len(COC_IDS)
            assert r.details["actual_count"] == actual_count
            assert r.details["expected_count"] == expected_coc_count
            assert r.details["deficit"] == expected_coc_count - actual_count
            assert f"{actual_count}/{expected_coc_count}" in r.message


# ============================================================================
# check_panel_balance tests
# ============================================================================


class TestCheckPanelBalance:
    """Tests for check_panel_balance conformance check."""

    @pytest.mark.parametrize(
        "scenario,rows,expected_result_count",
        [
            ("balanced", BALANCED_ROWS, 0),
            ("unbalanced", UNBALANCED_ROWS, 1),
            ("single_year", SINGLE_YEAR_ROWS, 0),
        ],
        ids=["balanced", "unbalanced", "single_year"],
    )
    def test_panel_balance(self, scenario, rows, expected_result_count):
        df = _make_df(rows)
        request = _default_request()
        results = check_panel_balance(df, request)

        assert len(results) == expected_result_count

        if scenario == "unbalanced":
            r = results[0]
            assert r.check_name == "unbalanced_panel"
            assert r.severity == "warning"
            assert r.details["incomplete_count"] == 1
            assert r.details["total_cocs"] == len(COC_IDS)
            assert r.details["most_common_gap"] == UNBALANCED_MISSING_YEAR
            assert sorted(r.details["expected_years"]) == sorted(YEARS)
            # Message format: "{incomplete_count} CoCs have incomplete year coverage ..."
            assert "1 CoCs have incomplete year coverage" in r.message
            assert f"1/{len(COC_IDS)}" in r.message


# ============================================================================
# check_coc_year_gaps tests
# ============================================================================


class TestCheckCocYearGaps:
    """Tests for check_coc_year_gaps conformance check."""

    def test_no_gaps(self):
        """Contiguous years for all CoCs produces no warnings."""
        df = _make_df(BALANCED_ROWS)
        request = _default_request()
        results = check_coc_year_gaps(df, request)
        assert len(results) == 0

    def test_has_gaps(self):
        """CoC with internal gap (present/absent/present) triggers warning."""
        df = _make_df(GAP_ROWS)
        request = _default_request()
        results = check_coc_year_gaps(df, request)

        assert len(results) == 1
        r = results[0]
        assert r.check_name == "coc_year_gaps"
        assert r.severity == "warning"
        assert r.details["gap_count"] == 1

        # Verify the example entry for the gapped CoC.
        examples = r.details["examples"]
        assert len(examples) == 1
        example = examples[0]
        assert example["coc_id"] == GAP_COC
        assert GAP_MISSING_YEAR in example["missing_years"]
        assert GAP_MISSING_YEAR not in example["present_years"]

    def test_edge_only_missing(self):
        """CoC starting late and ending early is NOT a gap (just edges)."""
        df = _make_df(EDGE_ROWS)
        request = _default_request(
            start_year=EDGE_PANEL_YEARS[0], end_year=EDGE_PANEL_YEARS[-1]
        )
        results = check_coc_year_gaps(df, request)
        assert len(results) == 0
