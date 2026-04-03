"""Tests for the shared panel finalization layer (coclab.panel.finalize).

Covers:
- detect_boundary_changes: boundary-vintage change detection
- determine_alignment_type: PIT year / boundary vintage classification
- finalize_panel: canonical ordering, dtypes, source labeling, aliases
- RECIPE_COLUMN_ALIASES: preferred recipe column aliases (coclab-t9rp)
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.panel.finalize import (
    COC_PANEL_COLUMNS,
    METRO_PANEL_COLUMNS,
    RECIPE_COLUMN_ALIASES,
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
    detect_boundary_changes,
    determine_alignment_type,
    finalize_panel,
)

# ---------------------------------------------------------------------------
# detect_boundary_changes
# ---------------------------------------------------------------------------

class TestDetectBoundaryChanges:
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["coc_id", "year", "boundary_vintage_used"])
        result = detect_boundary_changes(df)
        assert result.dtype == bool
        assert len(result) == 0

    def test_missing_columns(self):
        df = pd.DataFrame({"coc_id": ["A"], "year": [2020]})
        result = detect_boundary_changes(df)
        assert list(result) == [False]

    def test_no_changes(self):
        df = pd.DataFrame({
            "coc_id": ["A", "A", "A"],
            "year": [2020, 2021, 2022],
            "boundary_vintage_used": ["2020", "2020", "2020"],
        })
        result = detect_boundary_changes(df)
        assert list(result) == [False, False, False]

    def test_detects_change(self):
        df = pd.DataFrame({
            "coc_id": ["A", "A", "A"],
            "year": [2020, 2021, 2022],
            "boundary_vintage_used": ["2020", "2020", "2022"],
        })
        result = detect_boundary_changes(df)
        assert list(result) == [False, False, True]

    def test_multiple_geos(self):
        df = pd.DataFrame({
            "coc_id": ["A", "A", "B", "B"],
            "year": [2020, 2021, 2020, 2021],
            "boundary_vintage_used": ["2020", "2021", "2020", "2020"],
        })
        result = detect_boundary_changes(df)
        assert list(result) == [False, True, False, False]

    def test_custom_columns(self):
        df = pd.DataFrame({
            "metro_id": ["M1", "M1"],
            "year": [2020, 2021],
            "definition_version_used": ["v1", "v2"],
        })
        result = detect_boundary_changes(
            df, geo_col="metro_id", vintage_col="definition_version_used",
        )
        assert list(result) == [False, True]


# ---------------------------------------------------------------------------
# determine_alignment_type
# ---------------------------------------------------------------------------

class TestDetermineAlignmentType:
    @pytest.mark.parametrize("pit_year,vintage,expected", [
        (2020, "2020", "period_faithful"),
        (2020, "2025", "retrospective"),
        (2020, "2018", "custom"),
        (2020, "abc", "custom"),
    ])
    def test_classification(self, pit_year, vintage, expected):
        assert determine_alignment_type(pit_year, vintage) == expected


# ---------------------------------------------------------------------------
# finalize_panel
# ---------------------------------------------------------------------------

def _make_coc_panel() -> pd.DataFrame:
    """Minimal CoC panel for testing finalization."""
    return pd.DataFrame({
        "coc_id": ["C-500", "C-500"],
        "year": [2020, 2021],
        "pit_total": [100, 110],
        "pit_sheltered": [80, 90],
        "pit_unsheltered": [20, 20],
        "boundary_vintage_used": ["2020", "2020"],
        "acs_vintage_used": ["2019", "2020"],
        "weighting_method": ["area", "area"],
        "source": ["coclab_panel", "coclab_panel"],
        "total_population": [50000.0, 51000.0],
        "coverage_ratio": [0.95, 0.96],
    })


def _make_metro_panel() -> pd.DataFrame:
    """Minimal metro panel for testing finalization."""
    return pd.DataFrame({
        "metro_id": ["M1", "M1"],
        "metro_name": ["Metro One", "Metro One"],
        "geo_type": ["metro", "metro"],
        "geo_id": ["M1", "M1"],
        "year": [2020, 2021],
        "pit_total": [500, 510],
        "pit_sheltered": [400, 410],
        "pit_unsheltered": [100, 100],
        "definition_version_used": ["v1", "v1"],
        "acs_vintage_used": ["2019", "2020"],
        "weighting_method": ["area", "area"],
        "source": ["metro_panel", "metro_panel"],
        "total_population": [100000.0, 101000.0],
        "coverage_ratio": [0.98, 0.99],
    })


class TestFinalizePanel:
    def test_coc_column_ordering(self):
        panel = finalize_panel(_make_coc_panel(), geo_type="coc")
        canonical = [c for c in COC_PANEL_COLUMNS if c in panel.columns]
        assert list(panel.columns[:len(canonical)]) == canonical

    def test_metro_column_ordering(self):
        panel = finalize_panel(_make_metro_panel(), geo_type="metro")
        canonical = [c for c in METRO_PANEL_COLUMNS if c in panel.columns]
        assert list(panel.columns[:len(canonical)]) == canonical

    def test_adds_boundary_changed(self):
        panel = finalize_panel(_make_coc_panel(), geo_type="coc")
        assert "boundary_changed" in panel.columns
        assert panel["boundary_changed"].dtype == bool

    def test_skips_boundary_changed_if_present(self):
        df = _make_coc_panel()
        df["boundary_changed"] = True
        panel = finalize_panel(df, geo_type="coc")
        assert panel["boundary_changed"].all()

    def test_skips_boundary_changed_when_disabled(self):
        df = _make_coc_panel()
        panel = finalize_panel(df, geo_type="coc", add_boundary_changed=False)
        # Should still be present (filled with NA by canonical column fill)
        assert "boundary_changed" in panel.columns

    def test_source_labeling_default(self):
        df = _make_coc_panel().drop(columns=["source"])
        panel = finalize_panel(df, geo_type="coc")
        assert (panel["source"] == "coclab_panel").all()

    def test_source_labeling_metro_default(self):
        df = _make_metro_panel().drop(columns=["source"])
        panel = finalize_panel(df, geo_type="metro")
        assert (panel["source"] == "metro_panel").all()

    def test_source_labeling_override(self):
        df = _make_coc_panel().drop(columns=["source"])
        panel = finalize_panel(df, geo_type="coc", source_label="custom_source")
        assert (panel["source"] == "custom_source").all()

    def test_dtype_enforcement(self):
        panel = finalize_panel(_make_coc_panel(), geo_type="coc")
        assert panel["coc_id"].dtype == object  # str
        assert panel["year"].dtype == int
        assert panel["pit_total"].dtype == int
        assert panel["pit_sheltered"].dtype == "Int64"
        assert panel["pit_unsheltered"].dtype == "Int64"
        assert panel["weighting_method"].dtype == object  # str

    def test_fills_missing_canonical_columns(self):
        df = pd.DataFrame({
            "coc_id": ["C-500"],
            "year": [2020],
            "pit_total": [100],
            "source": ["coclab_panel"],
            "weighting_method": ["area"],
            "boundary_vintage_used": ["2020"],
            "acs_vintage_used": ["2019"],
        })
        panel = finalize_panel(df, geo_type="coc")
        for col in COC_PANEL_COLUMNS:
            assert col in panel.columns, f"Missing canonical column: {col}"

    def test_preserves_extra_columns(self):
        df = _make_coc_panel()
        df["custom_metric"] = 42.0
        panel = finalize_panel(df, geo_type="coc")
        assert "custom_metric" in panel.columns
        assert (panel["custom_metric"] == 42.0).all()

    def test_zori_columns_included(self):
        df = _make_coc_panel()
        panel = finalize_panel(df, geo_type="coc", include_zori=True)
        for col in ZORI_COLUMNS + ZORI_PROVENANCE_COLUMNS:
            assert col in panel.columns

    def test_zori_columns_excluded_by_default(self):
        df = _make_coc_panel()
        panel = finalize_panel(df, geo_type="coc", include_zori=False)
        for col in ZORI_COLUMNS:
            assert col not in panel.columns


# ---------------------------------------------------------------------------
# Column aliases (coclab-t9rp)
# ---------------------------------------------------------------------------

class TestColumnAliases:
    def test_recipe_aliases_defined(self):
        assert "total_population" in RECIPE_COLUMN_ALIASES
        assert RECIPE_COLUMN_ALIASES["total_population"] == "acs_total_population"
        assert "population" in RECIPE_COLUMN_ALIASES
        assert RECIPE_COLUMN_ALIASES["population"] == "pep_population"
        assert "zori_coc" in RECIPE_COLUMN_ALIASES
        assert RECIPE_COLUMN_ALIASES["zori_coc"] == "zori"

    def test_aliases_applied(self):
        df = _make_coc_panel()
        panel = finalize_panel(
            df, geo_type="coc",
            column_aliases={"total_population": "acs_total_population"},
        )
        assert "acs_total_population" in panel.columns
        assert "total_population" not in panel.columns

    def test_aliases_not_applied_by_default(self):
        df = _make_coc_panel()
        panel = finalize_panel(df, geo_type="coc")
        assert "total_population" in panel.columns

    def test_zori_alias(self):
        df = _make_coc_panel()
        df["zori_coc"] = 1500.0
        panel = finalize_panel(
            df, geo_type="coc",
            column_aliases={"zori_coc": "zori"},
        )
        assert "zori" in panel.columns
        assert "zori_coc" not in panel.columns

    def test_pep_population_alias(self):
        df = _make_coc_panel()
        df["population"] = 60000.0
        panel = finalize_panel(
            df, geo_type="coc",
            column_aliases={"population": "pep_population"},
        )
        assert "pep_population" in panel.columns
        assert "population" not in panel.columns

    def test_full_recipe_aliases(self):
        df = _make_coc_panel()
        df["population"] = 60000.0
        df["zori_coc"] = 1500.0
        panel = finalize_panel(
            df, geo_type="coc",
            column_aliases=RECIPE_COLUMN_ALIASES,
        )
        assert "acs_total_population" in panel.columns
        assert "pep_population" in panel.columns
        assert "zori" in panel.columns
        assert "total_population" not in panel.columns
        assert "population" not in panel.columns
        assert "zori_coc" not in panel.columns
