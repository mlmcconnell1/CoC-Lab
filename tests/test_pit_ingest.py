"""Tests for PIT data ingestion from HUD Exchange."""

import pytest

from coclab.pit.ingest.hud_exchange import (
    get_pit_source_url,
    list_available_years,
)


class TestGetPitSourceUrl:
    """Tests for get_pit_source_url function."""

    def test_known_year_2024(self):
        url = get_pit_source_url(2024)
        assert "2024" in url
        assert "hudexchange.info" in url
        assert url.endswith(".xlsx")

    def test_known_year_2023(self):
        url = get_pit_source_url(2023)
        assert "2023" in url
        assert url.endswith(".xlsx")

    def test_unknown_year_constructs_url(self):
        # Future year - should construct a URL
        url = get_pit_source_url(2030)
        assert "2030" in url
        assert url.endswith(".xlsx")


class TestListAvailableYears:
    """Tests for list_available_years function."""

    def test_returns_list(self):
        years = list_available_years()
        assert isinstance(years, list)

    def test_sorted_descending(self):
        years = list_available_years()
        assert years == sorted(years, reverse=True)

    def test_contains_recent_years(self):
        years = list_available_years()
        assert 2024 in years
        assert 2023 in years

    def test_all_integers(self):
        years = list_available_years()
        assert all(isinstance(y, int) for y in years)
