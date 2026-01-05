"""Tests for PIT data parsing and canonicalization."""

import pytest

from coclab.pit.ingest.parser import normalize_coc_id


class TestNormalizeCocId:
    """Tests for normalize_coc_id function."""

    def test_standard_format(self):
        assert normalize_coc_id("CO-500") == "CO-500"

    def test_lowercase(self):
        assert normalize_coc_id("co-500") == "CO-500"

    def test_no_dash(self):
        assert normalize_coc_id("CO500") == "CO-500"

    def test_whitespace(self):
        assert normalize_coc_id(" CO-500 ") == "CO-500"

    def test_space_separator(self):
        assert normalize_coc_id("CO 500") == "CO-500"

    def test_california(self):
        assert normalize_coc_id("CA-600") == "CA-600"

    def test_new_york(self):
        assert normalize_coc_id("NY-600") == "NY-600"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_coc_id("")

    def test_none_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_coc_id(None)

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize"):
            normalize_coc_id("INVALID")

    def test_too_many_digits_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize"):
            normalize_coc_id("CO-5000")

    def test_too_few_digits_raises(self):
        with pytest.raises(ValueError, match="Cannot normalize"):
            normalize_coc_id("CO-50")
