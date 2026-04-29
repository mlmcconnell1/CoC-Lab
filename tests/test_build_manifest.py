"""Tests for build manifest I/O and aggregate run recording.

Covers:
- read_build_manifest round-trip
- get_build_years helper
- record_aggregate_run appending and schema
"""

import json
from pathlib import Path

import pytest

from hhplab.builds import (
    get_build_years,
    read_build_manifest,
    record_aggregate_run,
)


def _write_manifest(build_dir: Path, name: str, years: list[int]) -> None:
    """Write a minimal build manifest for test setup."""
    manifest = {
        "schema_version": 1,
        "build": {"name": name, "years": years},
        "base_assets": [],
        "aggregate_runs": [],
    }
    (build_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


# ---------------------------------------------------------------------------
# Manifest read
# ---------------------------------------------------------------------------


class TestManifestRead:
    def test_read_returns_dict(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020, 2021])

        manifest = read_build_manifest(tmp_path)
        assert manifest["schema_version"] == 1
        assert manifest["build"]["name"] == "test"
        assert manifest["build"]["years"] == [2020, 2021]

    def test_read_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_build_manifest(tmp_path / "nonexistent")


class TestGetBuildYears:
    def test_returns_years(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020, 2021, 2022])
        assert get_build_years(tmp_path) == [2020, 2021, 2022]

    def test_returns_empty_for_minimal_manifest(self, tmp_path):
        (tmp_path / "manifest.json").write_text('{"schema_version": 1}\n')
        assert get_build_years(tmp_path) == []


# ---------------------------------------------------------------------------
# record_aggregate_run
# ---------------------------------------------------------------------------


class TestRecordAggregateRun:
    def test_appends_run_entry(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020])

        entry = record_aggregate_run(
            tmp_path,
            dataset="pep",
            alignment="as_of_july",
            years_requested=[2020],
        )

        assert entry["dataset"] == "pep"
        assert entry["status"] == "success"
        assert len(entry["run_id"]) == 12

        manifest = read_build_manifest(tmp_path)
        assert len(manifest["aggregate_runs"]) == 1
        assert manifest["aggregate_runs"][0]["run_id"] == entry["run_id"]

    def test_multiple_runs_append(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020, 2021])

        record_aggregate_run(
            tmp_path, dataset="pep", alignment="as_of_july", years_requested=[2020],
        )
        record_aggregate_run(
            tmp_path, dataset="acs", alignment="vintage_end_year", years_requested=[2020, 2021],
        )

        manifest = read_build_manifest(tmp_path)
        assert len(manifest["aggregate_runs"]) == 2
        assert manifest["aggregate_runs"][0]["dataset"] == "pep"
        assert manifest["aggregate_runs"][1]["dataset"] == "acs"

    def test_failed_run_records_error(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020])

        entry = record_aggregate_run(
            tmp_path,
            dataset="zori",
            alignment="monthly_native",
            years_requested=[2020],
            status="failed",
            error="Missing ZORI data",
        )

        assert entry["status"] == "failed"
        assert entry["error"] == "Missing ZORI data"

    def test_alignment_params_recorded(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020])

        entry = record_aggregate_run(
            tmp_path,
            dataset="pep",
            alignment="lagged",
            years_requested=[2020],
            alignment_params={"lag_months": 6},
        )

        assert entry["alignment"]["mode"] == "lagged"
        assert entry["alignment"]["lag_months"] == 6

    def test_years_materialized_defaults_to_requested(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020, 2021])

        entry = record_aggregate_run(
            tmp_path, dataset="pit", alignment="point_in_time_jan",
            years_requested=[2020, 2021],
        )

        assert entry["years_materialized"] == [2020, 2021]

    def test_outputs_recorded(self, tmp_path):
        _write_manifest(tmp_path, "test", [2020])

        entry = record_aggregate_run(
            tmp_path, dataset="pep", alignment="as_of_july",
            years_requested=[2020],
            outputs=["data/curated/pep/pep__B2020.parquet"],
        )

        assert entry["outputs"] == ["data/curated/pep/pep__B2020.parquet"]
