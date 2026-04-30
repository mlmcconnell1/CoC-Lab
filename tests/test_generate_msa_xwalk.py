"""Tests for `hhplab generate msa-xwalk`."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.registry.schema import RegistryEntry

runner = CliRunner()


def _write_test_inputs(tmp_path: Path) -> None:
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    msa_dir = tmp_path / "data" / "curated" / "msa"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    msa_dir.mkdir(parents=True, exist_ok=True)

    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")

    gpd.GeoDataFrame(
        {"GEOID": ["36061"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")

    pd.DataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "county_fips": ["36061"],
            "county_name": ["New York County"],
            "state_name": ["New York"],
            "central_outlying": ["Central"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__census_msa_2023.parquet")


def test_generate_msa_xwalk_json(monkeypatch, tmp_path: Path):
    _write_test_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate_msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate_msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    result = runner.invoke(
        app,
        ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["rows"] == 1
    assert payload["coc_count"] == 1
    assert payload["msa_count"] == 1
    assert payload["artifact"].endswith(
        "msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet"
    )


def test_generate_msa_xwalk_missing_membership_is_actionable(monkeypatch, tmp_path: Path):
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)

    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")
    gpd.GeoDataFrame(
        {"GEOID": ["36061"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")

    monkeypatch.setattr(
        "hhplab.cli.generate_msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )

    result = runner.invoke(
        app,
        ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert "Run: hhplab generate msa --definition-version census_msa_2023" in payload["error"]
