"""Tests for `hhplab ingest msa-boundaries` and `hhplab validate msa`."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


def test_ingest_msa_boundaries_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    artifact = tmp_path / "data" / "curated" / "msa" / "msa_boundaries__census_msa_2023.parquet"
    artifact.parent.mkdir(parents=True, exist_ok=True)

    def fake_ingest(definition_version: str, *, tiger_year: int):
        gpd.GeoDataFrame(
            {
                "msa_id": ["35620"],
                "cbsa_code": ["35620"],
                "msa_name": ["New York-Newark-Jersey City, NY-NJ-PA"],
                "area_type": ["Metropolitan Statistical Area"],
                "definition_version": [definition_version],
                "geometry_vintage": [str(tiger_year)],
                "source": ["census_tiger_cbsa"],
                "source_ref": ["https://example.test/cbsa.zip"],
                "ingested_at": [pd.Timestamp("2026-04-30T00:00:00Z")],
            },
            geometry=[box(0, 0, 1, 1)],
            crs="EPSG:4326",
        ).to_parquet(artifact)
        return artifact

    monkeypatch.setattr(
        "hhplab.msa.boundaries.ingest_msa_boundaries",
        fake_ingest,
    )
    monkeypatch.setattr(
        "hhplab.msa.boundaries.read_msa_boundaries",
        lambda definition_version: gpd.read_parquet(artifact),
    )

    result = runner.invoke(
        app,
        ["ingest", "msa-boundaries", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["geometry_vintage"] == 2023
    assert payload["msa_count"] == 1
    assert payload["artifact"].endswith("msa_boundaries__census_msa_2023.parquet")


def test_validate_msa_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    msadir = tmp_path / "data" / "curated" / "msa"
    msadir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "msa_name": ["New York-Newark-Jersey City, NY-NJ-PA"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
            "source": ["census_msa_delineation_2023"],
            "source_ref": ["https://example.test/list1.xlsx"],
        }
    ).to_parquet(msadir / "msa_definitions__census_msa_2023.parquet")
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
    ).to_parquet(msadir / "msa_county_membership__census_msa_2023.parquet")
    gpd.GeoDataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "msa_name": ["New York-Newark-Jersey City, NY-NJ-PA"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
            "geometry_vintage": ["2023"],
            "source": ["census_tiger_cbsa"],
            "source_ref": ["https://example.test/cbsa.zip"],
            "ingested_at": [pd.Timestamp("2026-04-30T00:00:00Z")],
        },
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    ).to_parquet(msadir / "msa_boundaries__census_msa_2023.parquet")

    result = runner.invoke(app, ["validate", "msa", "--json"], catch_exceptions=False)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["errors"] == []
