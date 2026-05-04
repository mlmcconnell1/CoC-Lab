"""Tests for ACS 1-year county-native ingest."""

from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.acs.ingest._acs1_api import ACS1_COUNTY_GEOGRAPHY
from hhplab.acs.ingest.county_acs1 import fetch_acs1_county_data, ingest_county_acs1
from hhplab.acs.variables_acs1 import (
    ACS1_COUNTY_OUTPUT_COLUMNS,
    ACS1_VARIABLES_BY_TABLE,
    acs1_tables_for_vintage,
)
from hhplab.cli.main import app
from hhplab.provenance import read_provenance

CENSUS_API_URL_PATTERN = re.compile(r"https://api\.census\.gov/data/\d{4}/acs/acs1.*")

SAMPLE_COUNTIES = [
    {
        "NAME": "Los Angeles County, California",
        "state": "06",
        "county": "037",
        "B23025_001E": "8000000",
        "B23025_003E": "5200000",
        "B23025_005E": "260000",
    },
    {
        "NAME": "Kings County, New York",
        "state": "36",
        "county": "047",
        "B23025_001E": "2100000",
        "B23025_003E": "1300000",
        "B23025_005E": "78000",
    },
]


def make_county_response(
    counties: list[dict[str, Any]],
    variables: list[str],
) -> list[list[str]]:
    headers = ["NAME", *variables, *ACS1_COUNTY_GEOGRAPHY.response_columns]
    rows = [headers]
    for county in counties:
        row = [county["NAME"]]
        row.extend(str(county.get(var, "0")) for var in variables)
        row.extend([county["state"], county["county"]])
        rows.append(row)
    return rows


def queue_acs1_county_responses(httpx_mock, counties: list[dict[str, Any]], vintage: int) -> None:
    for table in acs1_tables_for_vintage(vintage):
        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=make_county_response(counties, ACS1_VARIABLES_BY_TABLE[table]),
        )


def test_fetch_county_parses_census_response(httpx_mock) -> None:
    queue_acs1_county_responses(httpx_mock, SAMPLE_COUNTIES, vintage=2023)

    df = fetch_acs1_county_data(vintage=2023)

    assert len(df) == 2
    assert set(df["state"]) == {"06", "36"}
    assert set(df["county"]) == {"037", "047"}
    assert "B23025_003E" in df.columns


def test_ingest_county_writes_schema_and_provenance(httpx_mock, tmp_path) -> None:
    queue_acs1_county_responses(httpx_mock, SAMPLE_COUNTIES, vintage=2023)

    path = ingest_county_acs1(vintage=2023, project_root=tmp_path)
    df = pd.read_parquet(path)

    assert path == tmp_path / "data" / "curated" / "acs" / "acs1_county__A2023.parquet"
    assert list(df.columns) == ACS1_COUNTY_OUTPUT_COLUMNS
    assert list(df["county_fips"]) == ["06037", "36047"]
    assert list(df["geo_id"]) == ["06037", "36047"]
    assert df["unemployment_rate_acs1"].tolist() == pytest.approx([0.05, 0.06])
    assert df["civilian_labor_force"].dtype == "Int64"
    assert df["unemployment_rate_acs1"].dtype == "Float64"

    provenance = read_provenance(path)
    assert provenance.geo_type == "county"
    assert provenance.extra["dataset_type"] == "county_acs1"
    assert provenance.extra["counties_fetched"] == 2


def test_county_cli_json_output(httpx_mock, tmp_path, monkeypatch) -> None:
    queue_acs1_county_responses(httpx_mock, SAMPLE_COUNTIES, vintage=2023)
    (tmp_path / "pyproject.toml").touch()
    (tmp_path / "hhplab").mkdir()
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        ["ingest", "acs1-county", "--vintage", "2023", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["output_path"] == "data/curated/acs/acs1_county__A2023.parquet"
    assert payload["row_count"] == 2
    assert payload["unemployment_summary"]["mean"] == pytest.approx(0.055)
