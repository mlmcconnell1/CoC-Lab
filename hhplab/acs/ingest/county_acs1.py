"""ACS 1-year county-native data fetcher."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

import hhplab.naming as naming
from hhplab.acs.ingest._acs1_api import (
    ACS1_COUNTY_GEOGRAPHY,
    fetch_acs1_api_data,
    normalize_acs1_measures,
)
from hhplab.acs.variables_acs1 import (
    ACS1_COUNTY_OUTPUT_COLUMNS,
    ACS1_TABLES,
    ACS1_VARIABLES_BY_TABLE,
)
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.sources import CENSUS_API_ACS1

logger = logging.getLogger(__name__)


def fetch_acs1_county_data(
    vintage: int,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch ACS 1-year detailed-table data for all published counties."""
    return fetch_acs1_api_data(vintage, ACS1_COUNTY_GEOGRAPHY, api_key=api_key)


def ingest_county_acs1(
    vintage: int,
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch ACS 1-year detailed-table data at county geography."""
    ingested_at = datetime.now(UTC)

    df = fetch_acs1_county_data(vintage, api_key=api_key)
    fetched_tables = df.attrs.get("acs1_tables_fetched", ACS1_TABLES)
    unavailable_tables = df.attrs.get("acs1_tables_unavailable", [])

    result = normalize_acs1_measures(df)
    result["state"] = result["state"].astype(str).str.zfill(2)
    result["county"] = result["county"].astype(str).str.zfill(3)
    result["county_fips"] = result["state"] + result["county"]
    result["geo_id"] = result["county_fips"]
    result["county_name"] = result["NAME"].astype("string")

    api_url = CENSUS_API_ACS1.format(year=vintage)
    result["data_source"] = "census_acs1"
    result["source_ref"] = f"{api_url}?tables={'+'.join(fetched_tables)}"
    result["ingested_at"] = ingested_at
    result["acs1_vintage"] = str(vintage)

    col_order = [c for c in ACS1_COUNTY_OUTPUT_COLUMNS if c in result.columns]
    result = result[col_order].copy()
    result = result.sort_values("county_fips").reset_index(drop=True)

    base_dir = Path("data") if project_root is None else project_root / "data"
    output_path = naming.acs1_county_path(vintage, base_dir=base_dir)

    provenance = ProvenanceBlock(
        acs_vintage=str(vintage),
        geo_type="county",
        definition_version=None,
        extra={
            "dataset_type": "county_acs1",
            "acs_product": "acs1",
            "tables_requested": ACS1_TABLES,
            "tables_fetched": fetched_tables,
            "tables_unavailable_for_vintage": unavailable_tables,
            "variables": [
                variable_code
                for table in fetched_tables
                for variable_code in ACS1_VARIABLES_BY_TABLE[table]
            ],
            "api_year": vintage,
            "retrieved_at": ingested_at.isoformat(),
            "row_count": len(result),
            "counties_fetched": len(result),
        },
    )

    write_parquet_with_provenance(result, output_path, provenance)
    logger.info("Wrote ACS 1-year county data to %s (%d counties)", output_path, len(result))
    return output_path
