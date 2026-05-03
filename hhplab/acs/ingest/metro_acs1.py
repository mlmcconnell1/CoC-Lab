"""ACS 1-year metro-native data fetcher.

Fetches ACS 1-year detailed-table data from the Census Bureau API at CBSA
(metropolitan statistical area) geography, maps CBSAs to Glynn/Fox metro IDs,
and computes derived unemployment rates.

Unlike the ACS 5-year tract pipeline, ACS 1-year data is available directly at
CBSA geography -- no crosswalk or tract aggregation is needed.

Usage
-----
    from hhplab.acs.ingest.metro_acs1 import ingest_metro_acs1

    path = ingest_metro_acs1(vintage=2023)

Output Schema
-------------
- metro_id (str): Glynn/Fox metro identifier (e.g., "GF01")
- metro_name (str): Metro area name
- definition_version (str): e.g., "glynn_fox_v1"
- acs1_vintage (str): e.g., "2023"
- cbsa_code (str): Census CBSA code for traceability
- pop_16_plus (Int64): Population 16 years and over (B23025_001E)
- civilian_labor_force (Int64): Civilian labor force (B23025_003E)
- unemployed_count (Int64): Unemployed civilians (B23025_005E)
- unemployment_rate_acs1 (Float64): unemployed_count / civilian_labor_force
- additional ACS1 income, housing-cost, utility, tenure, and housing-stock
  measures from the requested detailed tables
- data_source (str): always "census_acs1"
- source_ref (str): API URL used
- ingested_at (datetime UTC)
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd
import hhplab.naming as naming

from hhplab.acs.variables_acs1 import (
    ACS1_FIRST_RELIABLE_YEAR,
    ACS1_FLOAT_COLUMNS,
    ACS1_INTEGER_COLUMNS,
    ACS1_METRO_OUTPUT_COLUMNS,
    ACS1_TABLES,
    ACS1_UNAVAILABLE_VINTAGES,
    ACS1_VARIABLES_BY_TABLE,
    ACS1_VARIABLE_NAMES,
    acs1_tables_for_vintage,
    acs1_unavailable_tables_for_vintage,
)
from hhplab.metro.definitions import (
    cbsa_to_metro_id,
    metro_name_for_id,
)
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.sources import CENSUS_API_ACS1

logger = logging.getLogger(__name__)

# Census API geography parameter for CBSA-level queries
CBSA_GEO_PARAM = "metropolitan statistical area/micropolitan statistical area"


def fetch_acs1_cbsa_data(
    vintage: int,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch ACS 1-year detailed-table data for all CBSAs from Census API.

    Makes one Census API request per requested ACS table, then merges the
    responses on CBSA geography.

    Parameters
    ----------
    vintage : int
        ACS 1-year vintage year (e.g., 2023).
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY environment variable.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns for each fetched ACS1 variable and ``cbsa_code``.

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    ValueError
        If the API response cannot be parsed.
    """
    if vintage in ACS1_UNAVAILABLE_VINTAGES:
        raise ValueError(
            f"ACS 1-year data for vintage {vintage} is not available from Census. "
            f"Census did not publish ACS 1-year estimates for {vintage} due to "
            f"COVID-19 data collection disruptions. "
            f"For labor-market measures in {vintage}, consider BLS LAUS data "
            f"('hhplab ingest laus-metro --year {vintage}') instead."
        )

    if vintage < ACS1_FIRST_RELIABLE_YEAR:
        logger.warning(
            "ACS 1-year vintage %d is before the first reliable year (%d); "
            "data may have limited coverage or reliability",
            vintage,
            ACS1_FIRST_RELIABLE_YEAR,
        )

    available_tables = acs1_tables_for_vintage(vintage)
    unavailable_tables = acs1_unavailable_tables_for_vintage(vintage)

    if api_key is None:
        api_key = os.environ.get("CENSUS_API_KEY")
    frames: list[pd.DataFrame] = []
    url = CENSUS_API_ACS1.format(year=vintage)

    logger.info(
        "Fetching ACS 1-year %d data for all CBSAs across %d tables",
        vintage,
        len(available_tables),
    )
    if unavailable_tables:
        logger.info(
            "Skipping ACS1 tables unavailable for vintage %d: %s",
            vintage,
            ", ".join(unavailable_tables),
        )

    with httpx.Client(timeout=60.0) as client:
        for table in available_tables:
            table_variables = ACS1_VARIABLES_BY_TABLE[table]
            params: dict[str, str] = {
                "get": f"NAME,{','.join(table_variables)}",
                "for": f"{CBSA_GEO_PARAM}:*",
            }
            if api_key:
                params["key"] = api_key

            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data or len(data) < 2:
                raise ValueError(
                    f"Census API returned empty or invalid response for ACS 1-year "
                    f"{vintage} table {table}. Verify that the table is available at "
                    f"{url}"
                )

            headers = data[0]
            rows = data[1:]
            table_df = pd.DataFrame(rows, columns=headers)

            cbsa_col = CBSA_GEO_PARAM
            if cbsa_col not in table_df.columns:
                cbsa_candidates = [
                    column
                    for column in table_df.columns
                    if "metropolitan" in column.lower()
                ]
                if cbsa_candidates:
                    cbsa_col = cbsa_candidates[0]
                else:
                    raise ValueError(
                        "Cannot find CBSA code column in Census API response for "
                        f"table {table}. Available columns: {list(table_df.columns)}. "
                        "Expected a column containing 'metropolitan'."
                    )

            table_df = table_df.rename(columns={cbsa_col: "cbsa_code"})

            for var_code in table_variables:
                if var_code in table_df.columns:
                    table_df[var_code] = pd.to_numeric(
                        table_df[var_code],
                        errors="coerce",
                    )
                    table_df.loc[table_df[var_code] < 0, var_code] = pd.NA

            keep_columns = ["NAME", "cbsa_code", *table_variables]
            frames.append(table_df[keep_columns].copy())

    if not frames:
        raise ValueError(
            f"No ACS 1-year tables were available to fetch for vintage {vintage}."
        )

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["NAME", "cbsa_code"], how="inner")

    logger.info("Fetched ACS 1-year data for %d CBSAs", len(merged))
    merged.attrs["acs1_tables_fetched"] = available_tables
    merged.attrs["acs1_tables_unavailable"] = unavailable_tables
    return merged


def ingest_metro_acs1(
    vintage: int,
    definition_version: str = "glynn_fox_v1",
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch ACS 1-year detailed-table data at CBSA geography and map to metros.

    Fetches requested ACS 1-year detailed tables for all CBSAs, maps them to
    Glynn/Fox metro IDs, derives unemployment rate, and writes a curated
    Parquet file with provenance metadata.

    Parameters
    ----------
    vintage : int
        ACS 1-year vintage year (e.g., 2023).
    definition_version : str
        Metro definition version (default: "glynn_fox_v1").
    project_root : Path, optional
        Project root for output path resolution. Defaults to current directory.
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY environment variable.

    Returns
    -------
    Path
        Path to the written Parquet file.

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    ValueError
        If the API response cannot be parsed or no metros could be mapped.
    """
    ingested_at = datetime.now(UTC)

    # Fetch all CBSA data across the requested ACS1 tables
    df = fetch_acs1_cbsa_data(vintage, api_key=api_key)
    total_cbsas = len(df)
    fetched_tables = df.attrs.get("acs1_tables_fetched", ACS1_TABLES)
    unavailable_tables = df.attrs.get("acs1_tables_unavailable", [])

    # Map CBSA codes to metro IDs
    df["metro_id"] = df["cbsa_code"].apply(cbsa_to_metro_id)
    mapped = df[df["metro_id"].notna()].copy()
    dropped = total_cbsas - len(mapped)

    logger.info(
        f"CBSA-to-metro mapping: {len(mapped)} of {total_cbsas} CBSAs mapped "
        f"to Glynn/Fox metros ({dropped} CBSAs dropped)"
    )

    if mapped.empty:
        raise ValueError(
            f"No CBSAs from the ACS 1-year {vintage} response could be mapped "
            f"to Glynn/Fox metros. Check that METRO_CBSA_MAPPING in "
            f"hhplab.metro.definitions is correct."
        )

    # Add metro name
    mapped["metro_name"] = mapped["metro_id"].apply(metro_name_for_id)

    # Rename raw Census variables to friendly names
    mapped = mapped.rename(columns=ACS1_VARIABLE_NAMES)

    # Compute derived unemployment rate.
    mapped["unemployment_rate_acs1"] = pd.NA
    valid_denom = (
        mapped["civilian_labor_force"].notna()
        & (mapped["civilian_labor_force"] > 0)
    )
    mapped.loc[valid_denom, "unemployment_rate_acs1"] = (
        mapped.loc[valid_denom, "unemployed_count"]
        / mapped.loc[valid_denom, "civilian_labor_force"]
    )

    # Add stable schema columns for tables unavailable in earlier vintages.
    for column_name in ACS1_INTEGER_COLUMNS + ACS1_FLOAT_COLUMNS:
        if column_name not in mapped.columns:
            mapped[column_name] = pd.NA

    # Add provenance columns.
    api_url = CENSUS_API_ACS1.format(year=vintage)
    mapped["data_source"] = "census_acs1"
    mapped["source_ref"] = f"{api_url}?tables={'+'.join(fetched_tables)}"
    mapped["ingested_at"] = ingested_at
    mapped["acs1_vintage"] = str(vintage)
    mapped["definition_version"] = definition_version

    # Ensure proper column types.
    mapped["metro_id"] = mapped["metro_id"].astype(str)
    mapped["cbsa_code"] = mapped["cbsa_code"].astype(str)

    for col in ACS1_INTEGER_COLUMNS:
        if col in mapped.columns:
            mapped[col] = mapped[col].astype("Int64")

    for col in ACS1_FLOAT_COLUMNS:
        if col in mapped.columns:
            mapped[col] = mapped[col].astype("Float64")

    # Reorder columns to canonical order
    col_order = [c for c in ACS1_METRO_OUTPUT_COLUMNS if c in mapped.columns]
    result = mapped[col_order].copy()

    # Sort by metro_id for deterministic output
    result = result.sort_values("metro_id").reset_index(drop=True)

    # Write output
    base_dir = Path("data") if project_root is None else project_root / "data"
    output_path = naming.acs1_metro_path(vintage, definition_version, base_dir=base_dir)

    provenance = ProvenanceBlock(
        acs_vintage=str(vintage),
        geo_type="metro",
        definition_version=definition_version,
        extra={
            "dataset_type": "metro_acs1",
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
            "total_cbsas_fetched": total_cbsas,
            "cbsas_mapped": len(result),
            "cbsas_dropped": dropped,
            "cbsa_mapping_version": definition_version,
        },
    )

    write_parquet_with_provenance(result, output_path, provenance)
    logger.info(
        "Wrote ACS 1-year metro data to %s (%d metros)",
        output_path,
        len(result),
    )

    return output_path
