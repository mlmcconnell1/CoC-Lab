"""Validation for metro definition artifacts.

Checks structural integrity, identifier formats, and cross-table
referential consistency of the three metro definition tables.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from hhplab.metro.definitions import (
    DEFINITION_VERSION,
    METRO_COUNT,
)


@dataclass
class MetroValidationResult:
    """Result of metro definition validation."""

    passed: bool
    errors: list[str]
    warnings: list[str]

    def summary(self) -> str:
        lines = [
            f"Metro validation: {'PASS' if self.passed else 'FAIL'} "
            f"({len(self.errors)} error(s), {len(self.warnings)} warning(s))"
        ]
        for e in self.errors:
            lines.append(f"  ERROR: {e}")
        for w in self.warnings:
            lines.append(f"  WARN:  {w}")
        return "\n".join(lines)


def validate_metro_artifacts(
    definitions_df: pd.DataFrame,
    coc_membership_df: pd.DataFrame,
    county_membership_df: pd.DataFrame,
) -> MetroValidationResult:
    """Validate metro definition DataFrames for structural and referential integrity.

    Checks:
    - Required columns present in each table.
    - metro_id format: ``GFnn`` (two-digit zero-padded).
    - county_fips format: 5-digit string.
    - coc_id format: ``XX-NNN`` (state abbreviation + 3 digits).
    - Expected metro count matches.
    - All metro_ids in membership tables exist in definitions.
    - definition_version consistency across tables.
    - No duplicate rows.

    Parameters
    ----------
    definitions_df : pd.DataFrame
        Metro definitions table.
    coc_membership_df : pd.DataFrame
        Metro-to-CoC membership table.
    county_membership_df : pd.DataFrame
        Metro-to-county membership table.

    Returns
    -------
    MetroValidationResult
    """
    errors: list[str] = []
    warnings: list[str] = []

    # -- Required columns --------------------------------------------------
    def _check_cols(df: pd.DataFrame, name: str, required: list[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            errors.append(f"{name}: missing columns {missing}")

    _check_cols(
        definitions_df,
        "definitions",
        ["metro_id", "metro_name", "membership_type", "definition_version"],
    )
    _check_cols(
        coc_membership_df,
        "coc_membership",
        ["metro_id", "coc_id", "definition_version"],
    )
    _check_cols(
        county_membership_df,
        "county_membership",
        ["metro_id", "county_fips", "definition_version"],
    )

    # -- metro_id format: GFnn ---------------------------------------------
    import re

    gf_pattern = re.compile(r"^GF\d{2}$")
    for name, df, col in [
        ("definitions", definitions_df, "metro_id"),
        ("coc_membership", coc_membership_df, "metro_id"),
        ("county_membership", county_membership_df, "metro_id"),
    ]:
        if col not in df.columns:
            continue
        bad = [v for v in df[col].unique() if not gf_pattern.match(str(v))]
        if bad:
            errors.append(
                f"{name}: invalid metro_id format (expected GFnn): {bad[:5]}"
            )

    # -- coc_id format: XX-NNN --------------------------------------------
    coc_pattern = re.compile(r"^[A-Z]{2}-\d{3}$")
    if "coc_id" in coc_membership_df.columns:
        bad_cocs = [
            v
            for v in coc_membership_df["coc_id"].unique()
            if not coc_pattern.match(str(v))
        ]
        if bad_cocs:
            errors.append(
                f"coc_membership: invalid coc_id format (expected XX-NNN): "
                f"{bad_cocs[:5]}"
            )

    # -- county_fips format: 5 digits --------------------------------------
    fips_pattern = re.compile(r"^\d{5}$")
    if "county_fips" in county_membership_df.columns:
        bad_fips = [
            v
            for v in county_membership_df["county_fips"].unique()
            if not fips_pattern.match(str(v))
        ]
        if bad_fips:
            errors.append(
                f"county_membership: invalid county_fips format "
                f"(expected 5 digits): {bad_fips[:5]}"
            )

    # -- Expected metro count ----------------------------------------------
    if "metro_id" in definitions_df.columns:
        actual_count = definitions_df["metro_id"].nunique()
        if actual_count != METRO_COUNT:
            errors.append(
                f"definitions: expected {METRO_COUNT} metros, "
                f"found {actual_count}"
            )

    # -- Referential integrity: all membership metro_ids in definitions -----
    if "metro_id" in definitions_df.columns:
        def_ids = set(definitions_df["metro_id"].unique())

        if "metro_id" in coc_membership_df.columns:
            coc_ids = set(coc_membership_df["metro_id"].unique())
            orphan_coc = coc_ids - def_ids
            if orphan_coc:
                errors.append(
                    f"coc_membership: metro_ids not in definitions: "
                    f"{sorted(orphan_coc)}"
                )
            missing_coc = def_ids - coc_ids
            if missing_coc:
                warnings.append(
                    f"definitions: metros with no CoC membership: "
                    f"{sorted(missing_coc)}"
                )

        if "metro_id" in county_membership_df.columns:
            county_ids = set(county_membership_df["metro_id"].unique())
            orphan_county = county_ids - def_ids
            if orphan_county:
                errors.append(
                    f"county_membership: metro_ids not in definitions: "
                    f"{sorted(orphan_county)}"
                )
            missing_county = def_ids - county_ids
            if missing_county:
                warnings.append(
                    f"definitions: metros with no county membership: "
                    f"{sorted(missing_county)}"
                )

    # -- definition_version consistency ------------------------------------
    for name, df in [
        ("definitions", definitions_df),
        ("coc_membership", coc_membership_df),
        ("county_membership", county_membership_df),
    ]:
        if "definition_version" in df.columns:
            versions = df["definition_version"].unique()
            if len(versions) != 1 or versions[0] != DEFINITION_VERSION:
                errors.append(
                    f"{name}: definition_version mismatch; "
                    f"expected '{DEFINITION_VERSION}', found {list(versions)}"
                )

    # -- No duplicate rows -------------------------------------------------
    if "metro_id" in definitions_df.columns:
        dups = definitions_df["metro_id"].duplicated().sum()
        if dups:
            errors.append(f"definitions: {dups} duplicate metro_id(s)")

    if {"metro_id", "coc_id"} <= set(coc_membership_df.columns):
        dups = coc_membership_df.duplicated(subset=["metro_id", "coc_id"]).sum()
        if dups:
            errors.append(f"coc_membership: {dups} duplicate (metro_id, coc_id) pair(s)")

    if {"metro_id", "county_fips"} <= set(county_membership_df.columns):
        dups = county_membership_df.duplicated(
            subset=["metro_id", "county_fips"]
        ).sum()
        if dups:
            errors.append(
                f"county_membership: {dups} duplicate "
                f"(metro_id, county_fips) pair(s)"
            )

    return MetroValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )
