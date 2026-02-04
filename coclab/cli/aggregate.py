"""CLI command group for aggregating datasets to CoC level.

Provides stub commands for acs, zori, pep, and pit dataset aggregation.
Each command validates inputs (build existence, alignment mode) and prints
what it would do, but does not run actual aggregation logic yet.
"""

from __future__ import annotations

from typing import Annotated

import typer

from coclab.builds import require_build_dir, resolve_build_dir
from coclab.year_spec import parse_year_spec

aggregate_app = typer.Typer(
    name="aggregate",
    help="Aggregate datasets to CoC level.",
    no_args_is_help=True,
)

# ---------------------------------------------------------------------------
# Valid alignment modes per dataset
# ---------------------------------------------------------------------------

PEP_ALIGN_MODES = ("as_of_july", "to_calendar_year", "to_pit_year", "lagged")
PIT_ALIGN_MODES = ("point_in_time_jan", "to_calendar_year")
ACS_ALIGN_MODES = ("vintage_end_year", "window_center_year", "as_reported")
ZORI_ALIGN_MODES = ("monthly_native", "pit_january", "calendar_year_average")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _validate_build(build: str) -> None:
    """Validate that the named build directory exists.

    Raises ``typer.Exit(2)`` with a helpful message when the build is missing.
    """
    try:
        require_build_dir(build)
    except FileNotFoundError:
        build_path = resolve_build_dir(build)
        typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
        typer.echo("Run: coclab build create --name <build>", err=True)
        raise typer.Exit(2) from None


def _validate_align(align: str, valid_modes: tuple[str, ...], dataset: str) -> None:
    """Validate that *align* is one of *valid_modes* for *dataset*."""
    if align not in valid_modes:
        typer.echo(
            f"Error: Invalid alignment mode '{align}' for {dataset}. "
            f"Valid modes: {', '.join(valid_modes)}",
            err=True,
        )
        raise typer.Exit(2)


def _validate_years(years: str | None) -> list[int] | None:
    """Parse ``--years`` if provided, returning ``None`` when omitted."""
    if years is None:
        return None
    try:
        return parse_year_spec(years)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


def _print_stub(
    dataset: str,
    build: str,
    align: str,
    years: list[int] | None,
    extras: dict[str, object] | None = None,
) -> None:
    """Print what the command would do and exit with a not-yet-implemented message."""
    typer.echo(
        f"Would aggregate {dataset} for build '{build}' with alignment '{align}'"
    )
    if years is not None:
        typer.echo(f"  Years: {years}")
    else:
        typer.echo("  Years: (build default)")
    if extras:
        for key, value in extras.items():
            typer.echo(f"  {key}: {value}")
    typer.echo("Not yet implemented.", err=True)
    raise typer.Exit(1)


# ---------------------------------------------------------------------------
# pep
# ---------------------------------------------------------------------------


@aggregate_app.command("pep")
def aggregate_pep(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: as_of_july, to_calendar_year, to_pit_year, lagged."
            ),
        ),
    ] = "as_of_july",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
    lag_years: Annotated[
        int | None,
        typer.Option(
            "--lag-years",
            help="Number of lag years (required when --align=lagged).",
        ),
    ] = None,
) -> None:
    """Aggregate PEP population estimates to CoC level."""
    _validate_build(build)
    _validate_align(align, PEP_ALIGN_MODES, "pep")
    parsed_years = _validate_years(years)

    if align == "lagged" and lag_years is None:
        typer.echo(
            "Error: --lag-years is required when --align=lagged.",
            err=True,
        )
        raise typer.Exit(2)

    extras: dict[str, object] = {}
    if lag_years is not None:
        extras["lag-years"] = lag_years

    _print_stub("pep", build, align, parsed_years, extras=extras or None)


# ---------------------------------------------------------------------------
# pit
# ---------------------------------------------------------------------------


@aggregate_app.command("pit")
def aggregate_pit(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: point_in_time_jan, to_calendar_year."
            ),
        ),
    ] = "point_in_time_jan",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
) -> None:
    """Aggregate PIT counts to CoC level."""
    _validate_build(build)
    _validate_align(align, PIT_ALIGN_MODES, "pit")
    parsed_years = _validate_years(years)
    _print_stub("pit", build, align, parsed_years)


# ---------------------------------------------------------------------------
# acs
# ---------------------------------------------------------------------------


@aggregate_app.command("acs")
def aggregate_acs(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: vintage_end_year, window_center_year, as_reported."
            ),
        ),
    ] = "vintage_end_year",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
    acs_vintage: Annotated[
        str | None,
        typer.Option(
            "--acs-vintage",
            help="ACS 5-year estimate vintage (e.g. '2019-2023').",
        ),
    ] = None,
) -> None:
    """Aggregate ACS estimates to CoC level."""
    _validate_build(build)
    _validate_align(align, ACS_ALIGN_MODES, "acs")
    parsed_years = _validate_years(years)

    extras: dict[str, object] = {}
    if acs_vintage is not None:
        extras["acs-vintage"] = acs_vintage

    _print_stub("acs", build, align, parsed_years, extras=extras or None)


# ---------------------------------------------------------------------------
# zori
# ---------------------------------------------------------------------------


@aggregate_app.command("zori")
def aggregate_zori(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: monthly_native, pit_january, calendar_year_average."
            ),
        ),
    ] = "monthly_native",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
) -> None:
    """Aggregate ZORI rent indices to CoC level."""
    _validate_build(build)
    _validate_align(align, ZORI_ALIGN_MODES, "zori")
    parsed_years = _validate_years(years)
    _print_stub("zori", build, align, parsed_years)
