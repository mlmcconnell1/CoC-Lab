"""CLI command for building CoC x year panels."""

from pathlib import Path
from typing import Annotated

import typer


def build_panel_cmd(
    start: Annotated[
        int,
        typer.Option(
            "--start",
            "-s",
            help="First PIT year to include in the panel (inclusive).",
        ),
    ],
    end: Annotated[
        int,
        typer.Option(
            "--end",
            "-e",
            help="Last PIT year to include in the panel (inclusive).",
        ),
    ],
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method for ACS measures: 'population' or 'area'.",
        ),
    ] = "population",
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Custom output path for the panel Parquet file.",
        ),
    ] = None,
) -> None:
    """Build a CoC x year analysis panel.

    Constructs an analysis-ready panel by joining PIT counts with ACS measures
    for each year in the specified range, using alignment policies to determine
    which boundary and ACS vintages to use.

    Examples:

        coclab build-panel --start 2018 --end 2024

        coclab build-panel --start 2018 --end 2024 --weighting population

        coclab build-panel --start 2018 --end 2024 --weighting area

        coclab build-panel --start 2020 --end 2024 --output custom_panel.parquet
    """
    from coclab.panel import AlignmentPolicy, build_panel, save_panel
    from coclab.panel.policies import default_acs_vintage, default_boundary_vintage

    # Validate weighting method
    valid_weighting = {"population", "area"}
    if weighting not in valid_weighting:
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. "
            f"Must be one of: {', '.join(sorted(valid_weighting))}",
            err=True,
        )
        raise typer.Exit(1)

    # Validate year range
    if start > end:
        typer.echo(
            f"Error: Start year ({start}) must be less than or equal to end year ({end}).",
            err=True,
        )
        raise typer.Exit(1)

    typer.echo(f"Building panel for {start}-{end} with {weighting} weighting...")

    # Create alignment policy
    policy = AlignmentPolicy(
        boundary_vintage_func=default_boundary_vintage,
        acs_vintage_func=default_acs_vintage,
        weighting_method=weighting,  # type: ignore[arg-type]
    )

    # Build the panel
    try:
        panel_df = build_panel(
            start_year=start,
            end_year=end,
            policy=policy,
        )
    except Exception as e:
        typer.echo(f"Error building panel: {e}", err=True)
        raise typer.Exit(1) from e

    if panel_df.empty:
        typer.echo("Warning: Panel is empty. No data found for the specified year range.")
        raise typer.Exit(1)

    # Save the panel
    try:
        if output:
            # Custom output path
            output_dir = output.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            from coclab.panel.assemble import PANEL_COLUMNS
            from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

            provenance = ProvenanceBlock(
                weighting=weighting,
                extra={
                    "dataset_type": "coc_panel",
                    "start_year": start,
                    "end_year": end,
                    "row_count": len(panel_df),
                    "coc_count": int(panel_df["coc_id"].nunique()),
                    "year_count": int(panel_df["year"].nunique()),
                    "policy": policy.to_dict(),
                },
            )
            write_parquet_with_provenance(panel_df, output, provenance)
            output_path = output
        else:
            # Default output path
            output_path = save_panel(
                df=panel_df,
                start_year=start,
                end_year=end,
                policy=policy,
            )
        typer.echo(f"Saved panel to: {output_path}")
    except Exception as e:
        typer.echo(f"Error saving panel: {e}", err=True)
        raise typer.Exit(1) from e

    # Display summary
    typer.echo("")
    typer.echo("Panel Summary:")
    typer.echo(f"  Years: {start} - {end} ({panel_df['year'].nunique()} years)")
    typer.echo(f"  CoCs: {panel_df['coc_id'].nunique()}")
    typer.echo(f"  Total rows: {len(panel_df)}")
    typer.echo(f"  Weighting: {weighting}")

    # Coverage statistics
    if "coverage_ratio" in panel_df.columns:
        coverage = panel_df["coverage_ratio"].dropna()
        if len(coverage) > 0:
            typer.echo("")
            typer.echo("Coverage Statistics:")
            typer.echo(f"  Mean coverage ratio: {coverage.mean():.3f}")
            typer.echo(f"  Min coverage ratio: {coverage.min():.3f}")
            typer.echo(f"  Max coverage ratio: {coverage.max():.3f}")
            low_coverage = (coverage < 0.9).sum()
            typer.echo(f"  Low coverage (<0.9): {low_coverage} observations")

    # Boundary changes
    if "boundary_changed" in panel_df.columns:
        changes = panel_df["boundary_changed"].sum()
        if changes > 0:
            typer.echo("")
            typer.echo(f"Boundary Changes: {int(changes)} observations had boundary changes")

    typer.echo("")
    typer.echo(f"Output: {output_path}")
