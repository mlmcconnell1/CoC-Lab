"""CLI command for ingesting NHGIS tract shapefiles."""

from pathlib import Path
from typing import Annotated

import typer

# Output directory matches the census ingest modules
OUTPUT_DIR = Path("data/curated/census")


def ingest_nhgis(
    years: Annotated[
        list[int],
        typer.Option(
            "--year",
            "-y",
            help="Census year(s) to download (2010, 2020). Can specify multiple.",
        ),
    ],
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            envvar="IPUMS_API_KEY",
            help="IPUMS API key. Can also set IPUMS_API_KEY environment variable.",
        ),
    ] = None,
    poll_interval: Annotated[
        int,
        typer.Option(
            "--poll-interval",
            help="Minutes between status checks while waiting for extract.",
        ),
    ] = 2,
    max_wait: Annotated[
        int,
        typer.Option(
            "--max-wait",
            help="Maximum minutes to wait for extract completion.",
        ),
    ] = 60,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Download census tract shapefiles from NHGIS.

    Submits an extract request to NHGIS via the IPUMS API, waits for
    completion, and downloads the national tract shapefile. This is
    especially useful for 2010 tracts, which TIGER distributes as
    3,000+ county-level files.

    Requires an IPUMS API key. Get one at:
    https://account.ipums.org/api_keys

    Examples:

        coclab ingest-nhgis --year 2010 --year 2020

        coclab ingest-nhgis --year 2010 --poll-interval 5

        IPUMS_API_KEY=your_key coclab ingest-nhgis --year 2020
    """
    from coclab.naming import tract_filename
    from coclab.nhgis.ingest import SUPPORTED_YEARS, NhgisExtractError, ingest_nhgis_tracts

    # Validate API key
    if not api_key:
        typer.echo(
            "Error: IPUMS API key required.\n"
            "Set IPUMS_API_KEY environment variable or use --api-key.\n"
            "Get a key at: https://account.ipums.org/api_keys",
            err=True,
        )
        raise typer.Exit(1)

    # Validate years
    invalid_years = [y for y in years if y not in SUPPORTED_YEARS]
    if invalid_years:
        supported = ", ".join(str(y) for y in sorted(SUPPORTED_YEARS))
        typer.echo(
            f"Error: Unsupported year(s): {invalid_years}. Supported: {supported}",
            err=True,
        )
        raise typer.Exit(1)

    # Track results
    downloaded = []
    skipped = []
    failed = []

    for year in years:
        output_path = OUTPUT_DIR / tract_filename(year)

        # Check if file exists
        if output_path.exists() and not force:
            typer.echo(f"File exists for {year}: {output_path}")
            typer.echo("  Use --force to re-download.")
            skipped.append(year)
            continue

        if output_path.exists() and force:
            typer.echo(f"Forcing re-download for {year} (removing existing file)")

        typer.echo(f"\nIngesting NHGIS tracts for {year}...")
        typer.echo(f"  Poll interval: {poll_interval} minutes")
        typer.echo(f"  Max wait: {max_wait} minutes")
        typer.echo("")

        def progress(msg: str) -> None:
            typer.echo(f"  {msg}")

        try:
            result_path = ingest_nhgis_tracts(
                year=year,
                api_key=api_key,
                poll_interval_minutes=poll_interval,
                max_wait_minutes=max_wait,
                progress_callback=progress,
            )
            downloaded.append((year, result_path))
            typer.echo(f"  Success: {result_path}")
        except NhgisExtractError as e:
            typer.echo(f"  Error: {e}", err=True)
            failed.append((year, str(e)))
        except Exception as e:
            typer.echo(f"  Unexpected error: {e}", err=True)
            failed.append((year, str(e)))

    # Summary
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("NHGIS INGEST SUMMARY")
    typer.echo("=" * 60)

    if downloaded:
        typer.echo(f"\nDownloaded ({len(downloaded)}):")
        for year, path in downloaded:
            typer.echo(f"  {year}: {path}")

    if skipped:
        typer.echo(f"\nSkipped - already exists ({len(skipped)}):")
        for year in skipped:
            typer.echo(f"  {year}")

    if failed:
        typer.echo(f"\nFailed ({len(failed)}):")
        for year, error in failed:
            typer.echo(f"  {year}: {error}")

    typer.echo("")

    if failed:
        raise typer.Exit(1)
