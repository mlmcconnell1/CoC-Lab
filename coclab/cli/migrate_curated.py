"""CLI command for curated data migration."""

from pathlib import Path
from typing import Annotated

import typer

from coclab.curated_migrate import apply_migration, scan_curated_for_migration


def migrate_curated_cmd(
    base_dir: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Path to the curated data directory.",
        ),
    ] = Path("data/curated"),
    apply: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Apply renames (default is dry-run only).",
        ),
    ] = False,
) -> None:
    """Scan curated data for legacy filenames and propose canonical renames.

    By default runs in dry-run mode showing what would change.
    Pass --apply to execute renames.

    Examples:

        coclab migrate curated-layout

        coclab migrate curated-layout --apply
    """
    plan = scan_curated_for_migration(base_dir)

    total = len(plan.renames) + len(plan.duplicates) + len(plan.unknown)
    if total == 0:
        typer.echo("No migration candidates found. Curated layout is clean.")
        return

    log = apply_migration(plan, dry_run=not apply)
    for line in log:
        typer.echo(f"  {line}")

    typer.echo("")
    typer.echo(
        f"Renames: {len(plan.renames)}  "
        f"Duplicates: {len(plan.duplicates)}  "
        f"Unknown: {len(plan.unknown)}"
    )

    if not apply and plan.renames:
        typer.echo("\nPass --apply to execute renames.")
