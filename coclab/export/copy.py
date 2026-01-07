"""File copy operations for export bundle generation."""

import os
import shutil
from pathlib import Path

from coclab.export.types import ArtifactRecord, SelectionPlan


def create_bundle_structure(bundle_root: Path) -> None:
    """Create the directory structure for an export bundle.

    Creates:
        - {bundle_root}/data/panels/
        - {bundle_root}/data/inputs/boundaries/
        - {bundle_root}/data/inputs/xwalks/
        - {bundle_root}/data/inputs/pit/
        - {bundle_root}/data/inputs/rents/
        - {bundle_root}/data/inputs/acs/
        - {bundle_root}/diagnostics/
        - {bundle_root}/codebook/
    """
    dirs = [
        bundle_root / "data" / "panels",
        bundle_root / "data" / "inputs" / "boundaries",
        bundle_root / "data" / "inputs" / "xwalks",
        bundle_root / "data" / "inputs" / "pit",
        bundle_root / "data" / "inputs" / "rents",
        bundle_root / "data" / "inputs" / "acs",
        bundle_root / "diagnostics",
        bundle_root / "codebook",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def _copy_single_artifact(
    artifact: ArtifactRecord, bundle_root: Path, copy_mode: str
) -> ArtifactRecord:
    """Copy a single artifact and return updated record with file metadata.

    Args:
        artifact: The artifact record to copy
        bundle_root: Root directory of export bundle
        copy_mode: 'copy', 'hardlink', or 'symlink'

    Returns:
        Updated ArtifactRecord with bytes populated from file size

    Raises:
        ValueError: If copy_mode is not valid
        FileNotFoundError: If source file does not exist
    """
    source = artifact.source_path
    dest = bundle_root / artifact.dest_path

    # Ensure destination directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)

    if copy_mode == "copy":
        shutil.copy2(source, dest)
    elif copy_mode == "hardlink":
        os.link(source, dest)
    elif copy_mode == "symlink":
        # Use relative path for symlinks (more portable)
        rel_source = os.path.relpath(source, dest.parent)
        os.symlink(rel_source, dest)
    else:
        raise ValueError(
            f"Invalid copy_mode: {copy_mode}. Must be 'copy', 'hardlink', or 'symlink'"
        )

    # Get file size (for symlinks, get size of target file)
    file_size = dest.stat().st_size if copy_mode != "symlink" else source.stat().st_size

    # Return updated artifact with bytes populated
    return ArtifactRecord(
        role=artifact.role,
        source_path=artifact.source_path,
        dest_path=artifact.dest_path,
        sha256=artifact.sha256,
        bytes=file_size,
        rows=artifact.rows,
        columns=artifact.columns,
        key_columns=artifact.key_columns,
        provenance=artifact.provenance,
    )


def copy_artifacts(
    selection_plan: SelectionPlan, bundle_root: Path, copy_mode: str = "copy"
) -> list[ArtifactRecord]:
    """Copy artifacts to bundle directory.

    Args:
        selection_plan: Plan of artifacts to copy
        bundle_root: Root directory of export bundle
        copy_mode: 'copy', 'hardlink', or 'symlink'

    Returns:
        List of ArtifactRecords with updated paths and metadata (bytes populated)
    """
    all_artifacts = (
        selection_plan.panel_artifacts
        + selection_plan.input_artifacts
        + selection_plan.derived_artifacts
        + selection_plan.diagnostic_artifacts
        + selection_plan.codebook_artifacts
    )

    copied_artifacts = []
    for artifact in all_artifacts:
        copied = _copy_single_artifact(artifact, bundle_root, copy_mode)
        copied_artifacts.append(copied)

    return copied_artifacts
