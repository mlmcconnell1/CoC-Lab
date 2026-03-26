"""Helpers for named build directories.

Provides build-directory resolution, manifest I/O, and aggregate-run
recording for the optional ``--build`` workflow.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_BUILDS_DIR = Path("builds")


def resolve_build_dir(name: str, builds_dir: Path | None = None) -> Path:
    """Resolve the root directory for a named build."""
    base = builds_dir if builds_dir is not None else DEFAULT_BUILDS_DIR
    return Path(base) / name


def build_curated_dir(build_dir: Path) -> Path:
    """Return the curated data directory for a build."""
    return build_dir / "data" / "curated"


def build_manifest_path(build_dir: Path) -> Path:
    """Return the manifest.json path for a build."""
    return build_dir / "manifest.json"


# ---------------------------------------------------------------------------
# Manifest I/O
# ---------------------------------------------------------------------------


def read_build_manifest(build_dir: Path) -> dict:
    """Read and return the build manifest as a dict.

    Raises:
        FileNotFoundError: if manifest.json is missing.
        json.JSONDecodeError: if manifest is invalid JSON.
    """
    manifest_path = build_manifest_path(build_dir)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    return json.loads(manifest_path.read_text())


def get_build_years(build_dir: Path) -> list[int]:
    """Return the sorted year list from a build manifest."""
    manifest = read_build_manifest(build_dir)
    return manifest.get("build", {}).get("years", [])


def record_aggregate_run(
    build_dir: Path,
    *,
    dataset: str,
    alignment: str,
    years_requested: list[int],
    years_materialized: list[int] | None = None,
    alignment_params: dict | None = None,
    outputs: list[str] | None = None,
    status: str = "success",
    error: str | None = None,
) -> dict:
    """Append an aggregate-run entry to the build manifest."""
    import uuid

    manifest = read_build_manifest(build_dir)

    run_entry: dict = {
        "run_id": uuid.uuid4().hex[:12],
        "dataset": dataset,
        "invoked_at": datetime.now(UTC).isoformat(),
        "years_requested": years_requested,
        "years_materialized": years_materialized or years_requested,
        "alignment": {"mode": alignment},
        "outputs": outputs or [],
        "status": status,
    }
    if alignment_params:
        run_entry["alignment"].update(alignment_params)
    if error:
        run_entry["error"] = error

    manifest.setdefault("aggregate_runs", []).append(run_entry)

    manifest_path = build_manifest_path(build_dir)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    return run_entry


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def list_builds(builds_dir: Path | None = None) -> list[Path]:
    """List named build directories."""
    base = builds_dir if builds_dir is not None else DEFAULT_BUILDS_DIR
    base = Path(base)
    if not base.exists():
        return []
    return sorted([path for path in base.iterdir() if path.is_dir()])


def require_build_dir(name: str, builds_dir: Path | None = None) -> Path:
    """Resolve a named build directory, raising if it does not exist."""
    build_dir = resolve_build_dir(name, builds_dir=builds_dir)
    if not build_dir.exists():
        raise FileNotFoundError(build_dir)
    return build_dir
