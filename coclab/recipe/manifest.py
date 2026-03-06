"""Recipe-level provenance manifest and replication export.

Records exactly which base assets (datasets, crosswalks) were consumed
during a recipe execution, with full metadata (paths, SHA-256 hashes,
sizes).  Provides the ability to export a self-contained bundle that a
replicator can use to reproduce the build.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class AssetRecord:
    """Record of a single asset consumed during recipe execution."""

    role: str  # "dataset" or "crosswalk"
    path: str  # Project-relative path
    sha256: str
    size: int
    dataset_id: str | None = None
    transform_id: str | None = None


@dataclass
class RecipeManifest:
    """Full provenance manifest for a recipe execution.

    Contains the recipe identity, execution timestamp, and a complete
    list of every file consumed during the build — enough information
    to verify reproducibility or bundle assets for replication.
    """

    recipe_name: str
    recipe_version: int
    pipeline_id: str
    executed_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
    )
    assets: list[AssetRecord] = field(default_factory=list)
    datasets: dict[str, dict] = field(default_factory=dict)
    transforms: dict[str, str] = field(default_factory=dict)
    output_path: str | None = None

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict) -> RecipeManifest:
        """Deserialize from a dictionary."""
        assets = [AssetRecord(**a) for a in data.get("assets", [])]
        return cls(
            recipe_name=data["recipe_name"],
            recipe_version=data["recipe_version"],
            pipeline_id=data["pipeline_id"],
            executed_at=data.get(
                "executed_at", datetime.now(UTC).isoformat(),
            ),
            assets=assets,
            datasets=data.get("datasets", {}),
            transforms=data.get("transforms", {}),
            output_path=data.get("output_path"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> RecipeManifest:
        """Deserialize from a JSON string."""
        return cls.from_dict(json.loads(json_str))


def write_manifest(manifest: RecipeManifest, path: Path) -> Path:
    """Write a manifest JSON file to disk."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.to_json(), encoding="utf-8")
    return path


def read_manifest(path: Path) -> RecipeManifest:
    """Read a manifest JSON file from disk."""
    return RecipeManifest.from_json(Path(path).read_text(encoding="utf-8"))


def export_bundle(
    manifest: RecipeManifest,
    project_root: Path,
    output_dir: Path,
) -> Path:
    """Copy all consumed assets into a self-contained replication bundle.

    Creates *output_dir* with:
    - ``manifest.json`` — the provenance manifest
    - ``assets/`` — copies of every consumed file, preserving relative paths

    Parameters
    ----------
    manifest : RecipeManifest
        The provenance manifest to export.
    project_root : Path
        Project root used to resolve asset paths.
    output_dir : Path
        Destination directory for the bundle.

    Returns
    -------
    Path
        The output directory.
    """
    output_dir = Path(output_dir)
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    for asset in manifest.assets:
        src = project_root / asset.path
        if not src.exists():
            continue
        dst = assets_dir / asset.path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    write_manifest(manifest, output_dir / "manifest.json")
    return output_dir
