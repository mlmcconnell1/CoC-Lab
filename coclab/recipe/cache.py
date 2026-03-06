"""Recipe-level asset caching.

Provides in-memory DataFrame caching within a single recipe execution,
avoiding redundant Parquet reads when the same file is accessed for
multiple years.  Also computes and caches file identity (SHA-256) for
provenance tracking.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class FileIdentity:
    """Content-addressed identity of a file on disk."""

    path: str
    sha256: str
    size: int


class RecipeCache:
    """In-memory cache for recipe execution.

    Caches DataFrame reads keyed by absolute file path, avoiding
    redundant reads when the same Parquet file is accessed for
    multiple years.  Also computes and caches file identity (SHA-256)
    for provenance tracking.

    Parameters
    ----------
    enabled : bool
        When False, all reads go directly to disk and no caching
        occurs.  File identity tracking still works.
    """

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self._frames: dict[str, pd.DataFrame] = {}
        self._identities: dict[str, FileIdentity] = {}

    def read_parquet(self, path: Path) -> pd.DataFrame:
        """Read a Parquet file, returning a cached copy when available.

        Returns a *copy* of the cached DataFrame so callers can freely
        filter/mutate without corrupting the cache.
        """
        key = str(path.resolve())
        if self.enabled and key in self._frames:
            return self._frames[key].copy()
        df = pd.read_parquet(path)
        if self.enabled:
            self._frames[key] = df
            return df.copy()
        return df

    def file_identity(self, path: Path) -> FileIdentity:
        """Get or compute file identity (SHA-256, size)."""
        key = str(path.resolve())
        if key in self._identities:
            return self._identities[key]

        stat = path.stat()
        sha256 = _sha256_file(path)
        identity = FileIdentity(
            path=str(path),
            sha256=sha256,
            size=stat.st_size,
        )
        self._identities[key] = identity
        return identity

    @property
    def cached_count(self) -> int:
        """Number of DataFrames currently cached."""
        return len(self._frames)


def _sha256_file(path: Path, chunk_size: int = 65536) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
