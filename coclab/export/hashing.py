"""File hashing utilities for export bundles."""

import hashlib
from pathlib import Path

# Chunk size for reading files (64KB)
_CHUNK_SIZE = 64 * 1024


def compute_sha256(path: Path) -> str:
    """
    Compute SHA-256 hash of a file.

    Args:
        path: Path to file to hash

    Returns:
        Hex digest string of SHA-256 hash (lowercase)

    Raises:
        FileNotFoundError: If file doesn't exist
        OSError: If file cannot be read
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    sha256_hash = hashlib.sha256()

    try:
        with path.open("rb") as f:
            while chunk := f.read(_CHUNK_SIZE):
                sha256_hash.update(chunk)
    except OSError as e:
        raise OSError(f"Cannot read file: {path}") from e

    return sha256_hash.hexdigest()


def hash_bundle_files(bundle_root: Path) -> dict[str, str]:
    """
    Hash all files in a bundle directory.

    Args:
        bundle_root: Root directory of export bundle

    Returns:
        Dict mapping relative paths to SHA-256 hex digests

    Note:
        Excludes MANIFEST.json from hashing (it contains hashes)
    """
    result: dict[str, str] = {}

    for file_path in bundle_root.rglob("*"):
        # Skip directories, only hash files
        if not file_path.is_file():
            continue

        # Skip MANIFEST.json as it contains the hashes
        if file_path.name == "MANIFEST.json":
            continue

        relative_path = file_path.relative_to(bundle_root)
        result[str(relative_path)] = compute_sha256(file_path)

    return result


def verify_file_hash(path: Path, expected_hash: str) -> bool:
    """
    Verify that a file matches an expected SHA-256 hash.

    Args:
        path: Path to file
        expected_hash: Expected SHA-256 hex digest

    Returns:
        True if hash matches, False otherwise
    """
    try:
        actual_hash = compute_sha256(path)
        return actual_hash.lower() == expected_hash.lower()
    except (FileNotFoundError, OSError):
        return False
