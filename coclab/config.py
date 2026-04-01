"""Runtime configuration with layered precedence for storage roots.

Provides ``load_config()`` to resolve ``asset_store_root`` and
``output_root`` from multiple configuration surfaces, merged in this
precedence order (highest wins):

1. Explicit keyword arguments (CLI flags)
2. Environment variables (``COCLAB_ASSET_STORE_ROOT``, ``COCLAB_OUTPUT_ROOT``)
3. Repo-local config file (``<project_root>/coclab.yaml``)
4. User config file (``~/.config/coclab/config.yaml``)
5. Built-in defaults

Built-in defaults preserve current behavior:

- ``asset_store_root = <project_root>/data``
- ``output_root = <project_root>/data/curated/panel``
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment variable names
# ---------------------------------------------------------------------------

ENV_ASSET_STORE_ROOT = "COCLAB_ASSET_STORE_ROOT"
ENV_OUTPUT_ROOT = "COCLAB_OUTPUT_ROOT"

# ---------------------------------------------------------------------------
# Well-known config file paths
# ---------------------------------------------------------------------------

REPO_CONFIG_FILENAME = "coclab.yaml"
USER_CONFIG_DIR = Path("~/.config/coclab").expanduser()
USER_CONFIG_PATH = USER_CONFIG_DIR / "config.yaml"


@dataclass(frozen=True)
class StorageConfig:
    """Resolved storage root configuration.

    Attributes
    ----------
    asset_store_root : Path
        Root directory for reusable CoC-Lab internal assets (raw snapshots,
        curated ingests, crosswalks, registries, aggregated artifacts).
    output_root : Path
        Root directory for downstream-consumable products (recipe-built
        panels, diagnostics).
    """

    asset_store_root: Path
    output_root: Path


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------


def _load_yaml_file(path: Path) -> dict:
    """Load a YAML config file, returning an empty dict on any failure."""
    try:
        if not path.is_file():
            return {}
        text = path.read_text(encoding="utf-8")
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        logger.debug("Could not load config from %s", path, exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_config(
    *,
    asset_store_root: Path | str | None = None,
    output_root: Path | str | None = None,
    project_root: Path | None = None,
) -> StorageConfig:
    """Load storage configuration with layered precedence.

    Parameters
    ----------
    asset_store_root : Path or str, optional
        Explicit override (highest precedence), typically from a CLI flag.
    output_root : Path or str, optional
        Explicit override (highest precedence), typically from a CLI flag.
    project_root : Path, optional
        Repository root used for locating ``coclab.yaml`` and computing
        built-in defaults.  Defaults to the current working directory.

    Returns
    -------
    StorageConfig
        Fully resolved storage configuration.
    """
    if project_root is None:
        project_root = Path.cwd()
    project_root = Path(project_root).resolve()

    # Layer 4 — user config (~/.config/coclab/config.yaml)
    user_cfg = _load_yaml_file(USER_CONFIG_PATH)

    # Layer 3 — repo-local config (<project_root>/coclab.yaml)
    repo_cfg = _load_yaml_file(project_root / REPO_CONFIG_FILENAME)

    # Layer 2 — environment variables
    env_asset = os.environ.get(ENV_ASSET_STORE_ROOT)
    env_output = os.environ.get(ENV_OUTPUT_ROOT)

    # Layer 5 — built-in defaults (backward compatible)
    default_asset = project_root / "data"
    default_output = project_root / "data" / "curated" / "panel"

    # Resolve asset_store_root: CLI > env > repo > user > default
    resolved_asset = _resolve_value(
        cli=asset_store_root,
        env=env_asset,
        repo=repo_cfg.get("asset_store_root"),
        user=user_cfg.get("asset_store_root"),
        default=default_asset,
    )

    # Resolve output_root: CLI > env > repo > user > default
    resolved_output = _resolve_value(
        cli=output_root,
        env=env_output,
        repo=repo_cfg.get("output_root"),
        user=user_cfg.get("output_root"),
        default=default_output,
    )

    return StorageConfig(
        asset_store_root=Path(resolved_asset),
        output_root=Path(resolved_output),
    )


def _resolve_value(
    *,
    cli: Path | str | None,
    env: str | None,
    repo: str | None,
    user: str | None,
    default: Path,
) -> Path:
    """Pick the highest-precedence non-None value and return as Path."""
    for candidate in (cli, env, repo, user):
        if candidate is not None:
            return Path(candidate)
    return default
