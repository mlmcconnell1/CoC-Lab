"""Bootstrap built-in recipe adapters."""

from __future__ import annotations

from coclab.recipe.adapters import dataset_registry, geometry_registry
from coclab.recipe.default_dataset_adapters import register_dataset_defaults
from coclab.recipe.default_geometry_adapters import register_geometry_defaults


def register_defaults() -> None:
    """Register all built-in geometry and dataset adapters.

    Safe to call multiple times (idempotent — re-registration overwrites
    with the same adapter function).
    """
    register_geometry_defaults(geometry_registry)
    register_dataset_defaults(dataset_registry)
