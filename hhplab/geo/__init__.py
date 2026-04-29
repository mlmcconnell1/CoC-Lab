"""Geospatial utilities for CoC boundary processing."""

from hhplab.geo.io import read_geoparquet, write_geoparquet
from hhplab.geo.normalize import (
    compute_geom_hash,
    ensure_polygon_type,
    fix_geometry,
    normalize_boundaries,
    normalize_crs,
)
from hhplab.geo.validate import ValidationResult, validate_boundaries

__all__ = [
    "normalize_boundaries",
    "normalize_crs",
    "fix_geometry",
    "compute_geom_hash",
    "ensure_polygon_type",
    "read_geoparquet",
    "write_geoparquet",
    "validate_boundaries",
    "ValidationResult",
]
