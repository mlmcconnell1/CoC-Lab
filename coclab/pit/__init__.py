"""PIT (Point-in-Time) homelessness count data processing.

This package provides modules for:
- Ingesting PIT data from HUD Exchange and other sources
- Parsing and canonicalizing PIT counts
- Registry tracking for PIT vintages
- Quality assurance and validation
"""

from coclab.pit.registry import (
    PitRegistryEntry,
    compute_file_hash,
    get_pit_path,
    latest_pit_year,
    list_pit_years,
    register_pit_year,
)

__all__ = [
    "PitRegistryEntry",
    "compute_file_hash",
    "get_pit_path",
    "latest_pit_year",
    "list_pit_years",
    "register_pit_year",
]
