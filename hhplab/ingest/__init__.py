"""Compatibility entrypoints for legacy generic ingest imports.

New source-owned public APIs live under packages such as ``hhplab.hud`` and
``hhplab.bls``. The legacy ``hhplab.ingest`` namespace remains available as a
thin compatibility layer for existing callers and tests.
"""

from hhplab.ingest.hud_exchange_gis import ingest_hud_exchange
from hhplab.ingest.hud_opendata_arcgis import ingest_hud_opendata

__all__ = ["ingest_hud_exchange", "ingest_hud_opendata"]
