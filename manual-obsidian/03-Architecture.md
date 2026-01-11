# Architecture

## System Overview

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        HUD_EX[HUD Exchange GIS Tools]
        HUD_OD[HUD Open Data ArcGIS]
    end

    subgraph Ingest["Ingestion Layer"]
        ING_EX[hud_exchange_gis.py]
        ING_OD[hud_opendata_arcgis.py]
    end

    subgraph Processing["Processing Layer"]
        NORM[normalize.py]
        VAL[validate.py]
    end

    subgraph Storage["Storage Layer"]
        RAW[(data/raw/)]
        CURATED[(data/curated/)]
        REG[(boundary_registry.parquet)]
    end

    subgraph Output["Output Layer"]
        VIZ[map_folium.py]
        HTML[Interactive HTML Maps]
    end

    HUD_EX --> ING_EX
    HUD_OD --> ING_OD
    ING_EX --> RAW
    ING_OD --> RAW
    RAW --> NORM
    NORM --> VAL
    VAL --> CURATED
    CURATED --> REG
    CURATED --> VIZ
    VIZ --> HTML
```

## Module Structure

```mermaid
graph LR
    subgraph coclab
        CLI[cli/]
        ING[ingest/]
        GEO[geo/]
        REG[registry/]
        VIZ[viz/]
        CENSUS[census/]
        XWALK[xwalks/]
        MEASURES[measures/]
    end

    CLI --> ING
    CLI --> REG
    CLI --> VIZ
    CLI --> XWALK
    CLI --> MEASURES
    ING --> GEO
    VIZ --> REG
    VIZ --> GEO
    XWALK --> CENSUS
    MEASURES --> XWALK
```

## Directory Layout

```
coclab/
  cli/          # CLI commands (Typer)
  geo/          # Geometry normalization and validation
  ingest/       # Data source ingesters
  registry/     # Vintage tracking and version selection
  viz/          # Map rendering (Folium)
  census/       # Census geometry ingestion (TIGER/Line)
    ingest/     # Tract and county downloaders
  xwalks/       # CoC-to-census crosswalk builders
  measures/     # ACS measure aggregation and diagnostics
  acs/          # ACS population ingest, rollup, and cross-check
    ingest/     # Tract population fetcher
  rents/        # ZORI rent data ingestion and aggregation
  pit/          # PIT count ingestion and QA (Phase 3)
    ingest/     # HUD Exchange PIT downloaders and parsers
  panel/        # CoC × year panel assembly (Phase 3)
data/
  raw/          # Downloaded source files
  curated/      # Processed GeoParquet files
    census/     # TIGER tract/county geometries
    xwalks/     # CoC-tract and CoC-county crosswalks
    measures/   # CoC-level demographic measures
    acs/        # ACS tract population, rollups, and county weights
    rents/      # ZORI rent data (county and CoC-level)
    pit/        # Canonical PIT count files
    panels/     # CoC × year analysis panels
tests/          # Test suite including smoke tests
```

---

**Previous:** [[02-Installation]] | **Next:** [[04-CLI-Reference]]
