"""Folium-based interactive map rendering for CoC, MSA, and metro overlays."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import folium
import geopandas as gpd
import pandas as pd

from hhplab.paths import curated_dir
from hhplab.registry import latest_vintage

if TYPE_CHECKING:
    from hhplab.recipe.recipe_schema import MapLayerSpec, MapViewportSpec, TargetSpec


DEFAULT_MAP_CENTER = [39.5, -98.35]
DEFAULT_MAP_ZOOM = 4
SUPPORTED_BASEMAPS = {
    "cartodbpositron": "CartoDB positron",
    "openstreetmap": "OpenStreetMap",
}
IDENTIFIER_COLUMNS = {
    "coc": "coc_id",
    "msa": "msa_id",
    "metro": "metro_id",
}
DEFAULT_TOOLTIP_FIELDS = {
    "coc": ["coc_id", "coc_name", "boundary_vintage", "source"],
    "msa": ["msa_id", "msa_name", "cbsa_code", "definition_version"],
    "metro": ["metro_id", "metro_name", "definition_version"],
}


@dataclass
class ResolvedMapLayer:
    """Resolved layer ready for Folium rendering."""

    name: str
    gdf: gpd.GeoDataFrame
    tooltip_fields: list[str]
    show: bool
    style: dict[str, Any]


def _normalize_selector(value: object) -> str:
    return str(value).strip().upper()


def _resolve_basemap(name: str) -> str:
    key = name.strip().lower()
    if key not in SUPPORTED_BASEMAPS:
        raise ValueError(
            f"Unsupported basemap '{name}'. Available: {sorted(SUPPORTED_BASEMAPS)}"
        )
    return SUPPORTED_BASEMAPS[key]


def _normalize_coc_id(coc_id: str) -> str:
    """Normalize CoC ID for case/whitespace robust matching."""
    return _normalize_selector(coc_id)


def _find_coc_boundary_file(vintage: str, *, base_dir: Path) -> Path:
    """Find the GeoParquet file for a given vintage."""
    from hhplab.geo.io import resolve_curated_boundary_path

    return resolve_curated_boundary_path(vintage, base_dir)


def _standardize_county_geometry_columns(county_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "GEOID" in county_gdf.columns:
        result = county_gdf.copy()
        result["county_fips"] = result["GEOID"].astype(str)
        return result
    if "geoid" in county_gdf.columns:
        result = county_gdf.rename(columns={"geoid": "GEOID"}).copy()
        result["county_fips"] = result["GEOID"].astype(str)
        return result
    if "county_fips" in county_gdf.columns:
        result = county_gdf.rename(columns={"county_fips": "GEOID"}).copy()
        result["county_fips"] = result["GEOID"].astype(str)
        return result
    raise ValueError("County geometry artifact must include GEOID, geoid, or county_fips.")


def _load_coc_boundaries(
    boundary_vintage: str,
    *,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    boundary_path = _find_coc_boundary_file(str(boundary_vintage), base_dir=base_dir)
    gdf = gpd.read_parquet(boundary_path)
    if "coc_id" not in gdf.columns:
        raise ValueError("CoC boundary artifact must contain 'coc_id'.")
    return gdf


def _load_counties(
    county_vintage: str | int,
    *,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    from hhplab.naming import county_path

    path = county_path(county_vintage, base_dir)
    gdf = gpd.read_parquet(path)
    return _standardize_county_geometry_columns(gdf)


def _validate_membership_counties(
    membership: pd.DataFrame,
    counties: gpd.GeoDataFrame,
    *,
    label: str,
    county_vintage: str | int,
) -> None:
    available = set(counties["county_fips"].astype(str))
    required = set(membership["county_fips"].astype(str))
    missing = sorted(required - available)
    if not missing:
        return
    preview = ", ".join(missing[:5])
    if len(missing) > 5:
        preview += ", ..."
    raise ValueError(
        f"{label} membership references counties missing from county geometry vintage "
        f"{county_vintage}: {preview}. Run: hhplab ingest tiger --year {county_vintage} --type counties"
    )


def _dissolve_county_membership(
    *,
    counties: gpd.GeoDataFrame,
    membership: pd.DataFrame,
    definitions: pd.DataFrame,
    id_column: str,
    name_columns: list[str],
) -> gpd.GeoDataFrame:
    joined = counties.merge(membership, on="county_fips", how="inner")
    if joined.empty:
        raise ValueError(
            f"County membership did not match any county geometries for '{id_column}'."
        )
    dissolved = joined.dissolve(by=id_column, as_index=False, aggfunc="first")
    merge_columns = [id_column, *name_columns]
    dedup_defs = definitions[merge_columns].drop_duplicates(subset=[id_column])
    dissolved = dissolved.drop(columns=[col for col in name_columns if col in dissolved.columns])
    return dissolved.merge(dedup_defs, on=id_column, how="left")


def _load_msa_boundaries(
    *,
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    from hhplab.msa.io import read_msa_county_membership, read_msa_definitions

    counties = _load_counties(county_vintage, base_dir=base_dir)
    membership = read_msa_county_membership(definition_version, base_dir)
    definitions = read_msa_definitions(definition_version, base_dir)
    _validate_membership_counties(
        membership,
        counties,
        label="MSA",
        county_vintage=county_vintage,
    )
    return _dissolve_county_membership(
        counties=counties,
        membership=membership,
        definitions=definitions,
        id_column="msa_id",
        name_columns=["msa_name", "cbsa_code", "definition_version"],
    )


def _load_metro_boundaries(
    *,
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    from hhplab.metro.io import read_metro_county_membership, read_metro_definitions

    counties = _load_counties(county_vintage, base_dir=base_dir)
    membership = read_metro_county_membership(definition_version, base_dir)
    definitions = read_metro_definitions(definition_version, base_dir)
    _validate_membership_counties(
        membership,
        counties,
        label="Metro",
        county_vintage=county_vintage,
    )
    return _dissolve_county_membership(
        counties=counties,
        membership=membership,
        definitions=definitions,
        id_column="metro_id",
        name_columns=["metro_name", "definition_version"],
    )


def _load_layer_geometries(layer: MapLayerSpec, *, base_dir: Path) -> gpd.GeoDataFrame:
    geo_type = layer.geometry.type
    vintage = layer.geometry.vintage

    if geo_type == "coc":
        boundary_vintage = str(vintage) if vintage is not None else latest_vintage()
        if boundary_vintage is None:
            raise ValueError("No boundary vintages available in registry")
        return _load_coc_boundaries(boundary_vintage, base_dir=base_dir)

    if geo_type == "msa":
        if not layer.geometry.source:
            raise ValueError(
                "MSA map layers require geometry.source to name the definition version."
            )
        if vintage is None:
            raise ValueError(
                "MSA map layers require geometry.vintage to name the county geometry vintage."
            )
        return _load_msa_boundaries(
            definition_version=layer.geometry.source,
            county_vintage=vintage,
            base_dir=base_dir,
        )

    if geo_type == "metro":
        if not layer.geometry.source:
            raise ValueError(
                "Metro map layers require geometry.source to name the definition version."
            )
        if vintage is None:
            raise ValueError(
                "Metro map layers require geometry.vintage to name the county geometry vintage."
            )
        return _load_metro_boundaries(
            definition_version=layer.geometry.source,
            county_vintage=vintage,
            base_dir=base_dir,
        )

    raise ValueError(
        f"Unsupported map layer geometry '{geo_type}'. Supported types: coc, msa, metro."
    )


def _select_layer_rows(
    gdf: gpd.GeoDataFrame,
    *,
    id_column: str,
    selector_ids: list[str],
) -> gpd.GeoDataFrame:
    normalized_series = gdf[id_column].astype(str).map(_normalize_selector)
    index = pd.Index(normalized_series)
    normalized_ids = [_normalize_selector(item) for item in selector_ids]
    missing = [selector_ids[i] for i, item in enumerate(normalized_ids) if item not in index]
    if missing:
        available = sorted(gdf[id_column].astype(str).unique().tolist())
        raise ValueError(
            f"Map layer selector values not found for '{id_column}': {missing}. "
            f"Available examples: {available[:10]}{'...' if len(available) > 10 else ''}"
        )
    positions = [index.get_loc(item) for item in normalized_ids]
    if any(isinstance(position, slice) for position in positions):
        raise ValueError(f"Map layer '{id_column}' contains duplicate normalized identifiers.")
    return gdf.iloc[positions].copy().reset_index(drop=True)


def _resolve_tooltip_fields(
    *,
    geo_type: str,
    gdf: gpd.GeoDataFrame,
    tooltip_fields: list[str],
) -> list[str]:
    fields = tooltip_fields or DEFAULT_TOOLTIP_FIELDS[geo_type]
    missing = [field for field in fields if field not in gdf.columns]
    if missing:
        raise ValueError(
            f"Map tooltip fields not available for {geo_type} layer: {missing}. "
            f"Available columns: {sorted(gdf.columns.tolist())}"
        )
    return fields


def _resolve_map_layer(layer: MapLayerSpec, *, base_dir: Path) -> ResolvedMapLayer:
    geo_type = layer.geometry.type
    if geo_type not in IDENTIFIER_COLUMNS:
        raise ValueError(
            f"Unsupported map layer geometry '{geo_type}'. Supported types: coc, msa, metro."
        )

    gdf = _load_layer_geometries(layer, base_dir=base_dir)
    id_column = IDENTIFIER_COLUMNS[geo_type]
    selected = _select_layer_rows(
        gdf,
        id_column=id_column,
        selector_ids=layer.selector_ids,
    )
    tooltip_fields = _resolve_tooltip_fields(
        geo_type=geo_type,
        gdf=selected,
        tooltip_fields=layer.tooltip_fields,
    )
    label = layer.label or layer.group or f"{geo_type}:{','.join(layer.selector_ids)}"
    style = {
        "fillColor": layer.style.fill_color,
        "color": layer.style.stroke_color,
        "weight": layer.style.line_weight,
        "fillOpacity": layer.style.fill_opacity,
        "opacity": layer.style.stroke_opacity,
    }
    return ResolvedMapLayer(
        name=label,
        gdf=selected.to_crs(epsg=4326),
        tooltip_fields=tooltip_fields,
        show=layer.initial_visibility,
        style=style,
    )


def _initial_map_view(layers: list[ResolvedMapLayer]) -> list[float]:
    if not layers:
        return DEFAULT_MAP_CENTER
    geometries = pd.concat([layer.gdf.geometry for layer in layers], ignore_index=True)
    centroid = geometries.union_all().centroid
    return [centroid.y, centroid.x]


def _fit_map_to_layers(
    m: folium.Map,
    layers: list[ResolvedMapLayer],
    *,
    padding: int,
) -> None:
    geometries = gpd.GeoSeries(
        pd.concat([layer.gdf.geometry for layer in layers], ignore_index=True),
        crs="EPSG:4326",
    )
    minx, miny, maxx, maxy = geometries.total_bounds
    m.fit_bounds(
        [[miny, minx], [maxy, maxx]],
        padding=(padding, padding),
    )


def render_overlay_map(
    *,
    layers: list[ResolvedMapLayer],
    basemap: str,
    viewport: MapViewportSpec,
    out_html: Path,
) -> Path:
    """Render multiple resolved overlay layers to one HTML map."""
    map_location = (
        list(viewport.center)
        if viewport.center is not None
        else _initial_map_view(layers)
    )
    zoom_start = viewport.zoom if viewport.zoom is not None else DEFAULT_MAP_ZOOM
    m = folium.Map(
        location=map_location,
        zoom_start=zoom_start,
        tiles=_resolve_basemap(basemap),
    )

    for layer in layers:
        feature_group = folium.FeatureGroup(name=layer.name, show=layer.show)
        geojson_tooltip = None
        if layer.tooltip_fields:
            geojson_tooltip = folium.GeoJsonTooltip(
                fields=layer.tooltip_fields,
                aliases=[f"{field}:" for field in layer.tooltip_fields],
                sticky=False,
            )
        layer_data = layer.gdf.copy()
        for column in layer_data.columns:
            if column == layer_data.geometry.name:
                continue
            if pd.api.types.is_datetime64_any_dtype(layer_data[column]):
                layer_data[column] = layer_data[column].astype(str)
                continue
            if layer_data[column].dtype == "object":
                layer_data[column] = layer_data[column].map(
                    lambda value: (
                        value.isoformat()
                        if isinstance(value, (pd.Timestamp, datetime, date))
                        else value
                    )
                )
        folium.GeoJson(
            data=layer_data.__geo_interface__,
            style_function=lambda _feature, style=layer.style: style,
            tooltip=geojson_tooltip,
        ).add_to(feature_group)
        feature_group.add_to(m)

    if len(layers) > 1:
        folium.LayerControl(collapsed=False).add_to(m)

    if viewport.fit_layers:
        _fit_map_to_layers(m, layers, padding=viewport.padding)

    out_html.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_html))
    return out_html


def render_recipe_map(
    target: TargetSpec,
    *,
    project_root: Path,
    out_html: Path,
) -> Path:
    """Render a recipe-native map target to its HTML artifact."""
    if target.map_spec is None:
        raise ValueError("Target does not define map_spec.")
    base_dir = project_root / "data"
    layers = [_resolve_map_layer(layer, base_dir=base_dir) for layer in target.map_spec.layers]
    return render_overlay_map(
        layers=layers,
        basemap=target.map_spec.basemap,
        viewport=target.map_spec.viewport,
        out_html=out_html,
    )


def render_coc_map(
    coc_id: str,
    vintage: str | None = None,
    out_html: Path | None = None,
) -> Path:
    """Render an interactive Folium map for a single CoC boundary."""
    boundary_vintage = vintage
    if boundary_vintage is None:
        boundary_vintage = latest_vintage()
        if boundary_vintage is None:
            raise ValueError("No boundary vintages available in registry")
    gdf = _load_coc_boundaries(str(boundary_vintage), base_dir=Path("data"))
    selected_gdf = _select_layer_rows(
        gdf,
        id_column="coc_id",
        selector_ids=[coc_id],
    ).to_crs(epsg=4326)
    selected = ResolvedMapLayer(
        name=str(coc_id).strip(),
        gdf=selected_gdf,
        tooltip_fields=_resolve_tooltip_fields(
            geo_type="coc",
            gdf=selected_gdf,
            tooltip_fields=[],
        ),
        show=True,
        style={
            "fillColor": "#3388ff",
            "color": "#3388ff",
            "weight": 2.0,
            "fillOpacity": 0.3,
            "opacity": 1.0,
        },
    )

    if out_html is None:
        curated_dir("maps").mkdir(parents=True, exist_ok=True)
        normalized = selected.gdf.iloc[0]["coc_id"]
        out_html = curated_dir("maps") / f"{normalized}__{boundary_vintage}.html"
    else:
        out_html = Path(out_html)

    from hhplab.recipe.recipe_schema import MapViewportSpec

    return render_overlay_map(
        layers=[selected],
        basemap="cartodbpositron",
        viewport=MapViewportSpec(fit_layers=True),
        out_html=out_html,
    )
