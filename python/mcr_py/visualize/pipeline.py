"""Pipeline-data visualization as an interactive lonboard/deck.gl map."""
import json
import pathlib
import re

import geopandas as gpd
import lonboard
import lonboard.basemap
import pandas as pd
import polars as pl
import shapely
from lonboard.view_state import MapViewState
from shapely.geometry import LineString

from mcr_py.utils.geometa import GeoMeta

_NET_COLOR = {
    "walking": [37, 99, 235, 160],
    "cycling": [234, 88, 12, 160],
    "car": [107, 114, 128, 160],
}
_SN_COLOR = {
    "walking": [22, 163, 74, 230],
    "cycling": [234, 88, 12, 230],
    "car": [220, 38, 38, 230],
}
_SN_RADIUS = {"walking": 6, "cycling": 5, "car": 5}


def visualize_pipeline_data(
    geometa: GeoMeta,
    graph_dir: pathlib.Path,
    start_nodes_dir: pathlib.Path,
    gtfs_dir: pathlib.Path,
    city_id: str,
    output_path: pathlib.Path | None = None,
) -> lonboard.Map:
    """Render all pipeline layers for one city as an interactive lonboard/deck.gl map.

    Saves a standalone HTML with a toggle panel and legend when output_path is given.
    """
    center_lat, center_lon = geometa.get_center_lat_lon()
    layers: list[lonboard.BaseLayer] = []
    # Parallel list: name, CSS color, initial visibility
    layer_meta: list[tuple[str, str, bool]] = []

    def _add(layer: lonboard.BaseLayer, name: str, rgba: list[int], visible: bool = True) -> None:
        layers.append(layer)
        r, g, b, a = rgba
        layer_meta.append((name, f"rgba({r},{g},{b},{a/255:.2f})", visible))

    # Boundaries
    for lyr, name, rgba in _boundary_layers(geometa):
        _add(lyr, name, rgba)

    # Networks (hidden by default — large data, toggle-on if needed)
    for layer_name, node_file, edge_file in [
        ("walking", "walking_nodes.parquet", "walking_edges.parquet"),
        ("cycling", "cycling_nodes.parquet", "cycling_edges.parquet"),
        ("car", "car_nodes.parquet", "car_edges.parquet"),
    ]:
        node_path = graph_dir / node_file
        edge_path = graph_dir / edge_file
        if node_path.exists() and edge_path.exists():
            nodes = pl.read_parquet(node_path)
            edges = pl.read_parquet(edge_path)
            lyr = _network_layer(layer_name, nodes, edges)
            if lyr is not None:
                _add(lyr, f"Network: {layer_name}", _NET_COLOR.get(layer_name, [0, 0, 0, 160]), visible=False)

    # H3 grid and start nodes
    start_node_dfs: dict[str, pl.DataFrame] = {}
    for layer_name in ("walking", "cycling", "car"):
        fn = start_nodes_dir / f"{city_id}_{layer_name}_h3mapping.parquet"
        if fn.exists():
            start_node_dfs[layer_name] = pl.read_parquet(fn)

    if "walking" in start_node_dfs:
        lyr = _h3_layer(start_node_dfs["walking"])
        if lyr is not None:
            _add(lyr, "H3 grid (start cells)", [14, 165, 233, 160])

    for layer_name, sn_df in start_node_dfs.items():
        gdf = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(sn_df["lon"].to_list(), sn_df["lat"].to_list()),
            crs="EPSG:4326",
        )
        color = _SN_COLOR.get(layer_name, [0, 0, 0, 230])
        lyr = lonboard.ScatterplotLayer.from_geopandas(
            gdf,
            get_fill_color=color,
            get_line_color=[255, 255, 255, 200],
            radius_min_pixels=_SN_RADIUS.get(layer_name, 5),
        )
        _add(lyr, f"Start nodes: {layer_name}", color)

    # GTFS
    stops_path = gtfs_dir / "stops.parquet"
    st_path = gtfs_dir / "stop_times.parquet"
    if stops_path.exists():
        stops = pl.read_parquet(stops_path)
        _add(_gtfs_stops_layer(stops), "GTFS stops", [124, 58, 237, 200])
        if st_path.exists():
            stop_times = pl.read_parquet(st_path)
            lyr = _gtfs_routes_layer(stops, stop_times)
            if lyr is not None:
                _add(lyr, "GTFS routes", [124, 58, 237, 160])

    m = lonboard.Map(
        layers=layers,
        basemap_style=lonboard.basemap.CartoBasemap.Positron,
        view_state=MapViewState(longitude=center_lon, latitude=center_lat, zoom=11),
    )

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        html = m.as_html().data
        html = _inject_controls(html, layer_meta)
        output_path.write_text(html, encoding="utf-8")

    return m


# ---------------------------------------------------------------------------
# Layer builders
# ---------------------------------------------------------------------------

def _boundary_layers(
    geometa: GeoMeta,
) -> list[tuple[lonboard.PolygonLayer, str, list[int]]]:
    result = []
    for wkt, name, fill_rgba, line_rgba in [
        (geometa.boundary_wkt, "Boundary (buffered)", [30, 64, 175, 15], [30, 64, 175, 180]),
        (geometa.unbuffered_boundary_wkt, "Boundary (unbuffered)", [30, 58, 95, 20], [30, 58, 95, 220]),
    ]:
        shape = shapely.from_wkt(wkt)
        gdf = gpd.GeoDataFrame(geometry=[shape], crs="EPSG:4326")
        lyr = lonboard.PolygonLayer.from_geopandas(
            gdf,
            get_fill_color=fill_rgba,
            get_line_color=line_rgba,
            filled=True,
            stroked=True,
            line_width_min_pixels=2,
        )
        result.append((lyr, name, line_rgba))
    return result


def _network_layer(
    layer: str,
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
) -> lonboard.PathLayer | None:
    coords: dict[int, tuple[float, float]] = {
        row["osm_id"]: (row["lat"], row["long"])
        for row in nodes.iter_rows(named=True)
    }
    geometries = []
    for edge in edges.iter_rows(named=True):
        src = coords.get(edge["source_osm"])
        dst = coords.get(edge["dest_osm"])
        if src is None or dst is None:
            continue
        geometries.append(LineString([(src[1], src[0]), (dst[1], dst[0])]))
    if not geometries:
        return None
    gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")
    return lonboard.PathLayer.from_geopandas(
        gdf,
        get_color=_NET_COLOR.get(layer, [0, 0, 0, 160]),
        width_min_pixels=1,
        visible=False,
    )


def _h3_layer(walking_start_nodes: pl.DataFrame) -> lonboard.H3HexagonLayer | None:
    cells = walking_start_nodes["h3_cell_id"].to_list()
    if not cells:
        return None
    df = pd.DataFrame({"h3_index": cells})
    return lonboard.H3HexagonLayer.from_pandas(
        df,
        get_hexagon=df["h3_index"],
        get_fill_color=[14, 165, 233, 40],
        get_line_color=[3, 105, 161, 180],
        filled=True,
        stroked=True,
        extruded=False,
        line_width_min_pixels=1,
    )


def _gtfs_stops_layer(stops: pl.DataFrame) -> lonboard.ScatterplotLayer:
    gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(stops["stop_lon"].to_list(), stops["stop_lat"].to_list()),
        crs="EPSG:4326",
    )
    return lonboard.ScatterplotLayer.from_geopandas(
        gdf,
        get_fill_color=[124, 58, 237, 200],
        get_line_color=[255, 255, 255, 180],
        radius_min_pixels=4,
    )


def _gtfs_routes_layer(
    stops: pl.DataFrame,
    stop_times: pl.DataFrame,
) -> lonboard.PathLayer | None:
    stop_coord: dict[int, tuple[float, float]] = {
        row["stop_idx"]: (row["stop_lat"], row["stop_lon"])
        for row in stops.iter_rows(named=True)
    }
    links = (
        stop_times.sort(["trip_idx", "stop_sequence"])
        .with_columns(
            pl.col("stop_idx").shift(-1).over("trip_idx").alias("next_stop_idx")
        )
        .filter(pl.col("next_stop_idx").is_not_null())
        .select(pl.col("stop_idx").cast(pl.UInt32), pl.col("next_stop_idx").cast(pl.UInt32))
        .unique()
    )
    geometries = []
    for from_idx, to_idx in links.iter_rows():
        src = stop_coord.get(from_idx)
        dst = stop_coord.get(to_idx)
        if src is None or dst is None:
            continue
        geometries.append(LineString([(src[1], src[0]), (dst[1], dst[0])]))
    if not geometries:
        return None
    gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")
    return lonboard.PathLayer.from_geopandas(
        gdf,
        get_color=[124, 58, 237, 160],
        width_min_pixels=1,
    )


# ---------------------------------------------------------------------------
# HTML post-processing: inject toggle panel + legend
# ---------------------------------------------------------------------------

def _inject_controls(html: str, layer_meta: list[tuple[str, str, bool]]) -> str:
    """Parse lonboard HTML widget state, match model IDs to layer_meta, inject UI."""
    state_match = re.search(
        r'(<script type="application/vnd\.jupyter\.widget-state\+json">)(.*?)(</script>)',
        html,
        re.DOTALL,
    )
    if not state_match:
        return html

    state = json.loads(state_match.group(2))

    # Locate the Map model: extract ordered layer refs and patch _esm to expose the model
    map_layer_refs: list[str] = []
    for mid, mdata in state["state"].items():
        s = mdata.get("state", {})
        if s.get("_anywidget_id") != "lonboard._map.Map":
            continue
        map_layer_refs = s.get("layers", [])
        # Patch the Map's _esm to store the anywidget model on window.__lbMap.
        # The _esm default-export is an object {render: fn}; we wrap render so that
        # window.__lbMap is set before the deck.gl canvas is created.  The render
        # wrapper works for both sync and async render functions.
        esm = s.get("_esm", "")
        export_m = re.search(r'export\{(\w+) as default\}', esm)
        if export_m:
            var_name = export_m.group(1)
            patch = (
                f";(function(){{var __r={var_name}.render;"
                f"{var_name}.render=function(a){{window.__lbMap=a.model;return __r(a);}};}})()"
            )
            state["state"][mid]["state"]["_esm"] = esm.replace(
                f"export{{{var_name} as default}}",
                f"{patch};export{{{var_name} as default}}",
            )
        break

    # Strip "IPY_MODEL_" prefix → raw UUIDs, ordered same as layer_meta
    model_ids = [ref.replace("IPY_MODEL_", "") for ref in map_layer_refs]

    # Build panel entries: (model_id, name, css_color, checked)
    entries: list[tuple[str, str, str, bool]] = []
    for (name, css_color, visible), model_id in zip(layer_meta, model_ids):
        entries.append((model_id, name, css_color, visible))

    # Re-serialise the (now patched) widget state back into the HTML
    new_state_json = json.dumps(state)
    html = (
        html[: state_match.start()]
        + state_match.group(1)
        + new_state_json
        + state_match.group(3)
        + html[state_match.end() :]
    )

    panel = _panel_html(entries)
    script = _panel_js(entries)
    return html.replace("</body>", f"{panel}\n{script}\n</body>")


def _panel_html(entries: list[tuple[str, str, str, bool]]) -> str:
    rows = []
    for model_id, name, css_color, visible in entries:
        checked = "checked" if visible else ""
        safe_id = model_id.replace("-", "")
        rows.append(
            f'<div class="lb-row">'
            f'<input type="checkbox" id="lb-{safe_id}" {checked} '
            f'onchange="lbToggle(\'{model_id}\', this.checked)">'
            f'<span class="lb-swatch" style="background:{css_color}"></span>'
            f'<label for="lb-{safe_id}">{name}</label>'
            f"</div>"
        )
    rows_html = "\n".join(rows)
    return f"""
<style>
  #lb-panel {{
    position: fixed;
    top: 10px;
    right: 10px;
    z-index: 9999;
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    padding: 12px 14px;
    min-width: 200px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 13px;
    color: #333;
    user-select: none;
  }}
  #lb-panel h3 {{
    margin: 0 0 8px;
    font-size: 13px;
    font-weight: 600;
    color: #111;
    letter-spacing: 0.03em;
    text-transform: uppercase;
  }}
  .lb-row {{
    display: flex;
    align-items: center;
    gap: 7px;
    margin-bottom: 5px;
    cursor: pointer;
  }}
  .lb-row input {{ cursor: pointer; margin: 0; }}
  .lb-row label {{ cursor: pointer; flex: 1; }}
  .lb-swatch {{
    width: 13px;
    height: 13px;
    border-radius: 3px;
    flex-shrink: 0;
    border: 1px solid rgba(0,0,0,0.15);
  }}
</style>
<div id="lb-panel">
  <h3>Layers</h3>
  {rows_html}
</div>"""


def _panel_js(entries: list[tuple[str, str, str, bool]]) -> str:
    return """
<script>
(function () {
  // window.__lbMap is set by the patched _esm render wrapper once deck.gl initialises.
  // model.widget_manager is the HTMLManager that owns all layer models.
  function withLayerModel(modelId, cb) {
    var poll = setInterval(function () {
      var mapModel = window.__lbMap;
      if (mapModel && mapModel.widget_manager) {
        clearInterval(poll);
        mapModel.widget_manager.get_model(modelId).then(function (model) {
          cb(model);
        }).catch(function (e) {
          console.warn('[lonboard toggle] model not found:', modelId, e);
        });
      }
    }, 100);
  }

  window.lbToggle = function (modelId, visible) {
    withLayerModel(modelId, function (model) {
      model.set('visible', visible);
      // save_changes() is a no-op without a kernel, but set() already fires
      // Backbone change events that lonboard subscribes to for re-renders.
    });
  };
})();
</script>"""
