"""Routes and scenario visualization for MCR5 reduced-paths output.

Two public entry points:
- `visualize_all_scenarios`: one H3 reachability layer per scenario for an
  overview of walking / cycling / car reach across all sampled origins.
- `visualize_origin_routes`: detailed route map for one H3 origin cell —
  pruned background network, per-mode route paths to POIs, start location,
  and clickable POI markers.
"""

from __future__ import annotations

import json
import pathlib
import re

import geopandas as gpd
import h3
import lonboard
import lonboard.basemap
import numpy as np
import pandas as pd
import polars as pl
from lonboard.view_state import MapViewState
from shapely.geometry import LineString, Point

from mcr_py.visualize.gtfs_lines import (
    TYPE_COLOR,
    TYPE_WIDTH,
    GtfsLines,
    LegMatcher,
    connections_in_cells,
    load_gtfs_lines,
)
from mcr_py.visualize.pipeline import _inject_controls

# ---------------------------------------------------------------------------
# Mode constants (mirror Rust MCRModes)
# ---------------------------------------------------------------------------
_WALKING = 1
_CYCLING = 2
_CAR = 3
_PT = 6

_MODE_COLOR: dict[int, list[int]] = {
    _WALKING: [37, 99, 235, 210],
    _CYCLING: [234, 88, 12, 220],
    _CAR:     [220, 38, 38, 220],
}
_MODE_NAME = {_WALKING: "Walking", _CYCLING: "Cycling", _CAR: "Car", _PT: "Public transport"}
_MODE_WIDTH: dict[int, int] = {_WALKING: 2, _CYCLING: 3, _CAR: 4}

# POI category → RGB
_POI_COLORS: dict[str, list[int]] = {
    "Parks":      [22,  163,  74],
    "Shops":      [124,  58, 237],
    "Sustenance": [234,  88,  12],
    "Grocery":    [ 16, 185, 129],
    "Health":     [239,  68,  68],
    "Banks":      [ 59, 130, 246],
    "Education":  [245, 158,  11],
}

# Scenario base colors for the reachability heat-map
_SCENARIO_COLOR: dict[str, tuple[int, int, int]] = {
    "walking":      (37,  99, 235),
    "private_bike": (234, 88,  12),
    "private_car":  (220, 38,  38),
    "public_transport_0": (126, 34, 206),
}
_SCENARIO_LABEL = {
    "walking":      "Walking",
    "private_bike": "Private bike",
    "private_car":  "Private car",
    "public_transport_0": "Public transport",
}


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def build_coord_map(graph_dir: pathlib.Path) -> dict[int, tuple[float, float]]:
    """Build {osm_id: (lat, lon)} from walking + cycling + car node parquets."""
    coords: dict[int, tuple[float, float]] = {}
    for name in ("walking", "cycling", "car"):
        nodes = pl.read_parquet(graph_dir / f"{name}_nodes.parquet")
        coords.update(
            {r["osm_id"]: (r["lat"], r["long"]) for r in nodes.iter_rows(named=True)}
        )
    return coords


def load_poi_nodes(poi_path: pathlib.Path) -> tuple[pl.DataFrame, set[int]]:
    """Return (DataFrame, set[nearest_osm_node]) from poi_nodes.parquet."""
    df = pl.read_parquet(poi_path)
    return df, set(df["nearest_osm_node"].to_list())


def best_poi_routes(
    scenario_dir: pathlib.Path,
    cell_id: str,
    poi_ids: set[int],
) -> pl.DataFrame:
    """Load one origin parquet, filter to POI destinations, keep min-time label per node."""
    p = scenario_dir / f"{cell_id}.parquet"
    if not p.exists():
        return pl.DataFrame()
    df = pl.read_parquet(p)
    poi_df = df.filter(pl.col("node_id").is_in(list(poi_ids)))
    if poi_df.is_empty():
        return poi_df
    return (
        poi_df
        .with_columns(pl.col("objectives").list.get(0).alias("time_ms"))
        .sort("time_ms")
        .group_by("node_id")
        .agg(
            pl.first("objectives"),
            pl.first("path"),
            pl.first("hop_modes"),
            pl.first("time_ms"),
        )
    )


def route_h3_cells(
    routes_df: pl.DataFrame,
    coord_map: dict[int, tuple[float, float]],
    resolution: int = 9,
) -> frozenset[str]:
    """H3 cells covering every node that appears in any path of `routes_df`."""
    cells: set[str] = set()
    for path in routes_df["path"].to_list():
        for nid in path:
            c = coord_map.get(nid)
            if c:
                cells.add(h3.latlng_to_cell(c[0], c[1], resolution))
    return frozenset(cells)


def nodes_in_cells(
    coord_map: dict[int, tuple[float, float]],
    h3_cells: frozenset[str],
    resolution: int = 9,
) -> set[int]:
    """Node IDs whose coordinates fall inside any of the H3 cells."""
    return {
        nid
        for nid, (lat, lon) in coord_map.items()
        if h3.latlng_to_cell(lat, lon, resolution) in h3_cells
    }


# ---------------------------------------------------------------------------
# Layer builders
# ---------------------------------------------------------------------------

def make_network_layer(
    edges_df: pl.DataFrame,
    pruned_nodes: set[int],
    coord_map: dict[int, tuple[float, float]],
    color: list[int],
) -> lonboard.PathLayer | None:
    """Background PathLayer: network edges clipped to the pruned node set."""
    in_map = {nid: coord_map[nid] for nid in pruned_nodes if nid in coord_map}
    filtered = edges_df.filter(
        pl.col("source_osm").is_in(pruned_nodes)
        & pl.col("dest_osm").is_in(pruned_nodes)
    )
    geoms: list[LineString] = []
    for src_id, dst_id in filtered.select("source_osm", "dest_osm").iter_rows():
        s = in_map.get(src_id)
        d = in_map.get(dst_id)
        if s and d:
            geoms.append(LineString([(s[1], s[0]), (d[1], d[0])]))
    if not geoms:
        return None
    gdf = gpd.GeoDataFrame(geometry=geoms, crs="EPSG:4326")
    return lonboard.PathLayer.from_geopandas(gdf, get_color=color, width_min_pixels=1)


def make_h3_layer(
    h3_cells: frozenset[str] | list[str],
    fill_color: list[int] | None = None,
    line_color: list[int] | None = None,
    line_width_min_pixels: int = 1,
) -> lonboard.H3HexagonLayer | None:
    cells = list(h3_cells)
    if not cells:
        return None
    df = pd.DataFrame({"h3_index": cells})
    return lonboard.H3HexagonLayer.from_pandas(
        df,
        get_hexagon=df["h3_index"],
        get_fill_color=fill_color or [14, 165, 233, 20],
        get_line_color=line_color or [3, 105, 161, 100],
        filled=True,
        stroked=True,
        extruded=False,
        line_width_min_pixels=line_width_min_pixels,
    )


def make_route_path_layers(
    routes_df: pl.DataFrame,
    coord_map: dict[int, tuple[float, float]],
    matcher: LegMatcher | None = None,
) -> list[tuple[lonboard.PathLayer, str, list[int]]]:
    """
    Split route paths by mode; return [(PathLayer, label, rgba), ...].

    Each path is segmented by hop_mode: walking → blue, cycling → orange, car → red.
    Adjacent nodes sharing the same mode form one LineString segment. Public-transport
    segments additionally carry `line`/`type` properties (identified through `matcher`)
    and are coloured by vehicle type, one layer per type.
    """
    mode_geoms: dict[int, list[LineString]] = {_WALKING: [], _CYCLING: [], _CAR: []}
    pt_legs: list[list[int]] = []

    for path, modes in zip(
        routes_df["path"].to_list(), routes_df["hop_modes"].to_list()
    ):
        if len(path) < 2:
            continue
        cur_mode = modes[1] if len(modes) > 1 else _WALKING
        seg: list[int] = [path[0], path[1]]

        for i in range(2, len(path)):
            m = modes[i] if i < len(modes) else cur_mode
            if m != cur_mode:
                _commit_segment(seg, cur_mode, coord_map, mode_geoms, pt_legs)
                seg = [path[i - 1], path[i]]
                cur_mode = m
            else:
                seg.append(path[i])
        _commit_segment(seg, cur_mode, coord_map, mode_geoms, pt_legs)

    result: list[tuple[lonboard.PathLayer, str, list[int]]] = []
    for mode, geoms in mode_geoms.items():
        if not geoms:
            continue
        gdf = gpd.GeoDataFrame(geometry=geoms, crs="EPSG:4326")
        color = _MODE_COLOR[mode]
        lyr = lonboard.PathLayer.from_geopandas(
            gdf, get_color=color, width_min_pixels=_MODE_WIDTH[mode]
        )
        result.append((lyr, f"Routes: {_MODE_NAME[mode]}", color))

    result.extend(_pt_leg_layers(pt_legs, coord_map, matcher))
    return result


def _pt_leg_layers(
    legs: list[list[int]],
    coord_map: dict[int, tuple[float, float]],
    matcher: LegMatcher | None,
) -> list[tuple[lonboard.PathLayer, str, list[int]]]:
    """One PathLayer per vehicle type; features carry `line` and `type` properties."""
    by_type: dict[str, dict[str, list]] = {}
    seen: set[tuple[int, ...]] = set()
    for nodes in legs:
        key = tuple(nodes)
        if key in seen:  # same leg reached via several POI routes
            continue
        seen.add(key)
        coords = [coord_map[n] for n in nodes if n in coord_map]
        if len(coords) < 2:
            continue
        line, vtype = matcher.match(nodes) if matcher else ("?", "Other")
        bucket = by_type.setdefault(
            vtype.split("/")[0], {"geometry": [], "line": [], "type": []}
        )
        bucket["geometry"].append(LineString([(lon, lat) for lat, lon in coords]))
        bucket["line"].append(line)
        bucket["type"].append(vtype)

    result = []
    for vtype, cols in by_type.items():
        gdf = gpd.GeoDataFrame(
            {"line": cols["line"], "type": cols["type"]},
            geometry=cols["geometry"],
            crs="EPSG:4326",
        )
        color = TYPE_COLOR.get(vtype, TYPE_COLOR["Other"])
        lyr = lonboard.PathLayer.from_geopandas(
            gdf,
            get_color=color,
            width_min_pixels=TYPE_WIDTH.get(vtype, 3) + 2,
            pickable=True,
        )
        result.append((lyr, f"Routes: {vtype}", color))
    return result


def _commit_segment(
    nodes: list[int],
    mode: int,
    coord_map: dict[int, tuple[float, float]],
    bucket: dict[int, list[LineString]],
    pt_legs: list[list[int]],
) -> None:
    if mode == _PT:
        pt_legs.append(nodes)
        return
    if mode not in bucket:
        return
    coords = [
        (c[1], c[0])
        for nid in nodes
        if (c := coord_map.get(nid)) is not None
    ]
    if len(coords) >= 2:
        bucket[mode].append(LineString(coords))


def make_gtfs_connection_layers(
    gtfs: GtfsLines,
    pruned: frozenset[str],
) -> list[tuple[lonboard.PathLayer, str, list[int]]]:
    """GTFS stop-to-stop connections inside the routed area, one layer per vehicle type.

    Features carry `line` (all lines serving the connection) and `type`
    (Train / Subway / Tram / Bus / …) so the hover tooltip can show them.
    """
    conns = connections_in_cells(gtfs, pruned)
    result = []
    for vtype in TYPE_COLOR:
        sub = conns.filter(pl.col("type") == vtype)
        if sub.is_empty():
            continue
        geoms = [
            LineString([(r["a_lon"], r["a_lat"]), (r["b_lon"], r["b_lat"])])
            for r in sub.iter_rows(named=True)
        ]
        gdf = gpd.GeoDataFrame(
            {
                "line": sub["lines"].to_list(),
                "type": sub["type"].to_list(),
                "from_stop": sub["a_name"].to_list(),
                "to_stop": sub["b_name"].to_list(),
            },
            geometry=geoms,
            crs="EPSG:4326",
        )
        color = TYPE_COLOR[vtype]
        lyr = lonboard.PathLayer.from_geopandas(
            gdf,
            get_color=color[:3] + [150],
            width_min_pixels=max(1, TYPE_WIDTH[vtype] - 1),
            pickable=True,
        )
        result.append((lyr, f"GTFS: {vtype} ({len(sub)})", color))
    return result


def make_poi_layer(
    poi_df: pl.DataFrame,
    pruned_nodes: set[int],
) -> lonboard.ScatterplotLayer | None:
    """Pickable ScatterplotLayer for POIs whose access node is in the pruned area."""
    visible = poi_df.filter(pl.col("nearest_osm_node").is_in(pruned_nodes))
    if visible.is_empty():
        return None
    types = visible["poi_type"].to_list()
    colors = np.array(
        [_POI_COLORS.get(t, [100, 100, 100]) + [220] for t in types], dtype=np.uint8
    )
    gdf = gpd.GeoDataFrame(
        {"poi_type": types},
        geometry=gpd.points_from_xy(
            visible["long"].to_list(),
            visible["lat"].to_list(),
        ),
        crs="EPSG:4326",
    )
    return lonboard.ScatterplotLayer.from_geopandas(
        gdf,
        get_fill_color=colors,
        get_line_color=[255, 255, 255, 180],
        radius_min_pixels=6,
        pickable=True,
    )


def make_start_layer(
    start_nodes_df: pl.DataFrame,
    cell_id: str,
    coord_map: dict[int, tuple[float, float]],
    color: list[int],
) -> lonboard.ScatterplotLayer | None:
    """Prominent ScatterplotLayer marking the origin node for a given H3 cell."""
    row = start_nodes_df.filter(pl.col("h3_cell_id") == cell_id)
    if row.is_empty():
        return None
    nid = int(row["osm_node_id"][0])
    c = coord_map.get(nid)
    if not c:
        return None
    gdf = gpd.GeoDataFrame(
        {"label": [f"Origin {cell_id}"]},
        geometry=[Point(c[1], c[0])],
        crs="EPSG:4326",
    )
    return lonboard.ScatterplotLayer.from_geopandas(
        gdf,
        get_fill_color=color,
        get_line_color=[255, 255, 255, 240],
        radius_min_pixels=11,
        stroked=True,
        line_width_min_pixels=2,
    )


def _time_colors(
    times: np.ndarray,
    base_rgb: tuple[int, int, int],
) -> np.ndarray:
    """RGBA array with alpha encoding proximity (fast=opaque, slow=transparent)."""
    max_t = float(times.max()) if times.size > 0 else 1.0
    t = 1.0 - np.clip(times / max_t, 0.0, 1.0)
    alphas = (35 + t * 185).astype(np.uint8)
    r, g, b = base_rgb
    rgb = np.full((len(times), 3), [r, g, b], dtype=np.uint8)
    return np.column_stack([rgb, alphas])


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

_POI_CATEGORY_ORDER = [
    "Parks", "Shops", "Sustenance", "Grocery", "Health", "Banks", "Education"
]


def best_times_by_category(
    routes_df: pl.DataFrame,
    poi_df: pl.DataFrame,
) -> dict[str, int]:
    """Min travel time (ms) to any POI of each category reachable from routes_df."""
    joined = (
        routes_df
        .join(
            poi_df.select("nearest_osm_node", "poi_type"),
            left_on="node_id",
            right_on="nearest_osm_node",
            how="inner",
        )
        .group_by("poi_type")
        .agg(pl.col("time_ms").min())
    )
    return {row["poi_type"]: int(row["time_ms"]) for row in joined.iter_rows(named=True)}


def _fmt_ms(ms: int) -> str:
    s = ms // 1000
    return f"{s // 60}:{s % 60:02d}"


# ---------------------------------------------------------------------------
# HTML post-processing
# ---------------------------------------------------------------------------

def _inject_summary_panel(
    html: str,
    summaries: dict[str, dict[str, int]],
) -> str:
    """Inject a fixed bottom-left panel summarising best time per POI category × scenario.

    summaries: {scenario_key: {poi_category: time_ms}}
    """
    scenario_order = [s for s in _SCENARIO_COLOR if s in summaries]
    categories = [c for c in _POI_CATEGORY_ORDER if any(c in summaries[s] for s in scenario_order)]

    # Header row
    header_cells = "".join(
        f'<th>{_SCENARIO_LABEL.get(s, s)}</th>' for s in scenario_order
    )
    header = f"<tr><th>Category</th>{header_cells}</tr>"

    # Data rows
    rows = []
    for cat in categories:
        rgb = _POI_COLORS.get(cat, [100, 100, 100])
        swatch = (
            f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
            f'background:rgb({rgb[0]},{rgb[1]},{rgb[2]});margin-right:5px;flex-shrink:0;"></span>'
        )
        cells = f'<td><span style="display:flex;align-items:center;">{swatch}{cat}</span></td>'
        for s in scenario_order:
            t = summaries[s].get(cat)
            val = _fmt_ms(t) if t is not None else "—"
            cells += f"<td>{val}</td>"
        rows.append(f"<tr>{cells}</tr>")

    rows_html = "\n".join(rows)
    panel = f"""
<style>
  #lb-summary {{
    position: fixed;
    bottom: 24px;
    left: 10px;
    z-index: 9999;
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    padding: 10px 14px 12px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 12px;
    color: #333;
    user-select: none;
  }}
  #lb-summary h3 {{
    margin: 0 0 8px;
    font-size: 11px;
    font-weight: 600;
    color: #666;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }}
  #lb-summary table {{
    border-collapse: collapse;
    font-variant-numeric: tabular-nums;
  }}
  #lb-summary th {{
    font-weight: 600;
    color: #555;
    padding: 2px 10px 4px 4px;
    text-align: left;
    border-bottom: 1px solid #e5e7eb;
    font-size: 11px;
  }}
  #lb-summary td {{
    padding: 2px 10px 2px 4px;
    color: #222;
    vertical-align: middle;
    white-space: nowrap;
  }}
  #lb-summary tr:not(:first-child) td {{
    border-top: 1px solid #f3f4f6;
  }}
</style>
<div id="lb-summary">
  <h3>Best time to nearest POI</h3>
  <table>
    {header}
    {rows_html}
  </table>
</div>"""
    return html.replace("</body>", panel + "\n</body>")


def _inject_poi_tooltip(html: str) -> str:
    """Inject a deck.gl getTooltip that shows POI category on hover.

    lonboard's React component sets window.__deck = the Deck instance via
    a useEffect hook in its ESM. Once available we can call setProps to
    add tooltip support without touching the minified bundle.
    """
    poi_colors_js = json.dumps(
        {k: f"rgb({v[0]},{v[1]},{v[2]})" for k, v in _POI_COLORS.items()}
    )
    script = f"""
<script>
(function () {{
  var _colors = {poi_colors_js};
  function setup() {{
    var deck = window.__deck;
    if (!deck) {{ setTimeout(setup, 80); return; }}
    deck.setProps({{
      getTooltip: function (info) {{
        if (!info.object || !info.object.properties) return null;
        var props = info.object.properties;
        var base = 'font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:12px;line-height:1.3;';
        var box = {{ background: '#fff', padding: '7px 10px', borderRadius: '6px',
                    boxShadow: '0 2px 10px rgba(0,0,0,0.18)', pointerEvents: 'none' }};
        if (props.line !== undefined) {{
          var stops = props.from_stop ? '<div style="color:#666">' + props.from_stop + ' – ' + props.to_stop + '</div>' : '';
          return {{
            html: '<div style="' + base + '"><strong>' + props.type + '</strong> · line ' + props.line + stops + '</div>',
            style: box
          }};
        }}
        var type = props.poi_type;
        if (!type) return null;
        var col = _colors[type] || '#888';
        return {{
          html: '<div style="display:flex;align-items:center;gap:6px;font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:12px;line-height:1;">' +
                '<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:' + col + ';flex-shrink:0;"></span>' +
                '<strong>' + type + '</strong></div>',
          style: {{ background: '#fff', padding: '7px 10px', borderRadius: '6px',
                    boxShadow: '0 2px 10px rgba(0,0,0,0.18)', pointerEvents: 'none' }}
        }};
      }}
    }});
  }}
  setup();
}})();
</script>"""
    return html.replace("</body>", script + "\n</body>")


# ---------------------------------------------------------------------------
# Main visualization functions
# ---------------------------------------------------------------------------

def visualize_all_scenarios(
    results_dir: pathlib.Path,
    graph_dir: pathlib.Path,
    poi_path: pathlib.Path,
    output_path: pathlib.Path,
    center_lat: float = 50.9375,
    center_lon: float = 6.9603,
    zoom: int = 11,
) -> None:
    """Scenario overview map: one H3 reachability layer per mode combination.

    Each layer shows the union of reachable H3 cells across all sampled origins,
    colored by the minimum travel time observed at each cell (bright = fast,
    faint = slow).  The 9 origin cells are highlighted in amber.
    """
    coord_map = build_coord_map(graph_dir)

    layers: list[lonboard.BaseLayer] = []
    layer_meta: list[tuple[str, str, bool]] = []

    def _add(lyr: lonboard.BaseLayer, name: str, rgba: list[int], visible: bool = True) -> None:
        layers.append(lyr)
        r, g, b, a = rgba
        layer_meta.append((name, f"rgba({r},{g},{b},{a/255:.2f})", visible))

    # Origin H3 cells
    _sn_path = next(
        (results_dir / s / "_start_nodes.parquet"
         for s in _SCENARIO_COLOR
         if (results_dir / s / "_start_nodes.parquet").exists()),
        None,
    )
    if _sn_path:
        sn_cells = pl.read_parquet(_sn_path)["h3_cell_id"].to_list()
        lyr = make_h3_layer(
            sn_cells,
            fill_color=[250, 204, 21, 70],
            line_color=[234, 179, 8, 220],
            line_width_min_pixels=2,
        )
        if lyr:
            _add(lyr, "Origin cells", [234, 179, 8, 220])

    # One H3 reachability layer per scenario
    for scenario_name, base_rgb in _SCENARIO_COLOR.items():
        scenario_dir = results_dir / scenario_name
        if not scenario_dir.is_dir():
            continue

        parquets = [
            p for p in scenario_dir.glob("*.parquet") if not p.name.startswith("_")
        ]
        if not parquets:
            continue

        all_rows: list[pl.DataFrame] = []
        for p in parquets:
            df = pl.read_parquet(p)
            all_rows.append(
                df.with_columns(pl.col("objectives").list.get(0).alias("time_ms"))
                .select("node_id", "time_ms")
            )

        combined = pl.concat(all_rows)
        min_time = combined.group_by("node_id").agg(pl.col("time_ms").min())

        h3_times: dict[str, float] = {}
        for nid, t in min_time.iter_rows():
            c = coord_map.get(nid)
            if c:
                cell = h3.latlng_to_cell(c[0], c[1], 9)
                if t < h3_times.get(cell, float("inf")):
                    h3_times[cell] = t

        if not h3_times:
            continue

        cells = list(h3_times.keys())
        times = np.array([h3_times[c] for c in cells], dtype=np.float64)
        colors = _time_colors(times, base_rgb)

        df_pd = pd.DataFrame({"h3_index": cells})
        lyr = lonboard.H3HexagonLayer.from_pandas(
            df_pd,
            get_hexagon=df_pd["h3_index"],
            get_fill_color=colors,
            get_line_color=list(base_rgb) + [40],
            filled=True,
            stroked=True,
            extruded=False,
            line_width_min_pixels=0,
        )
        label = _SCENARIO_LABEL.get(scenario_name, scenario_name)
        _add(lyr, f"Reach: {label}", list(base_rgb) + [180])

    m = lonboard.Map(
        layers=layers,
        basemap_style=lonboard.basemap.CartoBasemap.Positron,
        view_state=MapViewState(
            longitude=center_lon, latitude=center_lat, zoom=zoom
        ),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = m.as_html().data
    html = _inject_controls(html, layer_meta)
    output_path.write_text(html, encoding="utf-8")


def visualize_origin_routes(
    results_dir: pathlib.Path,
    graph_dir: pathlib.Path,
    poi_path: pathlib.Path,
    cell_id: str,
    output_path: pathlib.Path,
    zoom: int = 13,
    gtfs_dir: pathlib.Path | None = None,
) -> None:
    """Detailed route map for one origin H3 cell.

    Builds:
    - Pruned H3 background: cells covering nodes traversed by any POI route.
    - Filtered walking network: street edges within the pruned area.
    - Per-scenario, per-mode route paths to POI destinations (min-time label).
    - GTFS connections inside the routed area (one layer per vehicle type; hover for
      line + type). Needs `gtfs_dir` (indexed GTFS parquets with the sibling ``.zip``);
      defaults to ``<graph_dir>/../../gtfs_clean/<city>`` when that exists.
    - Origin start location marker.
    - Clickable POI markers (hover to see category).
    """
    coord_map = build_coord_map(graph_dir)
    poi_df, poi_ids = load_poi_nodes(poi_path)

    layers: list[lonboard.BaseLayer] = []
    layer_meta: list[tuple[str, str, bool]] = []

    def _add(lyr: lonboard.BaseLayer, name: str, rgba: list[int], visible: bool = True) -> None:
        layers.append(lyr)
        r, g, b, a = rgba
        layer_meta.append((name, f"rgba({r},{g},{b},{a/255:.2f})", visible))

    # Collect best POI routes across all scenarios
    all_routes_by_scenario: dict[str, pl.DataFrame] = {}
    for scenario_name in _SCENARIO_COLOR:
        scenario_dir = results_dir / scenario_name
        routes = best_poi_routes(scenario_dir, cell_id, poi_ids)
        if not routes.is_empty():
            all_routes_by_scenario[scenario_name] = routes

    if not all_routes_by_scenario:
        raise ValueError(f"No POI routes found for cell {cell_id} in {results_dir}")

    # Per-scenario best time per POI category (for the summary panel)
    category_summaries: dict[str, dict[str, int]] = {
        s: best_times_by_category(routes, poi_df)
        for s, routes in all_routes_by_scenario.items()
    }

    # Pruned H3 cells: union of all path nodes across all scenarios
    all_cells: set[str] = set()
    for routes in all_routes_by_scenario.values():
        all_cells.update(route_h3_cells(routes, coord_map))
    pruned = frozenset(all_cells)
    pruned_nodes = nodes_in_cells(coord_map, pruned)

    if gtfs_dir is None:
        default = graph_dir.parent.parent / "gtfs_clean" / graph_dir.name
        gtfs_dir = default if default.with_suffix(".zip").exists() else None
    gtfs = load_gtfs_lines(gtfs_dir) if gtfs_dir else None
    matcher = LegMatcher(gtfs, gtfs_dir) if gtfs and gtfs_dir else None

    # Centre the view on the origin cell
    center = h3.cell_to_latlng(cell_id)
    view = MapViewState(longitude=center[1], latitude=center[0], zoom=zoom)

    # 1. Pruned H3 background
    h3_lyr = make_h3_layer(pruned, fill_color=[186, 230, 253, 18], line_color=[125, 211, 252, 80])
    if h3_lyr:
        _add(h3_lyr, "Background: hex grid", [14, 165, 233, 100])

    # 2. Walking network clipped to pruned area (hidden by default — heavy layer)
    walking_edges = pl.read_parquet(graph_dir / "walking_edges.parquet")
    walk_net = make_network_layer(
        walking_edges, pruned_nodes, coord_map, [148, 163, 184, 80]
    )
    if walk_net:
        _add(walk_net, "Network: walking", [148, 163, 184, 130], visible=False)

    # 2b. Cycling network clipped to pruned area (hidden by default)
    cycling_edges = pl.read_parquet(graph_dir / "cycling_edges.parquet")
    cycling_net = make_network_layer(
        cycling_edges, pruned_nodes, coord_map, [253, 186, 116, 90]
    )
    if cycling_net:
        _add(cycling_net, "Network: cycling", [253, 186, 116, 150], visible=False)

    # 3. Car network clipped to pruned area (hidden by default)
    car_edges = pl.read_parquet(graph_dir / "car_edges.parquet")
    car_net = make_network_layer(
        car_edges, pruned_nodes, coord_map, [203, 213, 225, 80]
    )
    if car_net:
        _add(car_net, "Network: car", [203, 213, 225, 130], visible=False)

    # 3b. GTFS connections clipped to the pruned area (buses hidden by default)
    if gtfs:
        for gtfs_lyr, name, rgba in make_gtfs_connection_layers(gtfs, pruned):
            _add(gtfs_lyr, name, rgba, visible=not name.startswith("GTFS: Bus"))

    # 4. Route paths per scenario and mode
    # Walking scenario uses mode colors directly; bike/car get the same colors
    # (blue=walk-access, orange=cycling-leg, red=car-leg).
    for scenario_name, routes in all_routes_by_scenario.items():
        label_prefix = _SCENARIO_LABEL.get(scenario_name, scenario_name)
        for path_lyr, mode_label, rgba in make_route_path_layers(routes, coord_map, matcher):
            _add(path_lyr, f"{label_prefix}: {mode_label}", rgba)

    # 5. Start location — shared across scenarios (same walking origin node)
    _sn_path = results_dir / "walking" / "_start_nodes.parquet"
    if _sn_path.exists():
        sn_df = pl.read_parquet(_sn_path)
        start_lyr = make_start_layer(sn_df, cell_id, coord_map, [250, 204, 21, 255])
        if start_lyr:
            _add(start_lyr, "Start location", [250, 204, 21, 255])

    # 6. POIs in pruned area (pickable — hover for category)
    poi_lyr = make_poi_layer(poi_df, pruned_nodes)
    if poi_lyr:
        _add(poi_lyr, "POIs (hover for category)", [34, 197, 94, 220])

    m = lonboard.Map(
        layers=layers,
        basemap_style=lonboard.basemap.CartoBasemap.Positron,
        view_state=view,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = m.as_html().data
    html = _inject_controls(html, layer_meta)
    html = _inject_summary_panel(html, category_summaries)
    html = _inject_poi_tooltip(html)
    output_path.write_text(html, encoding="utf-8")
