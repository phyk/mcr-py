"""Pipeline-data visualization as an interactive Folium map."""
import pathlib

import folium
import polars as pl
import shapely

from mcr_py.utils.geometa import GeoMeta

_NET_COLOR = {
    "walking": "#2563eb",
    "cycling": "#ea580c",
    "car": "#6b7280",
}
_SN_COLOR = {
    "walking": "#16a34a",
    "cycling": "#ea580c",
    "car": "#dc2626",
}
_SN_RADIUS = {"walking": 6, "cycling": 5, "car": 5}


def visualize_pipeline_data(
    geometa: GeoMeta,
    graph_dir: pathlib.Path,
    start_nodes_dir: pathlib.Path,
    gtfs_dir: pathlib.Path,
    city_id: str,
    output_path: pathlib.Path | None = None,
) -> folium.Map:
    """Render all pipeline layers for one city as an interactive Folium map.

    Every dataset is wrapped in a named FeatureGroup that can be toggled on
    or off via the LayerControl in the top-right corner.

    Layers
    ------
    Boundary (buffered / unbuffered)
        City boundary polygons with and without the 10 km buffer used during
        OSM and GTFS cropping.
    H3 grid (start cells)
        Hex cells from the walking start-node mapping — only occupied cells
        are shown.
    Network: walking / cycling / car
        All road edges drawn as thin polylines; hidden by default.
    Start nodes: walking / cycling / car
        One marker per H3 cell, at the representative network node for that
        mode.  Each mode uses a distinct colour and radius.
    GTFS stops
        Transit stop positions.
    GTFS routes
        All unique consecutive stop-pair links across all trips.
    """
    center_lat, center_lon = geometa.get_center_lat_lon()
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles="CartoDB positron",
    )

    _add_boundary_layers(m, geometa)

    for layer, node_file, edge_file in [
        ("walking", "walking_nodes.parquet", "walking_edges.parquet"),
        ("cycling", "cycling_nodes.parquet", "cycling_edges.parquet"),
        ("car", "car_nodes.parquet", "car_edges.parquet"),
    ]:
        node_path = graph_dir / node_file
        edge_path = graph_dir / edge_file
        if not node_path.exists() or not edge_path.exists():
            continue
        nodes = pl.read_parquet(node_path)
        edges = pl.read_parquet(edge_path)
        coords: dict[int, tuple[float, float]] = {
            row["osm_id"]: (row["lat"], row["long"])
            for row in nodes.iter_rows(named=True)
        }
        _add_network_layer(m, layer, edges, coords)

    start_node_dfs: dict[str, pl.DataFrame] = {}
    for layer in ("walking", "cycling", "car"):
        fn = start_nodes_dir / f"{city_id}_{layer}_h3mapping.parquet"
        if fn.exists():
            start_node_dfs[layer] = pl.read_parquet(fn)

    if "walking" in start_node_dfs:
        _add_h3_layer(m, start_node_dfs["walking"])

    _add_start_node_layers(m, start_node_dfs)

    stops_path = gtfs_dir / "stops.parquet"
    st_path = gtfs_dir / "stop_times.parquet"
    if stops_path.exists():
        stops = pl.read_parquet(stops_path)
        _add_gtfs_stops_layer(m, stops)
        if st_path.exists():
            stop_times = pl.read_parquet(st_path)
            _add_gtfs_routes_layer(m, stops, stop_times)

    folium.LayerControl(collapsed=False).add_to(m)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        m.save(str(output_path))

    return m


def _style(color: str, opacity: float = 0.08, weight: int = 2) -> dict:
    return {
        "color": color,
        "fillColor": color,
        "fillOpacity": opacity,
        "weight": weight,
    }


def _add_boundary_layers(m: folium.Map, geometa: GeoMeta) -> None:
    for name, wkt, color in [
        ("Boundary (buffered)", geometa.boundary_wkt, "#1e40af"),
        ("Boundary (unbuffered)", geometa.unbuffered_boundary_wkt, "#1e3a5f"),
    ]:
        shape = shapely.from_wkt(wkt)
        fg = folium.FeatureGroup(name=name, show=True)
        c = color
        folium.GeoJson(
            shapely.to_geojson(shape),
            style_function=lambda _, c=c: _style(c),
        ).add_to(fg)
        fg.add_to(m)


def _add_network_layer(
    m: folium.Map,
    layer: str,
    edges: pl.DataFrame,
    coords: dict[int, tuple[float, float]],
) -> None:
    color = _NET_COLOR.get(layer, "#000000")
    fg = folium.FeatureGroup(name=f"Network: {layer}", show=False)

    features = []
    rows = edges.iter_rows(named=True)
    for edge in rows:
        src = coords.get(edge["source_osm"])
        dst = coords.get(edge["dest_osm"])
        if src is None or dst is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [src[1], src[0]],
                        [dst[1], dst[0]],
                    ],
                },
                "properties": {},
            }
        )

    c = color
    folium.GeoJson(
        {"type": "FeatureCollection", "features": features},
        style_function=lambda _, c=c: {"color": c, "weight": 1, "opacity": 0.45},
    ).add_to(fg)
    fg.add_to(m)


def _add_h3_layer(m: folium.Map, walking_start_nodes: pl.DataFrame) -> None:
    try:
        import h3
    except ImportError:
        return

    fg = folium.FeatureGroup(name="H3 grid (start cells)", show=True)
    for cell in walking_start_nodes["h3_cell_id"].to_list():
        boundary = h3.cell_to_boundary(cell)
        folium.Polygon(
            locations=list(boundary),
            color="#0369a1",
            fill=True,
            fill_color="#0ea5e9",
            fill_opacity=0.15,
            weight=1,
        ).add_to(fg)
    fg.add_to(m)


def _add_start_node_layers(
    m: folium.Map,
    start_node_dfs: dict[str, pl.DataFrame],
) -> None:
    layer_labels = {
        "walking": "Start nodes: walking",
        "cycling": "Start nodes: cycling",
        "car": "Start nodes: car",
    }
    for layer, sn_df in start_node_dfs.items():
        color = _SN_COLOR.get(layer, "#000000")
        radius = _SN_RADIUS.get(layer, 5)
        fg = folium.FeatureGroup(
            name=layer_labels.get(layer, f"Start nodes: {layer}"),
            show=True,
        )
        for row in sn_df.iter_rows(named=True):
            folium.CircleMarker(
                location=(row["lat"], row["lon"]),
                radius=radius,
                color="#ffffff",
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.9,
                popup=folium.Popup(
                    f"<b>{layer}</b><br>cell: {row['h3_cell_id']}<br>"
                    f"dist: {row['dist']:.1f} m",
                    max_width=220,
                ),
            ).add_to(fg)
        fg.add_to(m)


def _add_gtfs_stops_layer(m: folium.Map, stops: pl.DataFrame) -> None:
    fg = folium.FeatureGroup(name="GTFS stops", show=True)
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["stop_lon"], row["stop_lat"]],
            },
            "properties": {"stop_idx": row["stop_idx"]},
        }
        for row in stops.iter_rows(named=True)
    ]
    folium.GeoJson(
        {"type": "FeatureCollection", "features": features},
        style_function=lambda _: {
            "color": "#7c3aed",
            "fillColor": "#7c3aed",
            "fillOpacity": 0.8,
            "radius": 4,
            "weight": 0,
        },
        marker=folium.CircleMarker(
            radius=4,
            color="#7c3aed",
            fill=True,
            fill_color="#7c3aed",
            fill_opacity=0.8,
            weight=0,
        ),
    ).add_to(fg)
    fg.add_to(m)


def _add_gtfs_routes_layer(
    m: folium.Map,
    stops: pl.DataFrame,
    stop_times: pl.DataFrame,
) -> None:
    fg = folium.FeatureGroup(name="GTFS routes", show=True)

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
        .select(
            pl.col("stop_idx").cast(pl.UInt32),
            pl.col("next_stop_idx").cast(pl.UInt32),
        )
        .unique()
    )

    features = []
    for from_idx, to_idx in links.iter_rows():
        src = stop_coord.get(from_idx)
        dst = stop_coord.get(to_idx)
        if src is None or dst is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [src[1], src[0]],
                        [dst[1], dst[0]],
                    ],
                },
                "properties": {},
            }
        )

    folium.GeoJson(
        {"type": "FeatureCollection", "features": features},
        style_function=lambda _: {"color": "#7c3aed", "weight": 1, "opacity": 0.4},
    ).add_to(fg)
    fg.add_to(m)
