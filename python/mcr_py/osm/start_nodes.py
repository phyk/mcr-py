import math
import pathlib

import polars as pl
import polars_h3 as plh3
import polars_st as st
import rustworkx as rx
from pyproj import Transformer

from mcr_py.utils import logger
from mcr_py.utils.geometa import Buffering, GeoMeta, crs_to_srid


def calculate_start_nodes(
    nodes: pl.DataFrame, geometa: GeoMeta, resolution: int
) -> pl.DataFrame:
    """Pick one representative OSM node per H3 cell inside the city boundary.

    For every occupied cell we keep the node closest to the cell centre (in the
    projected sink CRS), then drop cells whose centre falls outside the
    unbuffered city boundary. Output columns are what `read_start_nodes`
    requires (``osm_node_id``, ``h3_cell_id``) plus diagnostics.
    """
    srid = crs_to_srid(geometa.crs_target)
    boundary = pl.lit([geometa.get_convex_hull_coord_list(buffering=Buffering.UNBUFFERED)])
    return (
        nodes.lazy()
        .with_columns(
            plh3.latlng_to_cell(
                pl.col("lat"), pl.col("long"), resolution, return_dtype=pl.String
            ).alias("h3_cell_id"),
            st.point(pl.concat_arr("long", "lat")).st.set_srid(4326).alias("node_pt"),
        )
        .with_columns(
            plh3.cell_to_lat("h3_cell_id").alias("center_lat"),
            plh3.cell_to_lng("h3_cell_id").alias("center_lon"),
        )
        .with_columns(
            st.point(pl.concat_arr("center_lon", "center_lat"))
            .st.set_srid(4326)
            .alias("center_pt")
        )
        .with_columns(
            st.to_srid("node_pt", srid=srid)
            .st.distance(st.to_srid("center_pt", srid=srid))
            .alias("dist")
        )
        .group_by("h3_cell_id")
        .agg(pl.all().sort_by("dist").first())
        .filter(
            st.point(pl.concat_arr("center_lon", "center_lat"))
            .st.set_srid(4326)
            .st.within(st.polygon(boundary).st.set_srid(4326))
        )
        .select(
            pl.col("osm_id").alias("osm_node_id"),
            pl.col("dist"),
            pl.col("lat"),
            pl.col("long").alias("lon"),
            pl.col("center_lat"),
            pl.col("center_lon"),
            pl.col("h3_cell_id"),
        )
        .collect()
    )


def snap_start_nodes_to_network(
    walking_start_nodes: pl.DataFrame,
    walking_graph: rx.PyDiGraph,
    walking_nodes: pl.DataFrame,
    mode_nodes: pl.DataFrame,
    geometa: GeoMeta,
) -> pl.DataFrame:
    """Map each walking start node to the nearest node in a mode network.

    If the walking start node's OSM ID exists in the mode network it is used
    directly (dist = 0).  Otherwise Dijkstra is run on the walking graph from
    that node and the closest reachable node that exists in the mode network is
    chosen.  Cells with no reachable mode node are dropped.

    ``dist`` is the geometric distance in the projected sink CRS (metres)
    between the walking start node and the snapped mode node.
    """
    mode_osm_ids: set[int] = set(mode_nodes["osm_id"].to_list())
    osm_to_rx: dict[int, int] = dict(
        zip(walking_nodes["osm_id"].to_list(), walking_nodes["rx_node_id"].to_list())
    )
    # walking rx_node_id -> mode osm_id, for nodes shared between the two networks
    rx_to_mode_osm: dict[int, int] = {
        osm_to_rx[osm_id]: osm_id
        for osm_id in mode_osm_ids
        if osm_id in osm_to_rx
    }
    mode_coord: dict[int, tuple[float, float]] = {
        row["osm_id"]: (row["lat"], row["long"])
        for row in mode_nodes.iter_rows(named=True)
    }

    transformer = Transformer.from_crs("EPSG:4326", geometa.crs_target, always_xy=True)

    rows = []
    for row in walking_start_nodes.iter_rows(named=True):
        walking_osm: int = row["osm_node_id"]
        h3_cell: str = row["h3_cell_id"]
        center_lat, center_lon = row["center_lat"], row["center_lon"]
        walking_lat, walking_lon = row["lat"], row["lon"]

        if walking_osm in mode_osm_ids:
            mode_osm = walking_osm
            dist = 0.0
        else:
            rx_id = osm_to_rx.get(walking_osm)
            if rx_id is None:
                continue
            lengths: dict[int, float] = rx.dijkstra_shortest_path_lengths(
                walking_graph, rx_id, edge_cost_fn=lambda x: x
            )
            candidates = {node: d for node, d in lengths.items() if node in rx_to_mode_osm}
            if not candidates:
                continue
            best_rx = min(candidates, key=lambda node: candidates[node])
            mode_osm = rx_to_mode_osm[best_rx]

            mode_lat, mode_lon = mode_coord[mode_osm]
            wx, wy = transformer.transform(walking_lon, walking_lat)
            mx, my = transformer.transform(mode_lon, mode_lat)
            dist = math.sqrt((wx - mx) ** 2 + (wy - my) ** 2)

        lat, lon = mode_coord[mode_osm]
        rows.append(
            {
                "osm_node_id": mode_osm,
                "h3_cell_id": h3_cell,
                "dist": dist,
                "lat": lat,
                "lon": lon,
                "center_lat": center_lat,
                "center_lon": center_lon,
            }
        )

    return pl.DataFrame(
        rows,
        schema={
            "osm_node_id": pl.UInt64,
            "h3_cell_id": pl.String,
            "dist": pl.Float64,
            "lat": pl.Float64,
            "lon": pl.Float64,
            "center_lat": pl.Float64,
            "center_lon": pl.Float64,
        },
    )


def build_start_nodes(
    node_dfs: dict[str, pl.DataFrame],
    geometa: GeoMeta,
    resolution: int,
    city_id: str,
    start_nodes_dir: pathlib.Path,
    walking_graph: rx.PyDiGraph,
) -> None:
    """Write the walking and per-mode start-node parquets.

    Files are named ``{city_id}_{layer}_h3mapping.parquet``.

    Walking is computed first and defines the canonical cell set.  Every other
    mode snaps to the walking start node when that node is present in the mode
    network, or walks the pedestrian graph outward to find the nearest reachable
    mode node.  This guarantees spatial alignment of origins across modes.
    """
    start_nodes_dir.mkdir(parents=True, exist_ok=True)

    with logger.Timed.info("Mapping start nodes (walking)"):
        walking_mapping = calculate_start_nodes(node_dfs["walking"], geometa, resolution)
    walking_mapping.write_parquet(start_nodes_dir / f"{city_id}_walking_h3mapping.parquet")
    logger.rlog.info(f"walking: {len(walking_mapping)} start nodes across H3 cells")

    for layer, nodes in node_dfs.items():
        if layer == "walking":
            continue
        with logger.Timed.info(f"Mapping start nodes ({layer})"):
            mapping = snap_start_nodes_to_network(
                walking_mapping, walking_graph, node_dfs["walking"], nodes, geometa
            )
        mapping.write_parquet(start_nodes_dir / f"{city_id}_{layer}_h3mapping.parquet")
        logger.rlog.info(f"{layer}: {len(mapping)} start nodes across H3 cells")
