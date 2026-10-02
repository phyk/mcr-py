import heapq
import pathlib

import polars as pl
import polars_h3 as plh3
import polars_st as st
import rustworkx as rx

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
) -> pl.DataFrame:
    """Map each walking start node to the nearest node in a mode network.

    Every walking hex gets a row. When no mode node is reachable from a hex,
    ``pickup_osm_node_id`` is null (and ``pickup_dist``/``pickup_lat``/
    ``pickup_lon`` are null too). The Rust runtime skips null pickups.

    Runs a single multi-source Dijkstra on the reversed walking graph, seeded
    from every mode node that is also present in the walking graph.  Each
    walking node is settled exactly once, recording which mode node it is
    nearest to and the walking graph-distance (metres) to it.
    """
    mode_osm_ids: set[int] = set(mode_nodes["osm_id"].to_list())
    osm_to_rx: dict[int, int] = dict(
        zip(walking_nodes["osm_id"].to_list(), walking_nodes["rx_node_id"].to_list())
    )
    mode_coord: dict[int, tuple[float, float]] = {
        row["osm_id"]: (row["lat"], row["long"])
        for row in mode_nodes.iter_rows(named=True)
    }

    # Seed: every mode node that is also a walking node
    sources: list[tuple[float, int, int]] = [
        (0.0, osm_to_rx[osm_id], osm_id)
        for osm_id in mode_osm_ids
        if osm_id in osm_to_rx
    ]

    # Multi-source Dijkstra on the reversed walking graph.
    # Traversing predecessors gives shortest walking distance TO any mode node.
    dist: dict[int, float] = {}
    origin: dict[int, int] = {}  # rx_node_id -> nearest mode osm_id
    heap = list(sources)
    heapq.heapify(heap)

    while heap:
        d, node, mode_osm = heapq.heappop(heap)
        if node in dist:
            continue
        dist[node] = d
        origin[node] = mode_osm
        for pred in walking_graph.predecessor_indices(node):
            if pred not in dist:
                w = walking_graph.get_edge_data(pred, node)
                heapq.heappush(heap, (d + w, pred, mode_osm))

    rows = []
    for row in walking_start_nodes.iter_rows(named=True):
        rx_id = osm_to_rx.get(row["osm_node_id"])
        if rx_id is None or rx_id not in dist:
            rows.append(
                {
                    "osm_node_id": row["osm_node_id"],
                    "h3_cell_id": row["h3_cell_id"],
                    "pickup_osm_node_id": None,
                    "pickup_dist": None,
                    "pickup_lat": None,
                    "pickup_lon": None,
                    "center_lat": row["center_lat"],
                    "center_lon": row["center_lon"],
                }
            )
            continue
        mode_osm = origin[rx_id]
        pickup_lat, pickup_lon = mode_coord[mode_osm]
        rows.append(
            {
                "osm_node_id": row["osm_node_id"],
                "h3_cell_id": row["h3_cell_id"],
                "pickup_osm_node_id": mode_osm,
                "pickup_dist": dist[rx_id],
                "pickup_lat": pickup_lat,
                "pickup_lon": pickup_lon,
                "center_lat": row["center_lat"],
                "center_lon": row["center_lon"],
            }
        )

    return pl.DataFrame(
        rows,
        schema={
            "osm_node_id": pl.UInt64,
            "h3_cell_id": pl.String,
            "pickup_osm_node_id": pl.UInt64,
            "pickup_dist": pl.Float64,
            "pickup_lat": pl.Float64,
            "pickup_lon": pl.Float64,
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

    Two files are written per layer:
    - ``{city_id}_{layer}_h3mapping.parquet``: full diagnostics, used by the
      visualise pipeline.
    - ``{layer}.parquet``: runtime file consumed by ``read_start_nodes``
      (columns: ``osm_node_id``, ``h3_cell_id``, ``pickup_osm_node_id``).

    Walking defines the canonical H3 cell set. Every other mode produces one
    row per walking hex; ``pickup_osm_node_id`` is null when the nearest mode
    node is unreachable or lies outside the origin hex, signalling the Rust
    runtime to skip private-mode routing for that origin.
    """
    start_nodes_dir.mkdir(parents=True, exist_ok=True)

    with logger.Timed.info("Mapping start nodes (walking)"):
        walking_mapping = calculate_start_nodes(node_dfs["walking"], geometa, resolution)

    # Walking has no pickup node; add null column for schema consistency.
    walking_mapping = walking_mapping.with_columns(
        pl.lit(None, dtype=pl.UInt64).alias("pickup_osm_node_id")
    )
    walking_mapping.write_parquet(start_nodes_dir / f"{city_id}_walking_h3mapping.parquet")
    walking_mapping.select(["osm_node_id", "h3_cell_id", "pickup_osm_node_id"]).write_parquet(
        start_nodes_dir / "walking.parquet"
    )
    logger.rlog.info(f"walking: {len(walking_mapping)} start nodes across H3 cells")

    for layer, nodes in node_dfs.items():
        if layer == "walking":
            continue
        with logger.Timed.info(f"Mapping start nodes ({layer})"):
            mapping = snap_start_nodes_to_network(
                walking_mapping, walking_graph, node_dfs["walking"], nodes
            )

        # Null out all pickup fields when the mode node falls outside the origin
        # H3 hex. All four columns are cleared together so the h3mapping file
        # stays consistent (no orphaned coordinates for a null pickup id).
        outside_hex = (
            pl.col("pickup_osm_node_id").is_not_null()
            & (
                plh3.latlng_to_cell(
                    pl.col("pickup_lat"),
                    pl.col("pickup_lon"),
                    resolution,
                    return_dtype=pl.String,
                )
                != pl.col("h3_cell_id")
            )
        )
        mapping = mapping.with_columns(
            pl.when(outside_hex).then(pl.lit(None, dtype=pl.UInt64)).otherwise(pl.col("pickup_osm_node_id")).alias("pickup_osm_node_id"),
            pl.when(outside_hex).then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("pickup_dist")).alias("pickup_dist"),
            pl.when(outside_hex).then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("pickup_lat")).alias("pickup_lat"),
            pl.when(outside_hex).then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("pickup_lon")).alias("pickup_lon"),
        )

        layer_file = {"cycling": "cycling", "car": "car"}.get(layer, layer)
        mapping.write_parquet(start_nodes_dir / f"{city_id}_{layer}_h3mapping.parquet")
        mapping.select(["osm_node_id", "h3_cell_id", "pickup_osm_node_id"]).write_parquet(
            start_nodes_dir / f"{layer_file}.parquet"
        )
        n_valid = mapping["pickup_osm_node_id"].drop_nulls().len()
        logger.rlog.info(
            f"{layer}: {n_valid}/{len(mapping)} start nodes have a pickup within their H3 cell"
        )
