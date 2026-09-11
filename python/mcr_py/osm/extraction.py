import pathlib

import polars as pl
import rustworkx as rx

from mcr_py._mcr_py import (
    DownloadMode,
    load_osm_cycling,
    load_osm_driving,
    load_osm_pois,
    load_osm_walking,
)
from mcr_py.osm.graph import create_rx_graph, crop_graph_to_largest_component
from mcr_py.utils import logger
from mcr_py.utils.geometa import GeoMeta


def extract_networks(
    geometa: GeoMeta,
    city_id: str,
    osm_path: pathlib.Path,
    cache_path: pathlib.Path,
    graph_dir: pathlib.Path,
) -> tuple[dict[str, pl.DataFrame], rx.PyDiGraph]:
    """Extract walking/cycling/driving networks and write them for mcr-rust.

    Returns a tuple of:
    - node DataFrames keyed by mcr-rust layer name (``walking`` / ``cycling``
      / ``car``); the walking entry retains ``rx_node_id`` for start-node
      snapping and is not written to parquet with that column
    - the cropped walking graph (rx.PyDiGraph) for start-node snapping
    """
    graph_dir.mkdir(parents=True, exist_ok=True)
    cache_path.mkdir(parents=True, exist_ok=True)
    hull = geometa.get_convex_hull_coord_list()
    archive = str(osm_path)
    out = str(cache_path)

    with logger.Timed.info("Extracting walking network"):
        walking_nodes, walking_edges = load_osm_walking(
            city_id, hull, archive, out, mode=DownloadMode.LocalOnly
        )
        _nodes_rx, _edges_rx, _graph = create_rx_graph(walking_nodes, walking_edges)
        walking_nodes, walking_edges, walking_graph = crop_graph_to_largest_component(
            _graph, _nodes_rx, _edges_rx
        )
        walking_nodes = walking_nodes.select(["osm_id", "lat", "long"])
        walking_edges = walking_edges.select(["source_osm", "dest_osm", "length"])
        # subgraph() remaps node indices to 0-based; rebuild rx_node_id from the
        # cropped graph's payloads so start-node Dijkstra uses valid indices.
        osm_to_new_rx: dict[int, int] = {
            int(osm_id): new_idx
            for new_idx, osm_id in zip(walking_graph.node_indices(), walking_graph.nodes())
        }
        walking_nodes = walking_nodes.with_columns(
            pl.col("osm_id")
            .map_elements(lambda oid: osm_to_new_rx[oid], return_dtype=pl.UInt64)
            .alias("rx_node_id")
        )
        walking_nodes.select(["osm_id", "lat", "long"]).write_parquet(
            graph_dir / "walking_nodes.parquet"
        )
        walking_edges.write_parquet(graph_dir / "walking_edges.parquet")

    with logger.Timed.info("Extracting cycling network"):
        cycling_nodes, cycling_edges = load_osm_cycling(
            city_id,
            hull,
            reverse_edges=True,
            archive_path=archive,
            outpath=out,
            mode=DownloadMode.LocalOnly,
        )
        _nodes_rx, _edges_rx, _graph = create_rx_graph(cycling_nodes, cycling_edges)
        cycling_nodes, cycling_edges, _ = crop_graph_to_largest_component(
            _graph, _nodes_rx, _edges_rx
        )
        cycling_nodes = cycling_nodes.select(["osm_id", "lat", "long"])
        cycling_edges = cycling_edges.select(["source_osm", "dest_osm", "length"])
        cycling_nodes.write_parquet(graph_dir / "cycling_nodes.parquet")
        cycling_edges.write_parquet(graph_dir / "cycling_edges.parquet")

    with logger.Timed.info("Extracting driving network"):
        driving_nodes, driving_edges = load_osm_driving(
            city_id, hull, archive, out, mode=DownloadMode.LocalOnly
        )
        _nodes_rx, _edges_rx, _graph = create_rx_graph(driving_nodes, driving_edges)
        driving_nodes, driving_edges, _ = crop_graph_to_largest_component(
            _graph, _nodes_rx, _edges_rx
        )
        # mcr-rust's car layer is osmtools' driving network.
        driving_nodes = driving_nodes.select(["osm_id", "lat", "long"])
        driving_edges = driving_edges.select(["source_osm", "dest_osm", "length"])
        driving_nodes.write_parquet(graph_dir / "car_nodes.parquet")
        driving_edges.write_parquet(graph_dir / "car_edges.parquet")

    node_dfs = {
        "walking": walking_nodes,
        "cycling": cycling_nodes,
        "car": driving_nodes,
    }
    return node_dfs, walking_graph


def extract_pois(
    geometa: GeoMeta,
    city_id: str,
    osm_path: pathlib.Path,
    cache_path: pathlib.Path,
    graph_dir: pathlib.Path,
    walking_nodes: pl.DataFrame,
) -> None:
    """Extract POIs, snapping each to its nearest walking node.

    POIs are matched against `walking_nodes`, so every ``nearest_osm_node`` is
    guaranteed to exist in the assembled graph — `add_poi_to_walking_graph`
    panics otherwise.
    """
    with logger.Timed.info("Extracting POIs"):
        pois = load_osm_pois(
            city_id,
            geometa.get_bounding_box_as_coord_list(),
            str(osm_path),
            str(cache_path),
            mode=DownloadMode.LocalOnly,
            nodes_to_match_df=walking_nodes,
            nodes_to_match_path=None,
        )
        pois.write_parquet(graph_dir / "poi_nodes.parquet")
