import os
from enum import Enum

import polars_st as st

from mcr_py.utils import storage
from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import Timed, rlog
from mcr_py.osm import graph, osm
from mcr_py import add_nearest_node_to_df


class GenerationMethod(Enum):
    RUSTWORKX = "rustworkx"
    FAST_PATH = "fast_path"

    @classmethod
    def from_str(cls, method: str) -> "GenerationMethod":
        if method.upper() not in cls.all():
            raise ValueError(f"Unknown generation method: {method}")
        return cls[method.upper()]

    @classmethod
    def all(cls) -> list[str]:
        return [method.name for method in cls]


def generate(
    city_name: str,
    cache_path: str,
    stops_path: str,
    avg_walking_speed: float,
    max_walking_duration: int,
    method: GenerationMethod = GenerationMethod.RUSTWORKX,
) -> dict[str, dict[str, int]]:
    nodes = storage.read_df(f"{cache_path}/{city_name}_walking_nodes.parquet")
    edges = storage.read_df(f"{cache_path}/{city_name}_walking_edges.parquet")
    with Timed.info("Reading stops and geo meta"):
        stops_df = storage.read_df(stops_path)

    with Timed.info("Creating rustworkx graph"):
        (nodes, edges, rx_graph) = graph.create_rx_graph(nodes, edges)

    with Timed.info("Adding nearest network node to each stop"):
        stops_df = add_nearest_node_to_df(stops_df, nodes, "EPSG:4839")

    with Timed.info("Finding potential nearby stops for each stop"):
        nearby_stops_map = create_nearby_stops_map(
            stops_df, avg_walking_speed, max_walking_duration
        )

    stop_to_node_map: dict[str, int] = stops_df.set_index("stop_id")[
        "nearest_node"
    ].to_dict()
    node_to_stop_map: dict[int, str] = stops_df.set_index("nearest_node")[
        "stop_id"
    ].to_dict()

    # this map contains the one-to-many queries that have to be solved on the graph
    source_targets_map: dict[int, list[int]] = {
        stop_to_node_map[stop_id]: [
            stop_to_node_map[stop_id] for stop_id in nearby_stops
        ]
        for stop_id, nearby_stops in nearby_stops_map.items()
    }
    # If this boils down to having all distances between stops, then rustworkx floyd_warshall_numpy might be the same or faster

    with Timed.info(f"Calculating distances between nearby stops using {method.name}"):
        if method == GenerationMethod.RUSTWORKX:
            source_targets_distance_map = graph.shortest_paths(rx_graph, num_threads=6)
        elif method == GenerationMethod.FAST_PATH:
            raise NotImplementedError()

    footpaths: dict[str, dict[str, int]] = {}
    for source_node, targets_distance_map in source_targets_distance_map.items():
        stop_id = node_to_stop_map[source_node]
        footpaths[stop_id] = {  # type: ignore
            node_to_stop_map[target_node]: int(distance / avg_walking_speed)
            for target_node, distance in targets_distance_map.items()
        }

    return footpaths


def create_nearby_stops_map(
    stops_df: st.GeoDataFrame,
    avg_walking_speed: float,
    max_walking_duration: int,
) -> dict[str, list[str]]:
    # crs for beeline distance
    stops_df = stops_df.copy().set_crs("EPSG:4326").to_crs("EPSG:32634")  # type: ignore

    max_walking_distance = avg_walking_speed * max_walking_duration

    nearby_stops_map: dict[str, list[str]] = {}
    for _, row in stops_df.iterrows():
        nearby_stops = stops_df.loc[
            stops_df.geometry.distance(row.geometry) < max_walking_distance
        ].stop_id.tolist()

        # remove self
        nearby_stops = [stop_id for stop_id in nearby_stops if stop_id != row.stop_id]
        nearby_stops_map[row.stop_id] = nearby_stops

    return nearby_stops_map
