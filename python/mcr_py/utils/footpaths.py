from enum import Enum
from typing import Any
import polars as pl


from mcr_py.utils import storage, key
from mcr_py.utils.logger import Timed
from mcr_py.osm import graph
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
    method: GenerationMethod = GenerationMethod.RUSTWORKX,
) -> dict[str, dict[str, int]]:
    nodes = storage.read_df(f"{cache_path}/{city_name}_walking_nodes.parquet")
    edges = storage.read_df(f"{cache_path}/{city_name}_walking_edges.parquet")
    with Timed.info("Reading stops and geo meta"):
        stops_df = storage.read_df(stops_path)

    with Timed.info("Creating rustworkx graph"):
        (nodes, edges, rx_graph) = graph.create_rx_graph(nodes, edges)

    with Timed.info("Adding nearest network node to each stop"):
        stops_df = add_nearest_node_to_df(
            stops_df.with_columns(
                pl.col(key.STOP_LAT_KEY).alias("lat"),
                pl.col(key.STOP_LON_KEY).alias("long"),
            ),
            nodes,
            "EPSG:4839",
        )
        stops_df = stops_df.join(
            nodes.select("osm_id", "rx_node_id"),
            left_on="nearest_node_osm_id",
            right_on="osm_id",
        )

    node_to_stop_map: dict[int, dict[str, Any]] = stops_df.rows_by_key(
        "rx_node_id", named=True, unique=True
    )

    with Timed.info(f"Calculating distances between nearby stops using {method.name}"):
        if method == GenerationMethod.RUSTWORKX:
            source_targets_distance_map = graph.shortest_paths(rx_graph, num_threads=6)
        elif method == GenerationMethod.FAST_PATH:
            raise NotImplementedError()

    footpaths: dict[str, dict[str, int]] = {}
    for source_node, targets_distance_map in source_targets_distance_map.items():
        if source_node not in node_to_stop_map:
            continue
        stop_id = node_to_stop_map[source_node]["stop_id"]
        footpaths[stop_id] = {  # type: ignore
            node_to_stop_map[target_node]["stop_id"]: int(distance / avg_walking_speed)
            for target_node, distance in targets_distance_map.items()
            if target_node in node_to_stop_map
        }

    return footpaths


# def create_nearby_stops_map(
#     stops_df: st.GeoDataFrame,
#     avg_walking_speed: float,
#     max_walking_duration: int,
# ) -> dict[str, list[str]]:
#     # crs for beeline distance
#     stops_df = stops_df.copy().set_crs("EPSG:4326").to_crs("EPSG:32634")  # type: ignore

#     max_walking_distance = avg_walking_speed * max_walking_duration

#     nearby_stops_map: dict[str, list[str]] = {}
#     for _, row in stops_df.iterrows():
#         nearby_stops = stops_df.loc[
#             stops_df.geometry.distance(row.geometry) < max_walking_distance
#         ].stop_id.tolist()

#         # remove self
#         nearby_stops = [stop_id for stop_id in nearby_stops if stop_id != row.stop_id]
#         nearby_stops_map[row.stop_id] = nearby_stops

#     return nearby_stops_map
