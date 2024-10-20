import os
from enum import Enum

import geopandas as gpd

from package import storage
from package.geometa import GeoMeta
from package.logger import Timed, rlog
from package.osm import graph, igraph, osm


class GenerationMethod(Enum):
    IGRAPH = "igraph"
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
    city_id: str,
    osm_path: str,
    stops_path: str,
    geo_meta_path: str,
    avg_walking_speed: float,
    max_walking_duration: int,
    method: GenerationMethod = GenerationMethod.IGRAPH,
) -> dict[str, dict[str, int]]:
    osm_path = osm_path if osm_path else osm.get_osm_path_from_city_id(city_id)

    with Timed.info("Reading stops and geo meta"):
        stops_df = storage.read_gdf(stops_path)
        geo_meta = GeoMeta.load(geo_meta_path)

    if not os.path.exists(osm_path) and city_id:
        rlog.info("Downloading OSM data")
        osm.download_city(city_id, osm_path)
    else:
        rlog.info("Using existing OSM data")

    osm_reader = osm.new_osm_reader(osm_path)

    with Timed.info("Getting OSM graph"):
        nodes, edges = osm.get_graph_for_city_cropped_to_boundary(osm_reader, geo_meta)

    with Timed.info("Creating networkx graph"):
        nx_graph = graph.create_nx_graph(osm_reader, nodes, edges)

    with Timed.info("Adding nearest network node to each stop"):
        stops_df = graph.add_nearest_node_to_stops(stops_df, nx_graph)

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

    with Timed.info(f"Calculating distances between nearby stops using {method.name}"):
        if method == GenerationMethod.IGRAPH:
            source_targets_distance_map = igraph.query_multiple_one_to_many(
                source_targets_map, osm_reader, nodes, edges
            )
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
    stops_df: gpd.GeoDataFrame,
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
