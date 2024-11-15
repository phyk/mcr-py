import os
import textwrap
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import geopandas as gpd
import overpy
import pandas as pd
from joblib import Memory
from shapely.geometry import Polygon

from mcr_py.package.key import TMP_DIR_LOCATION
from mcr_py.package.logger import rlog

api = overpy.Overpass()


def order_ways_and_nodes(result):
    ways_dict = {way.id: [node for node in way.nodes] for way in result.ways}

    current_way_id, current_way_nodes = ways_dict.popitem()
    ordered_nodes = current_way_nodes

    while ways_dict:
        previous_way_id = current_way_id
        for next_way_id, next_way_nodes in ways_dict.items():
            if ordered_nodes[-1] == next_way_nodes[0]:
                ordered_nodes.extend(next_way_nodes[1:])
                current_way_id = next_way_id
                break
            elif ordered_nodes[-1] == next_way_nodes[-1]:
                ordered_nodes.extend(reversed(next_way_nodes[:-1]))
                current_way_id = next_way_id
                break
        if previous_way_id == current_way_id:
            break
        ways_dict.pop(current_way_id)

    return [(node.lat, node.lon) for node in ordered_nodes]


def fetch_boundary_polygon(city_name_german: str, admin_level: int):
        # Koeln -> Admin level 6
    # Berlin -> Admin level 4
    query = f"""
    [out:json][timeout:50];
    area["name"="{city_name_german}"]->.searchArea;
    relation["boundary"="administrative"]["admin_level"="{admin_level}"](area.searchArea);
    out body;
    >;
    out skel qt;
    """

    result = api.query(query)
    boundary_coords = order_ways_and_nodes(result)
    boundary_polygon = Polygon([(lon, lat)
                        for lat, lon in boundary_coords])

    return boundary_polygon


def build(attr: list[tuple], bounding_box: tuple[float, float, float, float]) -> str:
    bounding_box = (bounding_box[1], bounding_box[0], bounding_box[3], bounding_box[2])
    query = textwrap.dedent(
        """
        [out:json];
        """
    )
    for category, feature in attr:
        query += textwrap.dedent(
            f"""
            (
                node["{category}"="{feature}"]{bounding_box};
                way["{category}"="{feature}"]{bounding_box};
                relation[{category}="{feature}"]{bounding_box};
            );
            out center;
            """
        )
    return query


memory = Memory(os.path.join(TMP_DIR_LOCATION, "overpy"), verbose=0)


@memory.cache
def query(query: str):
    result = api.query(query)

    def get_name(obj) -> str:
        try_attributes = ["name", "shop", "amenity", "leisure"]
        name = None
        for attr in try_attributes:
            if attr in obj.tags:
                name = obj.tags[attr]
                break
        if name is None:
            rlog.warning(f"Could not find name for {obj}")
            name = "Unknown"
        return name

    pois = []
    for obj in result.nodes + result.ways + result.relations:
        poi = {
            "name": get_name(obj),
            "id": obj.id,
        }
        if isinstance(obj, overpy.Node):
            poi["lat"] = obj.lat
            poi["lon"] = obj.lon
        elif isinstance(obj, overpy.Way):
            poi["lat"] = obj.center_lat
            poi["lon"] = obj.center_lon
        elif isinstance(obj, overpy.Relation):
            poi["lat"] = obj.center_lat
            poi["lon"] = obj.center_lon
        else:
            raise ValueError(f"Unknown type: {type(obj)}")
        pois.append(poi)

    if len(pois) == 0:
        print("No POIs found")
        return None

    pois = pd.DataFrame(pois)

    pois = gpd.GeoDataFrame(pois, geometry=gpd.points_from_xy(pois.lon, pois.lat))
    return pois


def fetch_and_merge_queries_async(
    queries: list[tuple[str, str]], area_of_interest: Polygon
) -> gpd.GeoDataFrame:
    def fetch_data(name_query_pair: tuple[str, str]) -> tuple[str, Any]:
        name, query_str = name_query_pair
        return (name, query(query_str))

    with ThreadPoolExecutor() as executor:
        poi_groups = list(executor.map(fetch_data, queries))

    for name, pois in poi_groups:
        pois["type"] = name
    pois: gpd.GeoDataFrame = pd.concat([pois for _, pois in poi_groups])  # type: ignore
    pois: gpd.GeoDataFrame = pois[pois.intersects(area_of_interest)]  # type: ignore

    return pois
