import overpy
from shapely.geometry import Polygon

api = overpy.Overpass()


def order_ways_and_nodes(result):
    """
    Orders nodes from the Overpass API result based on their connectivity in ways.

    :param result: overpy.Result - The result object containing ways and nodes from the Overpass API.
    :returns: list[tuple[float, float]] - A list of ordered tuples representing the latitude and longitude of nodes.
    """
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


def fetch_boundary_polygon(city_name_german: str, admin_level: int) -> Polygon:
    """
    Fetches the boundary polygon for a given city and administrative level using the Overpass API.

    :param city_name_german: str - The name of the city in German to query.
    :param admin_level: int - The administrative level for the boundary (e.g., 6 for Koeln, 4 for Berlin).
    :returns: Polygon - A Shapely Polygon object representing the boundary of the specified city.
    """
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
    boundary_polygon = Polygon([(lon, lat) for lat, lon in boundary_coords])

    return boundary_polygon
