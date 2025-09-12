from typing import Any

import polars as pl

from mcr_py.mcr.data import ACCURACY_MULTIPLIER
from mcr_py.utils.key import (
    IDX_BY_STOP_BY_ROUTE_KEY,
    ROUTE_ID_SET_KEY,
    ROUTES_BY_STOP_KEY,
    STOP_ID_SET_KEY,
    STOP_TIMES_BY_TRIP_KEY,
    STOPS_BY_ROUTE_KEY,
    TIMES_BY_STOP_BY_TRIP_KEY,
    TRIP_ID_SET_KEY,
    TRIP_IDS_BY_ROUTE_KEY,
)
from mcr_py.utils.logger import Timed
from mcr_py.utils.strtime import str_time_to_seconds

STRUCTS_KEYS = [
    STOP_TIMES_BY_TRIP_KEY,
    TRIP_IDS_BY_ROUTE_KEY,
    STOPS_BY_ROUTE_KEY,
    ROUTES_BY_STOP_KEY,
    IDX_BY_STOP_BY_ROUTE_KEY,
    TIMES_BY_STOP_BY_TRIP_KEY,
    STOP_ID_SET_KEY,
    ROUTE_ID_SET_KEY,
    TRIP_ID_SET_KEY,
]


def build_structures(trips_df: pl.DataFrame, stop_times_df: pl.DataFrame) -> dict[str, Any]:
    """
    Builds various data structures from trips and stop times DataFrames.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :returns: dict[str, Any] - A dictionary containing built data structures.
    """

    with Timed.info("Building structures"):
        with Timed.debug("Creating `stop_times_by_trip`"):
            stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
        # print(stop_times_by_trip[])
        with Timed.debug("Creating `trip_ids_by_route`"):
            trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(trips_df)
        with Timed.debug("Creating `stops_by_route`"):
            stops_by_route = create_stops_by_route_ordered(
                trip_ids_by_route, stop_times_by_trip
            )
        with Timed.debug("Creating `routes_by_stop`"):
            routes_by_stop = create_routes_by_stop(stops_by_route)
        with Timed.debug("Creating `idx_by_stop_by_route`"):
            idx_by_stop_by_route = create_idx_by_stop_by_route(stops_by_route)
        with Timed.debug("Creating `times_by_stop_by_trip`"):
            times_by_stop_by_trip = create_times_by_stop_by_trip(stop_times_by_trip)

        with Timed.debug("Creating id sets"):
            stop_id_set, route_id_set, trip_id_set = create_id_sets(trips_df, routes_by_stop)

    data = {
        STOP_TIMES_BY_TRIP_KEY: stop_times_by_trip,
        TRIP_IDS_BY_ROUTE_KEY: trip_ids_by_route,
        STOPS_BY_ROUTE_KEY: stops_by_route,
        ROUTES_BY_STOP_KEY: routes_by_stop,
        IDX_BY_STOP_BY_ROUTE_KEY: idx_by_stop_by_route,
        TIMES_BY_STOP_BY_TRIP_KEY: times_by_stop_by_trip,
        STOP_ID_SET_KEY: stop_id_set,
        ROUTE_ID_SET_KEY: route_id_set,
        TRIP_ID_SET_KEY: trip_id_set,
    }
    return data


def create_stop_times_by_trip(stop_times_df: pl.DataFrame) -> dict:
    """
    Creates a dictionary mapping trip IDs to their corresponding stop times.

    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times debugrmation.
    :returns: dict - A dictionary where each key is a trip ID and the value is a list of stop times.
    """
    with Timed.debug("creating stop_times_by_trip dictionary from dataframe"):
        stop_times_by_trip = (
            stop_times_df.select(
                pl.col("trip_id").cast(pl.String),
                pl.col("arrival_time"),
                pl.col("departure_time"),
                pl.col("stop_id").cast(pl.String),
                pl.col("stop_sequence"),
            )
            .sort(by=["trip_id", "stop_sequence"])
            .rows_by_key("trip_id", named=True)
        )
    return stop_times_by_trip


def create_trip_ids_by_route_sorted_by_departure(
    trips_df: pl.DataFrame,
) -> dict[str, list[str]]:
    """
    Creates a dictionary mapping route IDs to a list of trip IDs sorted by departure time.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :returns: dict[str, list[str]] - A dictionary where each key is a route ID and the value is a list of trip IDs.
    """
    return {
        k: v
        for k, (v,) in trips_df.sort(by=["trip_departure_time"])
        .select(pl.col("route_id"), pl.col("trip_id").cast(pl.String))
        .group_by("route_id", maintain_order=True)
        .agg(pl.col("trip_id"))
        .rows_by_key(key="route_id", unique=True)
        .items()
    }


def create_stops_by_route_ordered(
    trip_ids_by_route: dict[str, list[str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> dict[str, list[str]]:
    """
    Creates a dictionary mapping route IDs to ordered lists of stop IDs.

    :param trip_ids_by_route: dict[str, list[str]] - A dictionary mapping route IDs to trip IDs.
    :param stop_times_by_trip: dict[str, list[dict[str, str]]] - A dictionary mapping trip IDs to their stop times.
    :returns: dict[str, list[str]] - A dictionary where each key is a route ID and the value is an ordered list of stop IDs.
    """
    stops_by_route: dict[str, list[str]] = {}
    for route_id, trip_ids in trip_ids_by_route.items():
        stops_ordered: list[str] = []
        # we only need the ordered stops, but use the set to check for duplicates
        stops = set()
        for trip_id in trip_ids:
            trip_stop_times = stop_times_by_trip[trip_id]
            for stop_time in trip_stop_times:
                stop = stop_time["stop_id"]
                if stop not in stops:
                    stops_ordered.append(stop)
                    stops.add(stop)

        stops_by_route[route_id] = stops_ordered

    return stops_by_route


def create_routes_by_stop(stops_by_route: dict[str, list[str]]) -> dict[str, set[str]]:
    """
    Creates a dictionary mapping stop IDs to sets of route IDs.

    :param stops_by_route: dict[str, list[str]] - A dictionary mapping route IDs to ordered lists of stop IDs.
    :returns: dict[str, set[str]] - A dictionary where each key is a stop ID and the value is a set of route IDs.
    """
    routes_by_stop: dict[str, set[str]] = {}
    for route_id, stops in stops_by_route.items():
        for stop_id in stops:
            routes = routes_by_stop.get(stop_id, set())
            routes.add(route_id)
            routes_by_stop[stop_id] = routes
    assert isinstance(list(routes_by_stop.keys())[0], str)

    return routes_by_stop


def create_id_sets(
    trips_df: pl.DataFrame, routes_by_stop: dict[str, set[str]]
) -> tuple[set[str], set[str], set[str]]:
    """
    Creates sets of unique stop IDs, route IDs, and trip IDs.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param routes_by_stop: dict[str, set[str]] - A dictionary mapping stop IDs to sets of route IDs.
    :returns: tuple[set[str], set[str], set[str]] - A tuple containing sets of stop IDs, route IDs, and trip IDs.
    """
    stop_id_set = set(routes_by_stop.keys())  # some stops are not part of any trip
    route_id_set = set(trips_df["route_id"].unique())
    trip_id_set = set(trips_df["trip_id"].unique())

    return stop_id_set, route_id_set, trip_id_set


def create_idx_by_stop_by_route(
    stops_by_route: dict[str, list[str]],
) -> dict[str, dict[str, int]]:
    """
    Creates a dictionary mapping route IDs to dictionaries of stop IDs and their corresponding indices.

    :param stops_by_route: dict[str, list[str]] - A dictionary mapping route IDs to ordered lists of stop IDs.
    :returns: dict[str, dict[str, int]] - A dictionary where each key is a route ID and the value is a dictionary mapping stop IDs to their indices.
    """
    idx_by_stop_by_route = {
        k: {stop: idx for idx, stop in enumerate(v)} for k, v in stops_by_route.items()
    }
    return idx_by_stop_by_route


def create_times_by_stop_by_trip(
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> dict[str, dict[str, tuple[int, int]]]:
    """
    Creates a dictionary mapping trip IDs to dictionaries of stop IDs and their arrival and departure times.

    :param stop_times_by_trip: dict[str, list[dict[str, str]]] - A dictionary mapping trip IDs to their stop times.
    :returns: dict[str, dict[str, tuple[int, int]]] - A dictionary where each key is a trip ID and the value is a dictionary mapping stop IDs to their arrival and departure times as tuples.
    """
    return {
        trip_id: {
            stop["stop_id"]: (
                str_time_to_seconds(
                    stop["arrival_time"], accuracy_multiplier=ACCURACY_MULTIPLIER
                ),
                str_time_to_seconds(
                    stop["departure_time"], accuracy_multiplier=ACCURACY_MULTIPLIER
                ),
            )
            for stop in stops
        }
        for (trip_id, stops) in stop_times_by_trip.items()
    }


def validate_structs_dict(structs: dict) -> None:
    """
    Validates that the required keys are present in the structures dictionary.

    :param structs: dict - The dictionary containing various data structures.
    :raises Exception: If any required key is missing from the dictionary.
    """
    for key in STRUCTS_KEYS:
        if key not in structs:
            msg = f"Structs dict missing key {key}"
            raise Exception(msg)


def unpack_structs(
    structs: dict,
) -> tuple[
    dict[str, list[str]],  # TripIdsByRouteSortedByDeparture
    dict[str, list[str]],  # StopsByRouteOrdered
    dict[str, dict[str, int]],  # IdxByStopByRoute
    dict[str, set[str]],  # RoutesByStop
    dict[str, dict[str, tuple[int, int]]],  # TimesByStopByTrip
    set[str],  # StopIdSet
]:
    """
    Unpacks the structures dictionary into its component parts.

    :param structs: dict - The dictionary containing various data structures.
    :returns: tuple - A tuple containing the unpacked structures in the following order:
        - TripIdsByRouteSortedByDeparture
        - StopsByRouteOrdered
        - IdxByStopByRoute
        - RoutesByStop
        - TimesByStopByTrip
        - StopIdSet
    :raises Exception: If the structures dictionary is missing any required keys.
    """
    validate_structs_dict(structs)
    return (
        structs[TRIP_IDS_BY_ROUTE_KEY],
        structs[STOPS_BY_ROUTE_KEY],
        structs[IDX_BY_STOP_BY_ROUTE_KEY],
        structs[ROUTES_BY_STOP_KEY],
        structs[TIMES_BY_STOP_BY_TRIP_KEY],
        structs[STOP_ID_SET_KEY],
    )
