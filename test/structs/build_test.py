import polars as pl
import pytest
from mcr_py.structs.build import (
    create_id_sets,
    create_idx_by_stop_by_route,
    create_routes_by_stop,
    create_stop_times_by_trip,
    create_stops_by_route_ordered,
    create_times_by_stop_by_trip,
    create_trip_ids_by_route_sorted_by_departure,
    unpack_structs,
    validate_structs_dict,
)
from mcr_py.utils import strtime


def test_create_stops_by_route_ordered(cleaned_trips_df, stop_times_df) -> None:
    trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df)
    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    result = create_stops_by_route_ordered(trip_ids_by_route, stop_times_by_trip)
    assert isinstance(result, dict)
    assert len(result) == 3


def test_validate_structs_dict() -> None:
    valid_structs = {
        "stop_times_by_trip": {},
        "trip_ids_by_route": {},
        "stops_by_route": {},
        "routes_by_stop": {},
        "idx_by_stop_by_route": {},
        "times_by_stop_by_trip": {},
        "stop_id_set": {},
        "route_id_set": {},
        "trip_id_set": {},
    }
    validate_structs_dict(valid_structs)  # Should not raise an exception

    invalid_structs = {
        "stop_times_by_trip": {},
        "trip_ids_by_route": {},
        # Missing keys
    }
    with pytest.raises(Exception, match="Structs dict missing key"):
        validate_structs_dict(invalid_structs)


def test_unpack_structs() -> None:
    structs = {
        "stop_times_by_trip": {},
        "trip_ids_by_route": {},
        "stops_by_route": {},
        "routes_by_stop": {},
        "idx_by_stop_by_route": {},
        "times_by_stop_by_trip": {},
        "stop_id_set": {},
        "route_id_set": {},
        "trip_id_set": {},
    }
    result = unpack_structs(structs)
    assert len(result) == 6


def test_create_stop_times_by_trip(stop_times_df: pl.DataFrame) -> None:
    expected_stop_times_by_trip = {
        "trip1": [
            {
                "departure_time": "00:00:00",
                "arrival_time": "00:00:00",
                "stop_id": "stop1",
                "stop_sequence": 1,
            },
            {
                "departure_time": "00:10:00",
                "arrival_time": "00:10:00",
                "stop_id": "stop2",
                "stop_sequence": 2,
            },
            {
                "departure_time": "00:20:00",
                "arrival_time": "00:20:00",
                "stop_id": "stop3",
                "stop_sequence": 3,
            },
        ],
        "trip2": [
            {
                "departure_time": "01:00:00",
                "arrival_time": "01:00:00",
                "stop_id": "stop1",
                "stop_sequence": 1,
            },
            {
                "departure_time": "01:10:00",
                "arrival_time": "01:10:00",
                "stop_id": "stop4",
                "stop_sequence": 2,
            },
            {
                "departure_time": "01:20:00",
                "arrival_time": "01:20:00",
                "stop_id": "stop3",
                "stop_sequence": 3,
            },
        ],
        "trip3": [
            {
                "departure_time": "02:00:00",
                "arrival_time": "02:00:00",
                "stop_id": "stop3",
                "stop_sequence": 1,
            },
            {
                "departure_time": "02:10:00",
                "arrival_time": "02:10:00",
                "stop_id": "stop2",
                "stop_sequence": 2,
            },
            {
                "departure_time": "02:20:00",
                "arrival_time": "02:20:00",
                "stop_id": "stop1",
                "stop_sequence": 3,
            },
        ],
    }

    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    assert stop_times_by_trip == expected_stop_times_by_trip


def test_create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df: pl.DataFrame) -> None:
    expected_trip_ids_by_route = {
        "route1_1": ["trip1"],
        "route1_2": ["trip2"],
        "route1_3": ["trip3"],
    }
    trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df)
    assert trip_ids_by_route == expected_trip_ids_by_route


def test_create_stops_by_route(
    trip_ids_by_route: dict[str, list[str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> None:
    stops_by_route = create_stops_by_route_ordered(trip_ids_by_route, stop_times_by_trip)

    expected_stops_by_route = {
        "route1_1": ["stop1", "stop2", "stop3"],
        "route1_2": ["stop1", "stop4", "stop3"],
        "route1_3": ["stop3", "stop2", "stop1"],
    }
    assert stops_by_route == expected_stops_by_route


def test_create_routes_by_stop(stops_by_route: dict[str, list[str]]) -> None:
    routes_by_stop = create_routes_by_stop(stops_by_route)

    expected_routes_by_stop = {
        "stop1": {"route1_3", "route1_1", "route1_2"},
        "stop2": {"route1_3", "route1_1"},
        "stop3": {"route1_3", "route1_1", "route1_2"},
        "stop4": {"route1_2"},
    }

    assert routes_by_stop == expected_routes_by_stop


def test_create_id_sets(
    cleaned_trips_df: pl.DataFrame, routes_by_stop: dict[str, set[str]]
) -> None:
    stop_id_set, route_id_set, trip_id_set = create_id_sets(cleaned_trips_df, routes_by_stop)

    stop_id_set_expected = {"stop1", "stop2", "stop3", "stop4"}
    route_id_set_expected = {"route1_1", "route1_2", "route1_3"}
    trip_id_set_expected = {"trip1", "trip2", "trip3"}

    assert stop_id_set == stop_id_set_expected
    assert route_id_set == route_id_set_expected
    assert trip_id_set == trip_id_set_expected


def test_create_idx_by_stop_by_route(
    stops_by_route: dict[str, list[str]],
) -> None:
    idx_by_stop_by_route = create_idx_by_stop_by_route(stops_by_route)

    expected_idx_by_stop_by_route = {
        "route1_1": {
            "stop1": 0,
            "stop2": 1,
            "stop3": 2,
        },
        "route1_2": {
            "stop1": 0,
            "stop4": 1,
            "stop3": 2,
        },
        "route1_3": {
            "stop3": 0,
            "stop2": 1,
            "stop1": 2,
        },
    }
    assert idx_by_stop_by_route == expected_idx_by_stop_by_route


def test_create_times_by_stop_by_trip(
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> None:
    times_by_stop_by_trip = create_times_by_stop_by_trip(stop_times_by_trip)

    expected_times_by_stop_by_trip = {
        "trip1": {
            "stop1": (
                strtime.str_time_to_seconds("00:00:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("00:00:00", accuracy_multiplier=10),
            ),
            "stop2": (
                strtime.str_time_to_seconds("00:10:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("00:10:00", accuracy_multiplier=10),
            ),
            "stop3": (
                strtime.str_time_to_seconds("00:20:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("00:20:00", accuracy_multiplier=10),
            ),
        },
        "trip2": {
            "stop1": (
                strtime.str_time_to_seconds("01:00:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("01:00:00", accuracy_multiplier=10),
            ),
            "stop4": (
                strtime.str_time_to_seconds("01:10:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("01:10:00", accuracy_multiplier=10),
            ),
            "stop3": (
                strtime.str_time_to_seconds("01:20:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("01:20:00", accuracy_multiplier=10),
            ),
        },
        "trip3": {
            "stop3": (
                strtime.str_time_to_seconds("02:00:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("02:00:00", accuracy_multiplier=10),
            ),
            "stop2": (
                strtime.str_time_to_seconds("02:10:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("02:10:00", accuracy_multiplier=10),
            ),
            "stop1": (
                strtime.str_time_to_seconds("02:20:00", accuracy_multiplier=10),
                strtime.str_time_to_seconds("02:20:00", accuracy_multiplier=10),
            ),
        },
    }

    assert times_by_stop_by_trip == expected_times_by_stop_by_trip
