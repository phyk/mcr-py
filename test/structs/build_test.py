import pytest
import polars as pl
from mcr_py.package.structs.build import (
    build_structures,
    create_stop_times_by_trip,
    create_trip_ids_by_route_sorted_by_departure,
    create_stops_by_route_ordered,
    create_routes_by_stop,
    create_id_sets,
    create_idx_by_stop_by_route,
    create_times_by_stop_by_trip,
    validate_structs_dict,
    unpack_structs,
)


@pytest.fixture
def trips_df():
    # Create a sample DataFrame for trips
    return pl.DataFrame(
        {
            "trip_id": ["trip1", "trip2", "trip3"],
            "route_id": ["route1", "route1", "route2"],
            "trip_departure_time": ["08:00:00", "09:00:00", "10:00:00"],
        }
    )


@pytest.fixture
def stop_times_df():
    # Create a sample DataFrame for stop times
    return pl.DataFrame(
        {
            "trip_id": ["trip1", "trip1", "trip2", "trip2", "trip3"],
            "arrival_time": [
                "08:00:00",
                "08:05:00",
                "09:00:00",
                "09:10:00",
                "10:00:00",
            ],
            "departure_time": [
                "08:00:00",
                "08:05:00",
                "09:00:00",
                "09:10:00",
                "10:00:00",
            ],
            "stop_id": ["stop1", "stop2", "stop1", "stop3", "stop2"],
            "stop_sequence": [1, 2, 1, 2, 1],
        }
    )


def test_create_stop_times_by_trip(stop_times_df):
    result = create_stop_times_by_trip(stop_times_df)
    assert isinstance(result, dict)
    assert len(result) == 3  # Expecting 3 trips


def test_create_trip_ids_by_route_sorted_by_departure(trips_df):
    result = create_trip_ids_by_route_sorted_by_departure(trips_df)
    assert isinstance(result, dict)
    assert len(result) == 2  # Expecting 2 routes


def test_create_stops_by_route_ordered(trips_df, stop_times_df):
    trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(trips_df)
    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    result = create_stops_by_route_ordered(trip_ids_by_route, stop_times_by_trip)
    assert isinstance(result, dict)
    assert len(result) == 2  # Expecting 2 routes


def test_create_routes_by_stop(trips_df, stop_times_df):
    trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(trips_df)
    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    stops_by_route = create_stops_by_route_ordered(
        trip_ids_by_route, stop_times_by_trip
    )
    result = create_routes_by_stop(stops_by_route)
    assert isinstance(result, dict)
    assert len(result) == 3  # Expecting 3 stops


def test_create_id_sets(trips_df):
    routes_by_stop = {
        "stop1": {"route1"},
        "stop2": {"route1", "route2"},
        "stop3": {"route1"},
    }
    result = create_id_sets(trips_df, routes_by_stop)
    assert isinstance(result, tuple)
    assert len(result) == 3  # Expecting 3 sets
    assert isinstance(result[0], set)  # stop_id_set
    assert isinstance(result[1], set)  # route_id_set
    assert isinstance(result[2], set)  # trip_id_set


def test_create_idx_by_stop_by_route(trips_df, stop_times_df):
    trip_ids_by_route = create_trip_ids_by_route_sorted_by_departure(trips_df)
    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    stops_by_route = create_stops_by_route_ordered(
        trip_ids_by_route, stop_times_by_trip
    )
    result = create_idx_by_stop_by_route(stops_by_route)
    assert isinstance(result, dict)
    assert len(result) == 2  # Expecting 2 routes


def test_create_times_by_stop_by_trip(stop_times_df):
    stop_times_by_trip = create_stop_times_by_trip(stop_times_df)
    result = create_times_by_stop_by_trip(stop_times_by_trip)
    assert isinstance(result, dict)
    assert len(result) == 3  # Expecting 3 trips


def test_validate_structs_dict():
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


def test_unpack_structs():
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
