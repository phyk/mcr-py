import os
import pathlib

import polars as pl
import pytest
import shapely
from mcr_py.gtfs.clean import (
    add_first_stop_info,
    create_paths_df,
    split_routes,
    split_routes_by_direction,
)
from mcr_py.structs.build import (
    create_id_sets,
    create_idx_by_stop_by_route,
    create_routes_by_stop,
    create_stop_times_by_trip,
    create_stops_by_route_ordered,
    create_times_by_stop_by_trip,
    create_trip_ids_by_route_sorted_by_departure,
)
from mcr_py.utils.geometa import GeoMeta


@pytest.fixture(scope="session")
def testdata_path() -> pathlib.Path:
    return pathlib.Path(os.path.dirname(__file__)) / "testdata"


ROUTE1_ID = "route1"
ROUTES = {
    ROUTE1_ID: {
        "trip1": {
            "stop_times": {
                "stop1": {
                    "stop_sequence": 1,
                    "departure_time": "00:00:00",
                    "arrival_time": "00:00:00",
                },
                "stop2": {
                    "stop_sequence": 2,
                    "departure_time": "00:10:00",
                    "arrival_time": "00:10:00",
                },
                "stop3": {
                    "stop_sequence": 3,
                    "departure_time": "00:20:00",
                    "arrival_time": "00:20:00",
                },
            },
            "direction": 0,
        },
        "trip2": {
            "stop_times": {
                "stop1": {
                    "stop_sequence": 1,
                    "departure_time": "01:00:00",
                    "arrival_time": "01:00:00",
                },
                "stop4": {
                    "stop_sequence": 2,
                    "departure_time": "01:10:00",
                    "arrival_time": "01:10:00",
                },
                "stop3": {
                    "stop_sequence": 3,
                    "departure_time": "01:20:00",
                    "arrival_time": "01:20:00",
                },
            },
            "direction": 0,
        },
        "trip3": {
            "stop_times": {
                "stop3": {
                    "stop_sequence": 1,
                    "departure_time": "02:00:00",
                    "arrival_time": "02:00:00",
                },
                "stop2": {
                    "stop_sequence": 2,
                    "departure_time": "02:10:00",
                    "arrival_time": "02:10:00",
                },
                "stop1": {
                    "stop_sequence": 3,
                    "departure_time": "02:20:00",
                    "arrival_time": "02:20:00",
                },
            },
            "direction": 1,
        },
    }
}


@pytest.fixture(scope="session")
def trips_df() -> pl.DataFrame:
    # route_id, trip_id, direction_id
    trips = ROUTES[ROUTE1_ID]
    return pl.DataFrame(
        [
            {
                "route_id": ROUTE1_ID,
                "trip_id": trip_id,
                "direction_id": trips[trip_id]["direction"],
            }
            for trip_id in trips
        ]
    )


@pytest.fixture
def stop_times_df() -> pl.DataFrame:
    # trip_id, departure_time, arrival_time, stop_id, stop_sequence
    trips = ROUTES[ROUTE1_ID]
    stop_times = []
    for trip_id in trips:
        stop_times.extend(
            [
                {
                    "trip_id": trip_id,
                    "departure_time": trips[trip_id]["stop_times"][stop_id]["departure_time"],
                    "arrival_time": trips[trip_id]["stop_times"][stop_id]["arrival_time"],
                    "stop_id": stop_id,
                    "stop_sequence": trips[trip_id]["stop_times"][stop_id]["stop_sequence"],
                }
                for stop_id in trips[trip_id]["stop_times"]
            ]
        )
    return pl.DataFrame(stop_times)


@pytest.fixture
def paths_df(trips_df: pl.DataFrame, stop_times_df: pl.DataFrame) -> pl.DataFrame:
    split_routes_by_direction(trips_df)
    return create_paths_df(trips_df, stop_times_df)


@pytest.fixture
def stops_df() -> pl.DataFrame:
    # stop_id
    return pl.DataFrame({"stop_id": ["stop1", "stop2", "stop3", "stop4", "stop5"]})


@pytest.fixture
def routes_df() -> pl.DataFrame:
    # route_id, direction_id
    return pl.DataFrame({"route_id": [ROUTE1_ID]})


# Sample data for testing
@pytest.fixture
def sample_polygon():
    return shapely.geometry.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


@pytest.fixture
def geo_meta(sample_polygon):
    return GeoMeta.create(boundary=sample_polygon, crs="EPSG:4326", crs_target="EPSG:3857")


@pytest.fixture
def cleaned_trips_df(
    trips_df: pl.DataFrame, stop_times_df: pl.DataFrame, routes_df: pl.DataFrame
) -> pl.DataFrame:
    trips_df, routes_df = split_routes(trips_df, stop_times_df, routes_df)
    trips_df = add_first_stop_info(trips_df, stop_times_df)
    return trips_df


@pytest.fixture
def trip_ids_by_route(cleaned_trips_df: pl.DataFrame) -> dict[str, list[str]]:
    return create_trip_ids_by_route_sorted_by_departure(cleaned_trips_df)


@pytest.fixture
def stop_times_by_trip(stop_times_df: pl.DataFrame) -> dict[str, list[dict[str, str]]]:
    return create_stop_times_by_trip(stop_times_df)


@pytest.fixture
def stops_by_route(
    trip_ids_by_route: dict[str, list[str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> dict[str, list[str]]:
    return create_stops_by_route_ordered(trip_ids_by_route, stop_times_by_trip)


@pytest.fixture
def routes_by_stop(
    stops_by_route: dict[str, list[str]],
) -> dict[str, set[str]]:
    return create_routes_by_stop(stops_by_route)


@pytest.fixture
def idx_by_stop_by_route(
    stops_by_route: dict[str, list[str]],
) -> dict[str, dict[str, int]]:
    return create_idx_by_stop_by_route(stops_by_route)


@pytest.fixture
def times_by_stop_by_trip(
    stop_times_by_trip: dict[str, list[dict[str, str]]],
) -> dict[str, dict[str, tuple[int, int]]]:
    return create_times_by_stop_by_trip(stop_times_by_trip)


@pytest.fixture
def test_create_id_sets(
    trips_df: pl.DataFrame, routes_by_stop: dict[str, set[str]]
) -> tuple[set[str], set[str], set[str]]:
    return create_id_sets(trips_df, routes_by_stop)
