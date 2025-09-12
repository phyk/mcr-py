import logging

import polars as pl
from mcr_py.gtfs.clean import (
    add_first_stop_info,
    add_unique_route_ids,
    create_paths_df,
    remove_unused_stops,
    split_routes_by_direction,
)


def test_split_routes_by_direction(trips_df: pl.DataFrame) -> None:
    trips_df = split_routes_by_direction(trips_df)

    expected_route_ids = pl.Series(
        values=["route1_0", "route1_0", "route1_1"], name="route_id"
    )
    assert (trips_df.get_column("route_id") == expected_route_ids).all()


def test_create_paths_df(trips_df: pl.DataFrame, stop_times_df: pl.DataFrame) -> None:
    paths_df = create_paths_df(trips_df, stop_times_df)
    expected_paths = pl.Series(
        values=[
            ["stop1", "stop2", "stop3"],
            ["stop1", "stop4", "stop3"],
            ["stop3", "stop2", "stop1"],
        ],
        name="path",
    )
    assert (paths_df.get_column("path") == expected_paths).all()


def test_add_unique_route_ids(paths_df: pl.DataFrame) -> None:
    paths_df = add_unique_route_ids(paths_df)
    expected_route_ids = pl.Series(
        values=["route1_1", "route1_2", "route1_3"], name="new_route_id"
    )
    assert (paths_df.get_column("new_route_id") == expected_route_ids).all()


def test_add_first_stop_info(trips_df: pl.DataFrame, stop_times_df: pl.DataFrame) -> None:
    trips_df = add_first_stop_info(trips_df, stop_times_df)
    expected_first_stop_ids = pl.Series(
        values=["stop1", "stop1", "stop3"], name="first_stop_id"
    )
    expected_first_stop_departure_times = pl.Series(
        values=["00:00:00", "01:00:00", "02:00:00"], name="trip_departure_time"
    )
    logging.info(trips_df.columns)
    assert (trips_df.get_column("first_stop_id") == expected_first_stop_ids).all()
    assert (
        trips_df.get_column("trip_departure_time") == expected_first_stop_departure_times
    ).all()


def test_remove_unused_stops(stop_times_df: pl.DataFrame, stops_df: pl.DataFrame) -> None:
    stops_df = remove_unused_stops(stop_times_df, stops_df)
    expected_stops = pl.Series(values=["stop1", "stop2", "stop3", "stop4"], name="stop_id")
    assert stops_df.get_column("stop_id").is_in(expected_stops.implode()).all()
