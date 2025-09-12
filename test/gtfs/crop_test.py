import os
from datetime import datetime
from unittest.mock import patch

import polars as pl
import pytest
from mcr_py.gtfs.crop import (
    crop,
    crop_trips,
    reconcile_stop_times_with_trips,
    reconcile_stops_with_stop_times,
    reconcile_trips_and_stop_times_with_stops,
)


@pytest.fixture
def mock_data(tmp_path):
    # Create mock data for testing
    trips_data = {
        "trip_id": ["trip1", "trip2", "trip3"],
        "service_id": ["service1", "service2", "service1"],
    }
    stop_times_data = {
        "trip_id": ["trip1", "trip1", "trip2", "trip3"],
        "stop_id": ["stop1", "stop2", "stop3", "stop1"],
        "stop_sequence": [1, 2, 1, 1],
        "arrival_time": ["08:00:00", "08:05:00", "09:00:00", "10:00:00"],
        "departure_time": ["08:00:00", "08:05:00", "09:00:00", "10:00:00"],
    }
    stops_data = {
        "stop_id": ["stop1", "stop2", "stop3"],
        "stop_name": ["Stop One", "Stop Two", "Stop Three"],
        "stop_lat": [0.5, 2, 1],
        "stop_lon": [0.5, 2, 0],
    }
    calendar_data = {
        "service_id": ["service1", "service2"],
        "start_date": ["20230101", "20230101"],
        "end_date": ["20231231", "20231231"],
    }
    routes_data = {"mock": ["mock"]}

    # Write mock CSV files
    pl.DataFrame(trips_data).write_csv(tmp_path / "trips.csv")
    pl.DataFrame(stop_times_data).write_csv(tmp_path / "stop_times.csv")
    pl.DataFrame(stops_data).write_csv(tmp_path / "stops.csv")
    pl.DataFrame(calendar_data).write_csv(tmp_path / "calendar.csv")
    pl.DataFrame(routes_data).write_csv(tmp_path / "routes.csv")

    return {
        "trips": str(tmp_path / "trips.csv"),
        "stop_times": str(tmp_path / "stop_times.csv"),
        "stops": str(tmp_path / "stops.csv"),
        "calendar": str(tmp_path / "calendar.csv"),
        "routes": str(tmp_path / "routes.csv"),
    }


@patch("mcr_py.gtfs.archive.read_dfs")
def test_crop(mock_read_dfs, mock_data, geo_meta, tmp_path) -> None:
    # Mock the read_dfs function to return the mock data
    mock_read_dfs.return_value = {
        "trips": pl.read_csv(mock_data["trips"]),
        "stop_times": pl.read_csv(mock_data["stop_times"]),
        "stops": pl.read_csv(mock_data["stops"]),
        "calendar": pl.read_csv(mock_data["calendar"]),
        "routes": pl.read_csv(mock_data["routes"]),
    }

    time_start = datetime(2023, 1, 1, 0, 0)
    time_end = datetime(2023, 12, 31, 23, 59)
    output_path = tmp_path / "output_path.zip"

    crop(mock_data["trips"], output_path, geo_meta, time_start, time_end)

    # Check if the output files are created and contain expected data
    assert os.path.exists(output_path)
    # Additional checks can be added here based on the expected output


def test_reconcile_trips_and_stop_times_with_stops(mock_data) -> None:
    trips_df = pl.read_csv(mock_data["trips"])
    stop_times_df = pl.read_csv(mock_data["stop_times"])
    stops_df = pl.read_csv(mock_data["stops"])

    filtered_trips, filtered_stop_times = reconcile_trips_and_stop_times_with_stops(
        trips_df, stop_times_df, stops_df
    )

    assert len(filtered_trips) == 1  # Expecting only trips with valid stop times
    assert len(filtered_stop_times) == 2  # Expecting valid stop times


def test_crop_trips(mock_data) -> None:
    trips_df = pl.read_csv(mock_data["trips"])
    calendar_df = pl.read_csv(mock_data["calendar"])
    time_start = datetime(2023, 1, 1, 0, 0)
    time_end = datetime(2023, 12, 31, 23, 59)

    cropped_trips, cropped_calendar = crop_trips(trips_df, calendar_df, time_start, time_end)

    assert len(cropped_trips) == 3  # Expecting all trips to be within the time range
    assert len(cropped_calendar) == 2  # Expecting both services to be returned


def test_reconcile_stop_times_with_trips(mock_data) -> None:
    stop_times_df = pl.read_csv(mock_data["stop_times"])
    trips_df = pl.read_csv(mock_data["trips"])

    reconciled_stop_times = reconcile_stop_times_with_trips(stop_times_df, trips_df)

    assert len(reconciled_stop_times) == 4


def test_reconcile_stops_with_stop_times(mock_data) -> None:
    stops_df = pl.read_csv(mock_data["stops"])
    stop_times_df = pl.read_csv(mock_data["stop_times"])

    reconciled_stops = reconcile_stops_with_stop_times(stops_df, stop_times_df)
    assert len(reconciled_stops) == 3
