import pathlib
from datetime import datetime
from typing import Tuple

import polars as pl

from mcr_py.gtfs import archive
from mcr_py.utils import key
from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import Timed, rlog


def crop(
    path: pathlib.Path,
    output: pathlib.Path,
    geo_meta: GeoMeta,
    time_start: datetime,
    time_end: datetime,
) -> None:
    """
    Crops GTFS data based on geographic and temporal constraints.

    :param path: str - The path to the input GTFS data.
    :param output: str - The path where the cropped GTFS data will be saved.
    :param geo_meta: GeoMeta - An object containing geographic metadata for cropping.
    :param time_start: datetime - The start time for the cropping window.
    :param time_end: datetime - The end time for the cropping window.
    :raises ValueError: If the bounding box results in no trips or stops remaining.
    """
    with Timed.debug("Reading GTFS data"):
        dfs = archive.read_dfs(path)

    trips_df, stop_times_df, stops_df, calendar_df, routes_df = (
        dfs[key.TRIPS_KEY],
        dfs[key.STOP_TIMES_KEY],
        dfs[key.STOPS_KEY],
        dfs[key.CALENDAR_KEY],
        dfs[key.ROUTES_KEY],
    )

    n_trips, n_stop_times, n_stops = (
        len(trips_df),
        len(stop_times_df),
        len(stops_df),
    )
    rlog.debug(
        f"""
    # of trips: {n_trips}
    # of stop times: {n_stop_times}
    # of stops: {n_stops}
    """
    )

    stops_df = geo_meta.crop_df(stops_df, key.STOP_LAT_KEY, key.STOP_LON_KEY)
    trips_df, stop_times_df = reconcile_trips_and_stop_times_with_stops(
        trips_df, stop_times_df, stops_df
    )
    rlog.debug(
        f"""
        Bounding Box stops remaining: {len(stops_df)}/{n_stops} ({len(stops_df) / n_stops:.2%})
        Bounding Box trips remaining: {len(trips_df)}/{n_trips} ({len(trips_df) / n_trips:.2%})
        """
    )

    n_trips_after_bbox, n_stops_after_bbox = len(trips_df), len(stops_df)
    if n_trips_after_bbox == 0 or n_stops_after_bbox == 0:
        msg = f"Bounding box is too small, no trips or stops remain: {n_trips_after_bbox} trips, {n_stops_after_bbox} stops"
        raise ValueError(msg)

    trips_df, calendar_df = crop_trips(trips_df, calendar_df, time_start, time_end)
    stop_times_df = reconcile_stop_times_with_trips(stop_times_df, trips_df)
    stops_df = reconcile_stops_with_stop_times(stops_df, stop_times_df)

    rlog.debug(
        f"""
        Time range trips remaining: {len(trips_df)}/{n_trips_after_bbox} ({len(trips_df) / n_trips_after_bbox:.2%})
        Time range stops remaining: {len(stops_df)}/{n_stops_after_bbox} ({len(stops_df) / n_stops_after_bbox:.2%})
        Time range stop times remaining: {len(stop_times_df)}/{n_stop_times} ({len(stop_times_df) / n_stop_times:.2%})
        """
    )

    rlog.info(
        f"""\
        Crop results:
        # of trips: {len(trips_df)} ({len(trips_df) / n_trips:.2%})
        # of stop times: {len(stop_times_df)} ({len(stop_times_df) / n_stop_times:.2%})
        # of stops: {len(stops_df)} ({len(stops_df) / n_stops:.2%})"""
    )

    archive.write_dfs(
        {
            key.TRIPS_KEY: trips_df,
            key.STOP_TIMES_KEY: stop_times_df,
            key.STOPS_KEY: stops_df,
            key.CALENDAR_KEY: calendar_df,
            key.ROUTES_KEY: routes_df,  # todo this is not being cropped, but is small anyways
        },
        output,
    )


def reconcile_trips_and_stop_times_with_stops(
    trips_df: pl.DataFrame,
    stop_times_df: pl.DataFrame,
    stops_df: pl.DataFrame,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Crops trips and stop times to only include those associated with the given stops.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :param stops_df: pl.DataFrame - The DataFrame containing stop information.
    :returns: Tuple[pl.DataFrame, pl.DataFrame] - The cropped trips and stop times DataFrames.
    """
    stop_ids = stops_df.get_column(key.STOP_ID_KEY).unique()
    stop_times_df = stop_times_df.filter(
        pl.col(key.STOP_ID_KEY).is_in(stop_ids.implode())
        & (pl.col(key.TRIP_ID_KEY).is_duplicated())
    )
    trips_df = trips_df.filter(
        pl.col(key.TRIP_ID_KEY).is_in(
            stop_times_df.get_column(key.TRIP_ID_KEY).unique().implode()
        )
    )  # type: ignore

    return trips_df, stop_times_df


def crop_trips(
    trips_df: pl.DataFrame,
    calendar_df: pl.DataFrame,
    time_start: datetime,
    time_end: datetime,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Crops trips to those that occur within the specified time range.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param calendar_df: pl.DataFrame - The DataFrame containing calendar information.
    :param time_start: datetime - The start time for the cropping window.
    :param time_end: datetime - The end time for the cropping window.
    :returns: Tuple[pl.DataFrame, pl.DataFrame] - The cropped trips and calendar DataFrames.
    """
    calendar_df = calendar_df.filter(
        (
            pl.col(key.CALENDAR_START_DATE_KEY)
            .cast(pl.String)
            .str.to_date(format=key.CALENDAR_DATE_TIME_FORMAT)
            <= time_end
        )
        & (
            pl.col(key.CALENDAR_END_DATE_KEY)
            .cast(pl.String)
            .str.to_date(format=key.CALENDAR_DATE_TIME_FORMAT)
            >= time_start
        )
    )

    service_ids = calendar_df.get_column(key.SERVICE_ID_KEY).unique()
    trips_df = trips_df.filter(pl.col(key.SERVICE_ID_KEY).is_in(service_ids.implode()))  # type: ignore

    return trips_df, calendar_df


def reconcile_stop_times_with_trips(
    stop_times_df: pl.DataFrame,
    trips_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Crops stop times to only include those associated with the specified trips.

    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :returns: pl.DataFrame - The cropped stop times DataFrame.
    """
    trip_ids = trips_df.get_column(key.TRIP_ID_KEY).unique()
    stop_times_df = stop_times_df.filter(pl.col(key.TRIP_ID_KEY).is_in(trip_ids.implode()))

    return stop_times_df


def reconcile_stops_with_stop_times(
    stops_df: pl.DataFrame,
    stop_times_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Crops stops to only include those associated with the specified stop times.

    :param stops_df: pl.DataFrame - The DataFrame containing stop information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :returns: pl.DataFrame - The cropped stops DataFrame.
    """
    stop_ids = stop_times_df.get_column(key.STOP_ID_KEY).unique()
    stops_df = stops_df.filter(pl.col(key.STOP_ID_KEY).is_in(stop_ids.implode()))

    return stops_df
