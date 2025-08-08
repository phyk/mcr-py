import polars as pl
import polars_st as st

from mcr_py.gtfs import archive
from mcr_py.utils import key
from mcr_py.utils.logger import Timed


def clean(gtfs_zip_path: str) -> dict[str, pl.DataFrame]:
    """
    Cleans the GTFS data and writes the cleaned data to the output path.
    The resulting files are `trips.csv` and `stop_times.csv`, other files are
    not needed for our algorithms.

    :param gtfs_zip_path: str - The path to the GTFS zip file to be cleaned.
    :returns: dict[str, pl.DataFrame] - A dictionary containing cleaned DataFrames for trips, stop times, stops, and routes.
    """
    with Timed.debug("Reading GTFS data"):
        dfs = archive.read_dfs(gtfs_zip_path)
    trips_df, stop_times_df, stops_df, routes_df = (
        dfs[key.TRIPS_KEY],
        dfs[key.STOP_TIMES_KEY],
        dfs[key.STOPS_KEY],
        dfs[key.ROUTES_KEY],
    )

    with Timed.debug("Removing incompatible trips"):
        trips_df, stop_times_df = remove_circular_trips(trips_df, stop_times_df)

    with Timed.debug("Splitting routes"):
        trips_df, routes_df = split_routes(trips_df, stop_times_df, routes_df)
    with Timed.debug("Preparing dataframes"):
        trips_df = add_first_stop_info(trips_df, stop_times_df)
        stops_df = remove_unused_stops(stop_times_df, stops_df)
        stops_df = add_geometry(stops_df)

    return {
        key.TRIPS_KEY: trips_df,
        key.STOP_TIMES_KEY: stop_times_df,
        key.STOPS_KEY: stops_df,
        key.ROUTES_KEY: routes_df,
    }


def remove_circular_trips(
    trips_df: pl.DataFrame,
    stop_times_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Removes trips that have circular paths.

    Circular paths are not supported by our algorithms.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :returns: tuple[pl.DataFrame, pl.DataFrame] - A tuple containing the filtered trips DataFrame and stop times DataFrame.
    """
    stop_times_df = stop_times_df.with_columns(
        pl.col("stop_id").is_duplicated().over("trip_id").alias("duplicated")
    )
    stop_times_df = stop_times_df.filter(~pl.col("duplicated").any().over("trip_id"))
    trips_df = trips_df.filter(
        pl.col("trip_id").is_in(stop_times_df.get_column("trip_id"))
    )
    return trips_df, stop_times_df


def split_routes(
    trips_df: pl.DataFrame, stop_times_df: pl.DataFrame, routes_df: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Splits routes into one route per actual path.

    In GTFS data, one route can have multiple paths, e.g. one train route mostly
    has two directions. However, sometimes even routes with the same direction
    can have different paths. For our algorithms, it is easier to have one route per path.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :param routes_df: pl.DataFrame - The DataFrame containing route information.
    :returns: tuple[pl.DataFrame, pl.DataFrame] - A tuple containing the updated trips DataFrame and routes DataFrame.
    """
    # first we backup the old route_ids for debugging purposes
    trips_df = trips_df.with_columns(pl.col("route_id").alias("old_route_id"))

    split_routes_by_direction(trips_df)
    paths_df = create_paths_df(trips_df, stop_times_df)
    paths_df = add_unique_route_ids(paths_df)
    trips_df = update_route_ids(trips_df, paths_df)
    routes_df = insert_new_routes(routes_df, trips_df)

    return trips_df, routes_df


def split_routes_by_direction(trips_df: pl.DataFrame) -> pl.DataFrame:
    """
    Splits the route IDs by direction.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :returns: pl.DataFrame - The updated trips DataFrame with split route IDs.
    """
    if "direction_id" in trips_df.columns:
        return trips_df.with_columns(
            pl.col("route_id") + pl.lit("_") + pl.col("direction_id").cast(pl.String)
        )
    else:
        return trips_df


def create_paths_df(
    trips_df: pl.DataFrame, stop_times_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Creates a DataFrame containing route_id, trip_id, and path, where path is a string
    representation of the stops on the route in order.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :returns: pl.DataFrame - A DataFrame containing the paths for each trip.
    """
    trips_stop_times_df = trips_df.join(stop_times_df, on="trip_id")
    paths_df = (
        trips_stop_times_df.sort(by=["route_id", "trip_id", "stop_sequence"])
        .group_by(["route_id", "trip_id"], maintain_order=True)
        .agg(pl.col("stop_id").alias("path"))
    )

    return paths_df


def add_unique_route_ids(paths_df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a new column `new_route_id` to the DataFrame, which is a unique route_id
    for each path.

    :param paths_df: pl.DataFrame - The DataFrame containing paths to be processed.
    :returns: pl.DataFrame - The updated DataFrame with the new unique route_id column.
    """

    return (
        paths_df.sort(["route_id", "trip_id"])
        .with_columns(
            (
                pl.col("route_id").cast(pl.String)
                + "_"
                + pl.col("path").cum_count().over("route_id").cast(pl.String)
            ).alias("new_route_id")
        )
        .drop("path")
    )


def update_route_ids(trips_df: pl.DataFrame, paths_df: pl.DataFrame) -> pl.DataFrame:
    """
    Updates the route IDs in the trips DataFrame based on the paths DataFrame.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param paths_df: pl.DataFrame - The DataFrame containing unique paths and their corresponding route IDs.
    :returns: pl.DataFrame - The updated trips DataFrame with new route IDs.
    """
    trips_df = trips_df.join(paths_df, on=["route_id", "trip_id"])
    trips_df = trips_df.with_columns(pl.col("new_route_id").alias("route_id"))
    trips_df = trips_df.drop("new_route_id")
    return trips_df


def insert_new_routes(routes_df: pl.DataFrame, trips_df: pl.DataFrame) -> pl.DataFrame:
    """
    Reads the old and new route names of each trip and inserts the new routes into
    the routes DataFrame by copying the old routes.

    :param routes_df: pl.DataFrame - The DataFrame containing existing route information.
    :param trips_df: pl.DataFrame - The DataFrame containing updated trip information with new route IDs.
    :returns: pl.DataFrame - The updated routes DataFrame with new routes inserted.
    """
    return (
        routes_df.join(
            trips_df.select(
                pl.col("route_id").alias("new_route_id"), pl.col("old_route_id")
            ),
            left_on="route_id",
            right_on="old_route_id",
            how="left",
        )
        .with_columns(pl.col("new_route_id").alias("route_id"))
        .drop(["new_route_id"])
    )


def add_first_stop_info(
    trips_df: pl.DataFrame, stop_times_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Adds information about the first stop for each trip to the trips DataFrame.

    :param trips_df: pl.DataFrame - The DataFrame containing trip information.
    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :returns: pl.DataFrame - The updated trips DataFrame with first stop information added.
    """
    # add first stop id to trips
    first_stop_times = (
        stop_times_df.with_columns(
            pl.col("stop_sequence").min().over("trip_id").alias("min_stop_sequence")
        )
        .filter(pl.col("stop_sequence") == pl.col("min_stop_sequence"))
        .select(
            pl.col("trip_id"),
            pl.col("stop_id").alias("first_stop_id"),
            pl.col("departure_time").alias("trip_departure_time"),
        )
    )

    return trips_df.join(first_stop_times, on="trip_id", how="left")


def remove_unused_stops(
    stop_times_df: pl.DataFrame, stops_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Removes stops that are not used in the stop times DataFrame.

    :param stop_times_df: pl.DataFrame - The DataFrame containing stop times information.
    :param stops_df: pl.DataFrame - The DataFrame containing stops information.
    :returns: pl.DataFrame - The updated stops DataFrame with unused stops removed.
    """
    stops_df = stops_df.filter(
        pl.col("stop_id").is_in(stop_times_df.get_column("stop_id"))
    )
    return stops_df


def add_geometry(stops_df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds geometry information to the stops DataFrame based on stop coordinates.

    This function creates a new column in the stops DataFrame that contains
    geometric representations of the stops, typically in the form of point geometries.

    :param stops_df: pl.DataFrame - The DataFrame containing stops information, including stop coordinates.
    :returns: pl.DataFrame - The updated stops DataFrame with a new column for geometry.
    """
    return stops_df.with_columns(
        st.from_coords(pl.concat_arr(["stop_lon", "stop_lat"])).alias("geometry")
    )
