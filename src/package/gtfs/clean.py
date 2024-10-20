import geopandas as gpd
import pandas as pd

from package import key
from package.gtfs import archive
from package.logger import Timed


def clean(gtfs_zip_path: str) -> dict[str, pd.DataFrame]:
    """
    Cleans the GTFS data and writes the cleaned data to the output path.
    The resulting files are `trips.csv` and `stop_times.csv`, other files are
    not needed for our algorithms.
    """
    with Timed.info("Reading GTFS data"):
        dfs = archive.read_dfs(gtfs_zip_path)
    trips_df, stop_times_df, stops_df, routes_df = (
        dfs[key.TRIPS_KEY],
        dfs[key.STOP_TIMES_KEY],
        dfs[key.STOPS_KEY],
        dfs[key.ROUTES_KEY],
    )

    with Timed.info("Removing incompatible trips"):
        trips_df, stop_times_df = remove_circular_trips(trips_df, stop_times_df)

    with Timed.info("Splitting routes"):
        trips_df, routes_df = split_routes(trips_df, stop_times_df, routes_df)
    with Timed.info("Preparing dataframes"):
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
    trips_df: pd.DataFrame,
    stop_times_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Removes trips that have circular paths.

    Circular paths are not supported by our algorithms.
    """
    circular_trips = stop_times_df.groupby("trip_id").apply(is_circular_trip)
    circular_trips = circular_trips[circular_trips].index

    trips_df = trips_df[~trips_df["trip_id"].isin(circular_trips)].copy()
    stop_times_df = stop_times_df[~stop_times_df["trip_id"].isin(circular_trips)].copy()

    return trips_df, stop_times_df


def is_circular_trip(stop_times_df: pd.DataFrame) -> bool:
    return stop_times_df["stop_id"].nunique() != len(stop_times_df)


def split_routes(
    trips_df: pd.DataFrame, stop_times_df: pd.DataFrame, routes_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits routes into one route per actual path.

    In GTFS data one route can have multiple paths, e.g. one train route mostly
    has two directions. However, sometimes even routes with the same direction
    can have different paths.
    For our algorithms it is easier to have one route per path.
    """
    # first we backup the old route_ids for debugging purposes
    trips_df["old_route_id"] = trips_df["route_id"]

    split_routes_by_direction(trips_df)
    paths_df = create_paths_df(trips_df, stop_times_df)
    paths_df = add_unique_route_ids(paths_df)
    trips_df = update_route_ids(trips_df, paths_df)
    routes_df = insert_new_routes(routes_df, trips_df)

    return trips_df, routes_df


def split_routes_by_direction(trips_df: pd.DataFrame):
    trips_df["route_id"] = (
        trips_df["route_id"] + "_" + trips_df["direction_id"].astype(str)
    )


def create_paths_df(
    trips_df: pd.DataFrame, stop_times_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Creates a dataframe route_id, trip_id, and path, where path is a string
    representation of the stops on the route in order.
    """
    trips_stop_times_df = pd.merge(trips_df, stop_times_df, on="trip_id")
    paths_df = (
        trips_stop_times_df.sort_values(["route_id", "trip_id", "stop_sequence"])
        .groupby(["route_id", "trip_id"])["stop_id"]
        .apply(list)
        .apply(str)
        .reset_index()
    )
    paths_df = paths_df.rename(columns={"stop_id": "path"})
    return paths_df


def add_unique_route_ids(paths_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column `new_route_id` to the dataframe, which is a unique route_id
    for each path.
    """
    known_route_paths = {}
    path_counter_per_route = {}
    path_id_by_path = {}

    paths_df["new_route_id"] = paths_df["route_id"]

    for i, row in paths_df.iterrows():
        path_counter = path_counter_per_route.get(row["route_id"], 0)
        known_paths = known_route_paths.get(row["route_id"], set())

        path_id = None

        path = row["path"]
        if path in known_paths:
            path_id = path_id_by_path[path]
        else:
            path_id = chr(ord("A") + path_counter)
            paths_df.at[i, "new_route_id"] = row["route_id"] + "_" + path_id

            known_paths.add(path)
            known_route_paths[row["route_id"]] = known_paths

            path_counter_per_route[row["route_id"]] = path_counter + 1

            path_id_by_path[path] = path_id

        paths_df.at[i, "new_route_id"] = row["route_id"] + "_" + path_id

    return paths_df.drop(columns=["path"])  # we don't need the path column anymore


def update_route_ids(trips_df: pd.DataFrame, paths_df: pd.DataFrame) -> pd.DataFrame:
    trips_df = trips_df.merge(paths_df, on=["route_id", "trip_id"])
    trips_df["route_id"] = trips_df["new_route_id"]
    trips_df = trips_df.drop(columns=["new_route_id"])
    return trips_df


def insert_new_routes(routes_df: pd.DataFrame, trips_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reads the old and new route names of each trip and inserts the new routes into
    the routes_df by copying the old routes.
    """
    new_rows = []

    route_id_map = trips_df.set_index("route_id")["old_route_id"].to_dict()
    # Iterate over each row in the trips_df
    for route_id, old_route_id in route_id_map.items():
        # Find the corresponding row in the routes_df based on old_route_id
        old_route_row = routes_df[routes_df["route_id"] == old_route_id].iloc[0]

        # Create a new row by copying the old route row and update the route_id
        new_route_row = old_route_row.copy()
        new_route_row["route_id"] = route_id

        new_rows.append(new_route_row)

    return pd.DataFrame(new_rows)


def add_first_stop_info(
    trips_df: pd.DataFrame, stop_times_df: pd.DataFrame
) -> pd.DataFrame:
    # add first stop id to trips
    first_stop_times = (
        stop_times_df.sort_values(["trip_id", "stop_sequence"])
        .groupby("trip_id")
        .first()[["stop_id", "departure_time"]]
        .rename(
            columns={
                "stop_id": "first_stop_id",
                "departure_time": "trip_departure_time",
            }
        )
    )

    return trips_df.merge(
        first_stop_times, left_on="trip_id", right_index=True, how="left"
    )


def remove_unused_stops(
    stop_times_df: pd.DataFrame, stops_df: pd.DataFrame
) -> pd.DataFrame:
    stops_df = stops_df[stops_df["stop_id"].isin(stop_times_df["stop_id"])]
    return stops_df


def add_geometry(stops_df: pd.DataFrame) -> pd.DataFrame:
    return gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat),
    )
