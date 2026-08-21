import pathlib

import polars as pl

from mcr_py.utils import key
from mcr_py.utils.logger import rlog


def gtfs_to_indexed_parquet(dfs: dict[str, pl.DataFrame], out_dir: pathlib.Path) -> None:
    """Assign dense integer indices and write the mcr-rust GTFS parquet layout.

    Input DataFrames must already be cleaned and normalised (deduped, times in
    seconds) — see ``mcr_py.gtfs.clean.clean``.

    Writes ``stops.parquet``, ``trips.parquet``, and ``stop_times.parquet``
    under ``out_dir``. Route ids are derived from the direction/path-split trips.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    trips_df = dfs[key.TRIPS_KEY]
    stop_times_df = dfs[key.STOP_TIMES_KEY]
    stops_df = dfs[key.STOPS_KEY]

    stops_out = (
        stops_df.select("stop_id", "stop_lat", "stop_lon")
        .sort("stop_id")
        .with_row_index("stop_idx")
        .with_columns(pl.col("stop_idx").cast(pl.UInt32))
    )

    routes_idx = (
        trips_df.select("route_id")
        .unique(maintain_order=True)
        .sort("route_id")
        .with_row_index("route_idx")
        .with_columns(pl.col("route_idx").cast(pl.UInt32))
    )

    trips_idx = (
        trips_df.select("trip_id", "route_id")
        .join(routes_idx, on="route_id", how="inner")
        .sort("trip_id")
        .with_row_index("trip_idx")
        .with_columns(pl.col("trip_idx").cast(pl.UInt32))
    )

    stop_times_out = (
        stop_times_df.select(
            "trip_id", "stop_id", "stop_sequence", "arrival_secs", "departure_secs"
        )
        .join(trips_idx.select("trip_id", "trip_idx"), on="trip_id", how="inner")
        .join(stops_out.select("stop_id", "stop_idx"), on="stop_id", how="inner")
        .select("trip_idx", "stop_idx", "stop_sequence", "arrival_secs", "departure_secs")
        .sort(["trip_idx", "stop_sequence"])
    )

    stops_out.select("stop_idx", "stop_lat", "stop_lon").write_parquet(
        out_dir / "stops.parquet"
    )
    trips_idx.select("trip_idx", "route_idx").write_parquet(out_dir / "trips.parquet")
    stop_times_out.write_parquet(out_dir / "stop_times.parquet")
    rlog.info(
        f"GTFS: {len(stops_out)} stops, {len(trips_idx)} trips, {len(stop_times_out)} stop times"
    )
