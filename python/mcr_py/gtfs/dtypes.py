from mcr_py.utils import key
import polars as pl

GTFS_DTYPES = {
    key.TRIP_ID_KEY: pl.String,
    key.STOP_ID_KEY: pl.String,
    key.ROUTE_ID_KEY: pl.String,
    key.SERVICE_ID_KEY: pl.String,
    key.STOP_TIME_ARRIVAL_TIME_KEY: pl.String,
    key.STOP_TIME_DEPARTURE_TIME_KEY: pl.String,
    key.STOP_SEQUENCE_KEY: pl.Int64,
    key.STOP_HEADSIGN_KEY: pl.String,
    key.STOP_LAT_KEY: pl.Float64,
    key.STOP_LON_KEY: pl.Float64,
}
