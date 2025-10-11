import datetime
import io
import json
import pathlib
import tomllib
import typing
import zoneinfo

import mcr_py
import mcr_py.gtfs.clean
import mcr_py.gtfs.crop
import mcr_py.mcr.data
import mcr_py.overpass.query
import mcr_py.structs.build
import mcr_py.utils.cache
import mcr_py.utils.geometa
import mcr_py.utils.key
import mcr_py.utils.logger
import mcr_py.utils.storage
import polars as pl
from fsspec.implementations.http import HTTPFileSystem


def fetch_gbfs_to_parquet(gbfs_url: str, target_path: pathlib.Path) -> None:
    fs = HTTPFileSystem()
    with fs.open(gbfs_url) as file:
        content = json.load(file)
        df = pl.read_json(io.StringIO(json.dumps(content["data"]["bikes"])))
    target_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(target_path)


def load_data_for_city(
    data_directory: pathlib.Path,
    city_name: str,
    city_name_german: str,
    city_name_german_alt: str,
    admin_level: int,
    crs_sink_name: str,
    gtfs_timestamp: str,
    gbfs_url: typing.Union[str, None] = None,
    timestamp: typing.Union[str, None] = None,
    now: typing.Union[str, None] = None,
) -> None:
    if timestamp is None:
        timestamp = datetime.datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")).strftime(
            "%Y%m%d"
        )
    if now is None:
        now = datetime.datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")).strftime(
            "%Y%m%d-%H%M%S"
        )
    start_time = "01.01.1970-00:00:00 +0200"
    end_time = "01.01.2050-00:00:00 +0200"
    crs = "EPSG:4326"
    cache_path = data_directory / f"{timestamp}/cache/"
    gtfs_path = data_directory / f"gtfs_raw/{gtfs_timestamp}/latest.zip"
    gtfs_crop_path = data_directory / f"{timestamp}/gtfs_clean/{city_name}.zip"
    gtfs_clean_dir = data_directory / f"{timestamp}/gtfs_clean/{city_name}/"
    gtfs_clean_struct = data_directory / f"{timestamp}/gtfs_clean/{city_name}/structs.pkl"
    gbfs_path = data_directory / f"{timestamp}/gbfs_raw/{city_name}_{now}.parquet"
    osm_path = data_directory / f"{timestamp}/osm_raw"
    geometa_path = data_directory / f"{timestamp}/cache/{city_name}_geometa.json"
    time_start_datetime = datetime.datetime.strptime(start_time, "%d.%m.%Y-%H:%M:%S %z")
    time_end_datetime = datetime.datetime.strptime(end_time, "%d.%m.%Y-%H:%M:%S %z")

    mcr_py.utils.logger.setup("INFO")

    mcr_py.utils.cache.overwrite_tempdir(cache_path)

    # Create the GeoMeta object
    boundary_polygon = mcr_py.overpass.query.fetch_boundary_polygon(
        city_name_german, admin_level
    )
    geometa = mcr_py.utils.geometa.GeoMeta.create(boundary_polygon, crs, crs_sink_name)
    geometa.save(geometa_path)

    # Crop the GTFS data to the timeframe and geometa boundary
    mcr_py.gtfs.crop.crop(
        gtfs_path,
        gtfs_crop_path,
        geometa,
        time_start=time_start_datetime,
        time_end=time_end_datetime,
    )
    # Clean the GTFS data
    with mcr_py.utils.logger.Timed.info("Cleaning GTFS data"):
        dfs_dict = mcr_py.gtfs.clean.clean(gtfs_crop_path)
        mcr_py.utils.logger.rlog.info("Writing cleaned GTFS data")
        mcr_py.utils.storage.write_dfs_dict(dfs_dict, gtfs_clean_dir)

    # Build the structs from the cleaned GTFS data
    trips_df = mcr_py.utils.storage.read_df(
        gtfs_clean_dir
        / mcr_py.utils.storage.get_df_filename_for_name(mcr_py.utils.key.TRIPS_KEY),
    )
    stop_times_df = mcr_py.utils.storage.read_df(
        gtfs_clean_dir
        / mcr_py.utils.storage.get_df_filename_for_name(mcr_py.utils.key.STOP_TIMES_KEY)
    )
    data = mcr_py.structs.build.build_structures(trips_df, stop_times_df)
    mcr_py.utils.storage.write_any_dict(data, gtfs_clean_struct)

    # Fetch the GBFS data
    if gbfs_url is not None:
        fetch_gbfs_to_parquet(gbfs_url, gbfs_path)

    # Load the OSM data
    _ = mcr_py.mcr.data.OSMData(
        geo_meta=geometa,
        city_id=city_name_german_alt,
        osm_path=osm_path,
        cache_path=cache_path,
        additional_network_types=[
            mcr_py.mcr.data.NetworkType.CYCLING,
            mcr_py.mcr.data.NetworkType.DRIVING,
        ],
        redownload=mcr_py.mcr.data.RedownloadMode.OVERWRITE_NO_REDOWNLOAD,
    )
    msg = f"Finished loading data for {city_name_german}"
    mcr_py.utils.logger.rlog.info(msg)


if __name__ == "__main__":
    self_path = pathlib.Path(__file__).parent.resolve()
    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)

    data_path = pathlib.Path(__file__).parent.parent.resolve() / "data"
    for city in settings["city"]:
        load_data_for_city(
            data_directory=data_path,
            city_name=city,
            city_name_german=settings["city"][city]["german"],
            city_name_german_alt=settings["city"][city]["german_alt"],
            timestamp=settings["timestamp"]["timestamp"],
            admin_level=settings["city"][city]["admin_level"],
            crs_sink_name=settings["geography"]["crs_sink_name"],
            gtfs_timestamp=settings["gtfs"]["timestamp"],
            gbfs_url=settings["city"][city].get("gbfs_url", None),
        )
