import datetime
import io
import json
import pathlib
import typing
import zoneinfo

import mcr_py
import mcr_py.command.area
import mcr_py.command.build
import mcr_py.command.gtfs.gtfs
import mcr_py.mcr.data
import mcr_py.utils.cache
import mcr_py.utils.geometa
import mcr_py.utils.logger
import polars as pl
import tomllib
from fsspec.implementations.http import HTTPFileSystem


def fetch_gbfs_to_csv(gbfs_url: str, target_path: pathlib.Path) -> None:
    fs = HTTPFileSystem()
    with fs.open(gbfs_url) as file:
        content = json.load(file)
        df = pl.read_json(io.StringIO(json.dumps(content["data"]["bikes"])))
    target_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(target_path)


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
    start_time = "01.01.1970-00:00:00"
    end_time = "01.01.2050-00:00:00"
    cache_path = data_directory / f"{timestamp}/cache/"
    gtfs_path = data_directory / f"gtfs_raw/{gtfs_timestamp}/latest.zip"
    gtfs_crop_path = data_directory / f"{timestamp}/gtfs_clean/{city_name}.zip"
    gtfs_clean_dir = data_directory / f"{timestamp}/gtfs_clean/{city_name}/"
    gtfs_clean_struct = data_directory / f"{timestamp}/gtfs_clean/{city_name}/structs.pkl"
    gbfs_path = data_directory / f"{timestamp}/gbfs_raw/{city_name}_{now}.csv"
    osm_path = data_directory / f"{timestamp}/osm_raw"
    geometa_path = data_directory / f"{timestamp}/cache/{city_name}_geometa.pkl"
    mcr_py.utils.logger.setup("INFO")

    mcr_py.utils.cache.overwrite_tempdir(cache_path)
    mcr_py.command.area.create_area(
        city_name_german, admin_level, crs_sink_name, geometa_path=str(geometa_path)
    )
    mcr_py.command.gtfs.gtfs.crop_command(
        str(gtfs_path),
        str(gtfs_crop_path),
        start_time,
        end_time,
        geometa_path=geometa_path,
    )
    mcr_py.command.gtfs.gtfs.clean_gtfs(str(gtfs_crop_path), str(gtfs_clean_dir))
    mcr_py.command.build.build_structures(str(gtfs_clean_dir), str(gtfs_clean_struct))
    if gbfs_url is not None:
        fetch_gbfs_to_csv(gbfs_url, gbfs_path)

    geo_meta = mcr_py.utils.geometa.GeoMeta.load(geometa_path)

    _ = mcr_py.mcr.data.OSMData(
        geo_meta=geo_meta,
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
            admin_level=settings["city"][city]["admin_level"],
            crs_sink_name=settings["geography"]["crs_sink_name"],
            gtfs_timestamp=settings["gtfs"]["timestamp"],
            gbfs_url=settings["city"][city].get("gbfs_url", None),
        )
