import datetime
import io
import json
import os
import pathlib

import mcr_py
import mcr_py.command.area
import mcr_py.command.build
import mcr_py.command.gtfs.gtfs
import mcr_py.mcr.data
import mcr_py.utils.cache
import mcr_py.utils.geometa
import mcr_py.utils.logger
import polars as pl
from fsspec.implementations.http import HTTPFileSystem


def fetch_gbfs_to_csv(gbfs_url, target_path):
    fs = HTTPFileSystem()
    with fs.open(gbfs_url) as file:
        content = json.load(file)
        df = pl.read_json(io.StringIO(json.dumps(content["data"]["bikes"])))
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    df.write_csv(target_path)


def load_data_for_city(
    data_directory,
    city_name,
    city_name_german,
    city_name_german_alt,
    admin_level,
    gbfs_url=None,
):
    # timestamp = datetime.datetime.today().strftime("%Y%m%d")
    timestamp = "20250718"
    now = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    start_time = "01.01.1970-00:00:00"
    end_time = "01.01.2050-00:00:00"
    crs_sink_name = "EPSG:4839"
    gtfs_timestamp = "20250718"
    cache_path = f"{data_directory}/{timestamp}/cache/"
    gtfs_path = f"{data_directory}/gtfs_raw/{gtfs_timestamp}/latest.zip"
    gtfs_crop_path = f"{data_directory}/{timestamp}/gtfs_clean/{city_name}.zip"
    gtfs_clean_dir = f"{data_directory}/{timestamp}/gtfs_clean/{city_name}/"
    gtfs_clean_struct = f"{data_directory}/{timestamp}/gtfs_clean/{city_name}/structs.pkl"
    gbfs_path = f"{data_directory}/{timestamp}/gbfs_raw/{city_name}_{now}.csv"
    osm_path = f"{data_directory}/{timestamp}/osm_raw"
    geometa_path = f"{data_directory}/{timestamp}/cache/{city_name}_geometa.pkl"
    mcr_py.utils.logger.setup("INFO")

    mcr_py.utils.cache.overwrite_tempdir(cache_path)
    mcr_py.command.area.create_area(
        city_name_german, admin_level, crs_sink_name, geometa_path=geometa_path
    )
    mcr_py.command.gtfs.gtfs.crop_command(
        gtfs_path, gtfs_crop_path, start_time, end_time, geometa_path=geometa_path
    )
    mcr_py.command.gtfs.gtfs.clean_gtfs(gtfs_crop_path, gtfs_clean_dir)
    mcr_py.command.build.build_structures(gtfs_clean_dir, gtfs_clean_struct)
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
        redownload=mcr_py.mcr.data.RedownloadMode.OVERWRITE_NOREDOWNLOAD,
    )
    print("Finished")


data_path = pathlib.Path(__file__).parent.parent.resolve() / "data"

load_data_for_city(
    data_path,
    "cologne",
    "Köln",
    "Koeln",
    6,
    "https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_kg/de/free_bike_status.json",
)

load_data_for_city(data_path, "berlin", "Berlin", "Berlin", 4)
