import os
import pathlib
import pickle
from datetime import datetime

from mcr_py.command.step_config import (
    get_walking_only_config_with_data,
)
from mcr_py.mcr.data import NetworkType, OSMData, RedownloadMode
from mcr_py.mcr5.mcr5 import MCR5
from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import rlog, setup

setup("DEBUG")

data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
city_name = "cologne"
city_name_german = "Köln"
city_name_german_alt = "Koeln"


crs_sink_name = "EPSG:4839"

timestamp = "20250718"
now = "20250718-114512"

base_directory = f"{data_directory}/{timestamp}"

cache_path = f"{base_directory}/cache/"
gtfs_path = f"{base_directory}/gtfs_raw/{city_name}.zip"
gtfs_crop_path = f"{base_directory}/gtfs_clean/{city_name}.zip"
gtfs_clean_dir = f"{base_directory}/gtfs_clean/{city_name}/"
gtfs_clean_struct = f"{gtfs_clean_dir}/structs.pkl"
gtfs_clean_stops = f"{gtfs_clean_dir}/stops.csv"

gbfs_path = f"{base_directory}/gbfs_raw/{city_name}_{now}.csv"
osm_path = f"{base_directory}/osm_raw"
geometa_path = f"{base_directory}/cache/{city_name}_geometa.pkl"

mcr5_output_path = f"{base_directory}/mcr5_results/{city_name}"
# bicycle_base_path = f"../data/sharing_locations_clustered/{city_name.lower()}_bikes/"


def load_auxiliary_classes(geo_meta_path: str, city_id: str, osm_path: str, cache_path: str):
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(
        geo_meta,
        city_id,
        cache_path=cache_path,
        osm_path=osm_path,
        redownload=RedownloadMode.REUSE,
        additional_network_types=[NetworkType.CYCLING, NetworkType.DRIVING],
    )
    return geo_meta, geo_data


geo_meta, geo_data = load_auxiliary_classes(
    geo_meta_path=geometa_path,
    city_id=city_name_german_alt,
    osm_path=osm_path,
    cache_path=cache_path,
)

configs = {}

# def get_bicyle_public_transport_config_ready(bicycle_location_path, start_time):
#     initial_steps, repeating_steps = get_bicycle_public_transport_config(
#         geo_meta_path=geometa_path,
#         city_id=city_id_osm,
#         bicycle_price_function="next_bike_no_tariff",
#         bicycle_location_path=bicycle_location_path,
#         structs_path=structs,
#         stops_path=stops,
#     )
#     return {
#         "init_kwargs": {
#             "initial_steps": initial_steps,
#             "repeating_steps": repeating_steps,
#         },
#         "location_mappings": location_mappings,
#         "max_transfers": 2,
#         "start_time": start_time,
#     }


# def get_car_only_config_ready():
#     initial_steps, repeating_steps = get_car_only_config(
#         geo_meta_path=geo_meta_path,
#         city_id=city_id_osm,
#     )
#     return {
#         "init_kwargs": {
#             "initial_steps": initial_steps,
#             "repeating_steps": repeating_steps,
#         },
#         "location_mappings": car_location_mappings,
#         "max_transfers": 1,
#     }


# def get_bicycle_only_config_ready(bicycle_location_path):
#     initial_steps, repeating_steps = get_bicycle_only_config(
#         geo_meta_path=geo_meta_path,
#         city_id=city_id_osm,
#         bicycle_price_function="next_bike_no_tariff",
#         bicycle_location_path=bicycle_location_path,
#     )
#     return {
#         "init_kwargs": {
#             "initial_steps": initial_steps,
#             "repeating_steps": repeating_steps,
#         },
#         "location_mappings": location_mappings,
#         "max_transfers": 2,
#     }


# def get_public_transport_only_config_ready(start_time):
#     initial_steps, repeating_steps = get_public_transport_only_config(
#         geo_meta_path=geo_meta_path,
#         city_id=city_id_osm,
#         structs_path=structs,
#         stops_path=stops,
#     )
#     return {
#         "init_kwargs": {
#             "initial_steps": initial_steps,
#             "repeating_steps": repeating_steps,
#         },
#         "location_mappings": location_mappings,
#         "max_transfers": 2,
#         "start_time": start_time,
#     }


def get_walking_only_config_ready(geo_data: OSMData):
    initial_steps, repeating_steps = get_walking_only_config_with_data(geo_data)
    rlog.info("Walking step configured")
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": geo_data.location_mapping,
        "max_transfers": 0,
    }


configs["walking"] = get_walking_only_config_ready

runtimes = {}
for key, config in configs.items():
    start = datetime.now()
    rlog.info(f"Running MCR5 for {key}")

    config = config(geo_data)
    mcr5 = MCR5(**config["init_kwargs"])

    loaded_at = datetime.now()
    load_time = loaded_at - start

    output_path = os.path.join(mcr5_output_path, key)

    location_mappings = config["location_mappings"]

    rlog.info("Calculating for {} hexes".format(len(location_mappings)))

    start_time = config.get("start_time", "08:00:00")
    rlog.debug("Running MCR5")
    errors = mcr5.run(
        location_mappings,
        start_time=start_time,
        output_dir=output_path,
        max_transfers=config["max_transfers"],
        verbose=True,
    )
    rlog.info("Found {} errors".format(len(errors)))

    run_time = datetime.now() - loaded_at
    total_time = datetime.now() - start
    runtimes[key] = {
        "load_time": load_time,
        "run_time": run_time,
        "total_time": total_time,
    }

with open(os.path.join(mcr5_output_path, "runtimes.pkl"), "wb") as f:
    pickle.dump(runtimes, f)

print("Ready")
