import os
import pathlib
from datetime import datetime
from multiprocessing import Queue

import polars as pl
from mcr_py.command.step_config import (
    get_walking_only_config,
)
from mcr_py.mcr5.mcr5 import MCR5
from mcr_py.utils.logger import rlog, setup

setup("DEBUG")

data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
city_name = "cologne"
city_name_german = "Köln"
city_name_german_alt = "Koeln"

h3_cell_to_debug = "881fa18883fffff"
osm_node_id = 1611111990

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

walking_location_mapping = (
    f"{cache_path}/{city_name_german_alt.lower()}_walking_h3mapping.parquet"
)
# bicycle_base_path = f"../data/sharing_locations_clustered/{city_name.lower()}_bikes/"
location_mappings = pl.read_parquet(walking_location_mapping)


configs = {}


def get_walking_only_config_ready():
    initial_steps, repeating_steps = get_walking_only_config(
        geo_meta_path=geometa_path,
        city_id=city_name_german_alt,
        osm_path=osm_path,
        cache_path=cache_path,
    )
    rlog.info("Walking step configured")
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": location_mappings,
        "max_transfers": 0,
    }


configs["walking"] = get_walking_only_config_ready

runtimes = {}
for key, config in configs.items():
    start = datetime.now()
    rlog.info(f"Running MCR5 for {key}")

    config = config()
    mcr5 = MCR5(**config["init_kwargs"])

    loaded_at = datetime.now()
    load_time = loaded_at - start

    output_path = os.path.join("", key)

    location_mappings = config["location_mappings"]

    rlog.info("Calculating for {} hexes".format(len(location_mappings)))

    start_time = config.get("start_time", "08:00:00")
    rlog.debug("Running MCR5")
    mcr5.run_mcr(
        errors=Queue(maxsize=-1),
        h3_cell=h3_cell_to_debug,
        osm_node_id=osm_node_id,
        initial_steps=mcr5.initial_steps,
        repeating_steps=mcr5.repeating_steps,
        start_time=start_time,
        max_transfers=2,
        output_dir=output_path,
    )

print("Ready")
