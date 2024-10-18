import itertools
import os
import pickle
import sys
import tracemalloc
from datetime import datetime
from functools import partial

import psutil

sys.path.append("../src/")
from command.step_config import (
    get_bicycle_only_config_with_data,
    get_bicycle_public_transport_config_with_data,
    get_car_only_config_with_data,
    get_public_transport_only_config_with_data,
    get_walking_only_config_with_data,
)
from mcr_py.package import key, storage
from mcr_py.package.geometa import GeoMeta
from mcr_py.package.logger import rlog, setup
from mcr_py.package.mcr.data import NetworkType, OSMData
from mcr_py.package.mcr5.mcr5 import MCR5

setup("INFO")


def pretty_bytes(b: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f}EiB"


key.TMP_DIR_LOCATION = "../tmp"

city_id = "cologne"  # 'Koeln'
city_id_osm = "Koeln"
date = "20240530"
geo_meta_path = f"../data/stateful_variables/{city_id.lower()}_geometa.pkl"
stops = f"../data/gtfs-cleaned/{city_id.lower()}_{date}/stops.csv"
structs = f"../data/gtfs-cleaned/{city_id.lower()}_{date}/structs.pkl"
location_mapping_path = f"../data/city_data/location_mappings_{city_id.lower()}.pkl"
bicycle_base_path = f"../data/sharing_locations_clustered/{city_id.lower()}_nextbike/"
mcr5_output_path = f"../data/mcr5/{city_id.lower()}_{date}"

geo_meta = GeoMeta.load(geo_meta_path)
geo_data = OSMData(
    geo_meta,
    city_id,
    additional_network_types=[NetworkType.DRIVING, NetworkType.CYCLING],
)

lm_data = storage.read_any_dict(location_mapping_path)
location_mappings = lm_data["location_mappings"]
car_location_mappings = lm_data["car_location_mappings"]

configs = {}


def get_bicyle_public_transport_config_ready(bicycle_location_path, start_time):
    initial_steps, repeating_steps = get_bicycle_public_transport_config_with_data(
        geo_meta=geo_meta,
        geo_data=geo_data,
        bicycle_price_function="next_bike_no_tariff",
        bicycle_location_path=bicycle_location_path,
        structs_path=structs,
        stops_path=stops,
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": location_mappings,
        "max_transfers": 2,
        "start_time": start_time,
    }


def get_car_only_config_ready():
    initial_steps, repeating_steps = get_car_only_config_with_data(
        geo_meta=geo_meta,
        geo_data=geo_data,
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": car_location_mappings,
        "max_transfers": 1,
    }


def get_bicycle_only_config_ready(bicycle_location_path):
    initial_steps, repeating_steps = get_bicycle_only_config_with_data(
        geo_meta=geo_meta,
        geo_data=geo_data,
        bicycle_price_function="next_bike_no_tariff",
        bicycle_location_path=bicycle_location_path,
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": location_mappings,
        "max_transfers": 2,
    }


def get_public_transport_only_config_ready(start_time):
    initial_steps, repeating_steps = get_public_transport_only_config_with_data(
        geo_meta=geo_meta,
        geo_data=geo_data,
        structs_path=structs,
        stops_path=stops,
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": location_mappings,
        "max_transfers": 2,
        "start_time": start_time,
    }


def get_walking_only_config_ready():
    initial_steps, repeating_steps = get_walking_only_config_with_data(
        geo_meta=geo_meta,
        geo_data=geo_data,
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": location_mappings,
        "max_transfers": 0,
    }


times = [
    "08:00:00",
    "12:00:00",
    "18:00:00",
]
bicycle_location_paths = [
    os.path.join(bicycle_base_path, path) for path in os.listdir(bicycle_base_path)
]

bicycle_public_transport_config_args = list(
    itertools.product(
        times,
        bicycle_location_paths,
    )
)


for i, (time, bicycle_location_path) in enumerate(bicycle_public_transport_config_args):
    configs[f"bicycle_public_transport_{i}"] = partial(
        get_bicyle_public_transport_config_ready, bicycle_location_path, time
    )

for i, time in enumerate(times):
    print(i, time)
    configs[f"public_transport_{i}"] = partial(
        get_public_transport_only_config_ready, time
    )

for i, bicycle_location_path in enumerate(bicycle_location_paths):
    print(i, bicycle_location_path)
    configs[f"bicycle_{i}"] = partial(
        get_bicycle_only_config_ready, bicycle_location_path
    )

configs["car"] = get_car_only_config_ready
configs["walking"] = get_walking_only_config_ready

if os.path.exists(mcr5_output_path):
    raise Exception("Output path already exists")

pre_loop_memory = psutil.Process().memory_info().vms
rlog.info("Pre Loop Memory : %s", pretty_bytes(pre_loop_memory))
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()

runtimes = {}
for key, config in configs.items():
    start = datetime.now()
    rlog.info(f"Running MCR5 for {key}")

    config = config()
    snapshot1 = tracemalloc.take_snapshot()
    mcr5 = MCR5(**config["init_kwargs"], max_processes=8)

    snapshot2 = tracemalloc.take_snapshot()
    top_stats = snapshot2.compare_to(snapshot1, "lineno")
    print("[ Top 10 differences ]")
    for stat in top_stats[:10]:
        print(stat)
    snapshot1 = snapshot2

    memory_pre_loop = psutil.Process().memory_info().vms
    current, peak = tracemalloc.get_traced_memory()
    rlog.info(
        "Memory difference after mcr5 load: %s - %s",
        pretty_bytes(memory_pre_loop - pre_loop_memory),
        pretty_bytes(current),
    )

    loaded_at = datetime.now()
    load_time = loaded_at - start

    output_path = os.path.join(mcr5_output_path, key)

    location_mappings = config["location_mappings"]

    start_time = config.get("start_time", "08:00:00")

    errors = mcr5.run(
        location_mappings,
        start_time=start_time,
        output_dir=output_path,
        max_transfers=config["max_transfers"],
    )

    run_time = datetime.now() - loaded_at
    total_time = datetime.now() - start
    runtimes[key] = {
        "load_time": load_time,
        "run_time": run_time,
        "total_time": total_time,
    }
    memory_in_loop = psutil.Process().memory_info().vms
    rlog.info("Memory in Loop : %s", pretty_bytes(memory_in_loop))
    rlog.info(
        "Memory difference after a single run %s",
        pretty_bytes(memory_in_loop - pre_loop_memory),
    )
    pre_loop_memory = memory_in_loop

with open(os.path.join(mcr5_output_path, "runtimes.pkl"), "wb") as f:
    pickle.dump(runtimes, f)
