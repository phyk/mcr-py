import itertools
import os
import pickle
import sys
from datetime import datetime
from functools import partial

from guppy import hpy

heap = hpy()

sys.path.append("../src/")
from command.step_config import (
    get_bicycle_only_config,
    get_bicycle_public_transport_config,
    get_car_only_config,
    get_public_transport_only_config,
    get_walking_only_config,
)
from package import storage
from package.geometa import GeoMeta
from package.logger import rlog, setup
from package.mcr5.mcr5 import MCR5

setup("INFO")
heap_status1 = heap.heap()
rlog.info("Heap Size : %s bytes", str(heap_status1.size))

city_id = "cologne"  # 'Koeln'
city_id_osm = "koeln"
date = "20240427"
geo_meta_path = f"../data/stateful_variables/{city_id.lower()}_geometa.pkl"
stops = f"../data/gtfs-cleaned/{city_id.lower()}_{date}/stops.csv"
structs = f"../data/gtfs-cleaned/{city_id.lower()}_{date}/structs.pkl"
location_mapping_path = f"../data/city_data/location_mappings_{city_id.lower()}.pkl"
bicycle_base_path = f"../data/sharing_locations_clustered/{city_id.lower()}_bikes/"
mcr5_output_path = f"../data/mcr5/{city_id.lower()}_{date}"

geo_meta = GeoMeta.load(geo_meta_path)

lm_data = storage.read_any_dict(location_mapping_path)
location_mappings = lm_data["location_mappings"]
car_location_mappings = lm_data["car_location_mappings"]

configs = {}


def get_bicyle_public_transport_config_ready(bicycle_location_path, start_time):
    initial_steps, repeating_steps = get_bicycle_public_transport_config(
        geo_meta_path=geo_meta_path,
        city_id=city_id_osm,
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
    initial_steps, repeating_steps = get_car_only_config(
        geo_meta_path=geo_meta_path,
        city_id=city_id_osm,
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
    initial_steps, repeating_steps = get_bicycle_only_config(
        geo_meta_path=geo_meta_path,
        city_id=city_id_osm,
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
    initial_steps, repeating_steps = get_public_transport_only_config(
        geo_meta_path=geo_meta_path,
        city_id=city_id_osm,
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
    initial_steps, repeating_steps = get_walking_only_config(
        geo_meta_path=geo_meta_path,
        city_id=city_id_osm,
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

heap_status2 = heap.heap()
rlog.info("Heap Size : %s bytes", str(heap_status2.size))

runtimes = {}
for key, config in configs.items():
    start = datetime.now()
    rlog.info(f"Running MCR5 for {key}")

    config = config()
    mcr5 = MCR5(**config["init_kwargs"], max_processes=8)

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
    heap_status3 = heap.heap()
    rlog.info("Heap Size : %s bytes", str(heap_status3.size))
    rlog.info(
        "Memory Usage after a single run %s bytes",
        str(heap_status3.size - heap_status2.size),
    )
    rlog.info(heap_status3)

with open(os.path.join(mcr5_output_path, "runtimes.pkl"), "wb") as f:
    pickle.dump(runtimes, f)
