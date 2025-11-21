import functools
import json
import pathlib
import tomllib
import typing
import zoneinfo
from datetime import datetime

import mcr_py.command.step_config
import mcr_py.helper_functions
import polars as pl
from mcr_py.mcr.data import NetworkType, OSMData
from mcr_py.mcr5.mcr5 import MCR5
from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import rlog, setup


def get_bicycle_public_transport_config_ready(
    geo_data: OSMData,
    geo_meta: GeoMeta,
    bicycle_location_path: pathlib.Path,
    structs: pathlib.Path,
    stops: pathlib.Path,
    start_time: str,
) -> dict[str, typing.Any]:
    initial_steps, repeating_steps = (
        mcr_py.command.step_config.get_bicycle_public_transport_config_with_data(
            geo_data=geo_data,
            geo_meta=geo_meta,
            bicycle_price_function="next_bike_no_tariff",
            bicycle_location_path=bicycle_location_path,
            structs_path=structs,
            stops_path=stops,
        )
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": geo_data.location_mapping[NetworkType.WALKING],
        "max_transfers": 5,
        "start_time": start_time,
    }


def get_car_only_config_ready(geo_data: OSMData) -> dict[str, typing.Any]:
    initial_steps, repeating_steps = mcr_py.command.step_config.get_car_only_config_with_data(
        geo_data=geo_data
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": geo_data.location_mapping[NetworkType.DRIVING],
        "max_transfers": 1,
    }


def get_bicycle_only_config_ready(
    geo_data: OSMData, geo_meta: GeoMeta, bicycle_location_path: pathlib.Path
) -> dict[str, typing.Any]:
    initial_steps, repeating_steps = (
        mcr_py.command.step_config.get_bicycle_only_config_with_data(
            geo_data=geo_data,
            geo_meta=geo_meta,
            bicycle_price_function="next_bike_no_tariff",
            bicycle_location_path=bicycle_location_path,
        )
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": geo_data.location_mapping[NetworkType.WALKING],
        "max_transfers": 5,
    }


def get_public_transport_only_config_ready(
    geo_data: OSMData,
    start_time: str,
    structs_path: pathlib.Path,
    stops_path: pathlib.Path,
    **_: str,
) -> dict[str, typing.Any]:
    initial_steps, repeating_steps = (
        mcr_py.command.step_config.get_public_transport_only_config_with_data(
            geo_data=geo_data,
            structs_path=structs_path,
            stops_path=stops_path,
        )
    )
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": geo_data.location_mapping[NetworkType.WALKING],
        "max_transfers": 5,
        "start_time": start_time,
    }


def get_walking_only_config_ready(geo_data: OSMData, **_: str) -> dict[str, typing.Any]:
    initial_steps, repeating_steps = (
        mcr_py.command.step_config.get_walking_only_config_with_data(geo_data)
    )
    rlog.info("Walking step configured")
    return {
        "init_kwargs": {
            "initial_steps": initial_steps,
            "repeating_steps": repeating_steps,
        },
        "location_mappings": geo_data.location_mapping[NetworkType.WALKING],
        "max_transfers": 0,
    }


if __name__ == "__main__":
    city_name = "cologne"

    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)

    setup("DEBUG")
    data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
    base_directory = data_directory / settings["timestamp"]["timestamp"]
    cache_path = base_directory / "cache/"
    gtfs_clean_dir = base_directory / f"gtfs_clean/{city_name}/"
    gtfs_clean_struct = gtfs_clean_dir / "structs.pkl"
    gtfs_clean_stops = gtfs_clean_dir / "stops.parquet"

    gbfs_path = base_directory / f"gbfs_raw/{city_name}_{settings['timestamp']['now']}.parquet"
    osm_path = base_directory / "osm_raw"
    geometa_path = base_directory / f"cache/{city_name}_geometa.json"

    mcr5_output_path = base_directory / f"mcr5_results/{city_name}_reduced_paths"
    bicycle_base_path = f"../data/sharing_locations_clustered/{city_name.lower()}_bikes/"

    geo_meta, geo_data = mcr_py.helper_functions.load_auxiliary_classes(
        geo_meta_path=geometa_path,
        city_id=settings["city"][city_name]["german_alt"],
        osm_path=osm_path,
        cache_path=cache_path,
    )

    configs = {}
    if "public_transport" in settings["mcr5_types"]["mcr5_types"]:
        for idx, time in enumerate(settings["public_transport"]["start_times"]):
            configs[f"public_transport_{idx}"] = functools.partial(
                get_public_transport_only_config_ready,
                start_time=time,
                structs_path=gtfs_clean_struct,
                stops_path=gtfs_clean_stops,
            )
    if "walking" in settings["mcr5_types"]["mcr5_types"]:
        configs["walking"] = functools.partial(
            get_walking_only_config_ready, start_time="08:00:00"
        )
    if "bicycle" in settings["mcr5_types"]["mcr5_types"]:
        configs["bicycle"] = functools.partial(
            get_bicycle_only_config_ready,
            geo_meta=geo_meta,
            bicycle_location_path=gbfs_path,
        )
    if "car" in settings["mcr5_types"]["mcr5_types"]:
        configs["car"] = get_car_only_config_ready
    if "bicycle_public_transport" in settings["mcr5_types"]["mcr5_types"]:
        for idx, time in enumerate(settings["public_transport"]["start_times"]):
            configs[f"bicycle_public_transport_{idx}"] = functools.partial(
                get_bicycle_public_transport_config_ready,
                geo_meta=geo_meta,
                bicycle_location_path=gbfs_path,
                structs=gtfs_clean_struct,
                stops=gtfs_clean_stops,
                start_time=time,
            )

    runtimes = {}
    for key, config in configs.items():
        start = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        rlog.info(f"Running MCR5 for {key}")

        config = config(geo_data=geo_data)
        mcr5 = MCR5(**config["init_kwargs"], disable_paths=False)

        loaded_at = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        load_time = loaded_at - start

        output_path = mcr5_output_path / key
        output_path.mkdir(parents=True, exist_ok=True)

        location_mappings: pl.DataFrame = config["location_mappings"].filter(
            pl.col("h3_cell").is_in(["891fa199c77ffff"])
        )

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

        run_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - loaded_at
        total_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - start
        runtimes[key] = {
            "load_time": str(load_time),
            "run_time": str(run_time),
            "total_time": str(total_time),
        }

    with open(mcr5_output_path / "runtimes.json", "w") as f:
        json.dump(runtimes, f)
