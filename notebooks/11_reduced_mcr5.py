import json
import pathlib
import typing
import zoneinfo
from datetime import datetime

import mcr_py.helper_functions
import polars as pl
import tomllib
from mcr_py.command.step_config import (
    get_walking_only_config_with_data,
)
from mcr_py.mcr.data import OSMData
from mcr_py.mcr5.mcr5 import MCR5
from mcr_py.utils.logger import rlog, setup


def get_walking_only_config_ready(geo_data: OSMData) -> dict[str, typing.Any]:
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


if __name__ == "__main__":
    city_name = "cologne"
    first_n_rows = 20

    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)

    setup(settings["run_type"]["run_type"])
    data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
    base_directory = data_directory / settings["timestamp"]["timestamp"]
    cache_path = base_directory / "cache/"
    gtfs_clean_dir = base_directory / f"/gtfs_clean/{city_name}/"
    gtfs_clean_struct = gtfs_clean_dir / "structs.pkl"
    gtfs_clean_stops = gtfs_clean_dir / "stops.csv"

    gbfs_path = base_directory / f"gbfs_raw/{city_name}_{settings['timestamp']['now']}.csv"
    osm_path = base_directory / "osm_raw"
    geometa_path = base_directory / f"cache/{city_name}_geometa.pkl"

    mcr5_output_path = base_directory / f"mcr5_results/{city_name}_{first_n_rows}_rows"
    bicycle_base_path = f"../data/sharing_locations_clustered/{city_name.lower()}_bikes/"

    geo_meta, geo_data = mcr_py.helper_functions.load_auxiliary_classes(
        geo_meta_path=geometa_path,
        city_id=settings["city"][city_name]["german_alt"],
        osm_path=osm_path,
        cache_path=cache_path,
    )

    configs = {}
    configs["walking"] = get_walking_only_config_ready
    runtimes = {}
    for key, config in configs.items():
        start = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        rlog.info(f"Running MCR5 for {key}")

        config = config(geo_data)
        mcr5 = MCR5(**config["init_kwargs"])

        loaded_at = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        load_time = loaded_at - start

        output_path = mcr5_output_path / key
        output_path.mkdir(parents=True, exist_ok=True)

        location_mappings: pl.DataFrame = config["location_mappings"]
        location_mappings = location_mappings.head(first_n_rows)

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
