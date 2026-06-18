"""Run the MCR5 core loop for a reduced set of origins, with full paths.

Same migration as ``10_mcr5.py`` (drives mcr-rust's parallel ``run_mcr5``),
but restricted to a handful of H3 start cells — useful for inspecting the
per-label traversal paths of a single origin without computing the whole city.

The old version passed ``disable_paths=False`` to the Python ``MCR5`` class.
The Rust pipeline always writes the traversal path for every surviving label
(``write_bags``), so "reduced paths" now just means "run over a reduced set of
start nodes": we filter the start-node parquet down to ``TARGET_H3_CELLS`` and
hand the filtered file to ``run_mcr5``.
"""

import functools
import json
import pathlib
import tomllib
import typing
import zoneinfo
from datetime import datetime

import polars as pl
from mcr_py.mcr5.mcr5 import (
    PriceFunction,
    PrivateModeConfig,
    PublicTransportConfig,
    SharedMicromobileConfig,
    WalkingConfig,
    build_config,
    build_graph,
    run_mcr5,
)
from mcr_py.utils.logger import rlog, setup

# H3 cells to run for. The single-cell default mirrors the original notebook.
TARGET_H3_CELLS = ["891fa199c77ffff"]

# --- Mode parameters --------------------------------------------------------
# Mirror the mcr-rust integration-test configs; tune per city / tariff.
WALKING_SPEED_KMH = 5
CYCLING = PrivateModeConfig(speed_kmh=15, switch_time_s=0, price_function_base=10)
CAR = PrivateModeConfig(speed_kmh=50, switch_time_s=0, price_function_base=20)

# The old "next_bike_no_tariff" scenario: nextbike shared bikes with no fare.
SHARED_BIKE_PRICE = PriceFunction(
    unlock_fee=0,
    interval_minutes=1,
    price_per_interval=0,
    first_interval_free=True,
)


def shared_bike_config() -> SharedMicromobileConfig:
    return SharedMicromobileConfig(
        speed_kmh=15, switch_time_s=30, price_function=SHARED_BIKE_PRICE
    )


def time_str_to_secs(time_str: str) -> int:
    """Convert a ``HH:MM:SS`` clock time into seconds since midnight."""
    hours, minutes, seconds = (int(part) for part in time_str.split(":"))
    return hours * 3600 + minutes * 60 + seconds


# --- Scenario builders ------------------------------------------------------
Scenario = dict[str, typing.Any]


def get_walking_scenario(paths: dict[str, str], **_: str) -> Scenario:
    return {
        "graph": build_graph(
            walking_nodes=paths["walking_nodes"],
            walking_edges=paths["walking_edges"],
            poi_nodes=paths["poi_nodes"],
        ),
        "config": {"walking": WalkingConfig(speed_kmh=WALKING_SPEED_KMH)},
        "start_nodes": paths["walking_start_nodes"],
    }


def get_car_scenario(paths: dict[str, str], **_: str) -> Scenario:
    return {
        "graph": build_graph(
            walking_nodes=paths["walking_nodes"],
            walking_edges=paths["walking_edges"],
            poi_nodes=paths["poi_nodes"],
            car_nodes=paths["car_nodes"],
            car_edges=paths["car_edges"],
        ),
        "config": {
            "walking": WalkingConfig(speed_kmh=WALKING_SPEED_KMH),
            "car": CAR,
        },
        "start_nodes": paths["car_start_nodes"],
    }


def get_bicycle_scenario(paths: dict[str, str], **_: str) -> Scenario:
    return {
        "graph": build_graph(
            walking_nodes=paths["walking_nodes"],
            walking_edges=paths["walking_edges"],
            poi_nodes=paths["poi_nodes"],
            cycling_nodes=paths["cycling_nodes"],
            cycling_edges=paths["cycling_edges"],
            shared_bike_stations=paths["shared_bike_stations"],
            shared_bike_dropoff_zones=paths["shared_bike_dropoff_zones"],
        ),
        "config": {
            "walking": WalkingConfig(speed_kmh=WALKING_SPEED_KMH),
            "shared_micromobile": [shared_bike_config()],
        },
        "start_nodes": paths["walking_start_nodes"],
    }


def get_public_transport_scenario(
    paths: dict[str, str], start_time: str, **_: str
) -> Scenario:
    return {
        "graph": build_graph(
            walking_nodes=paths["walking_nodes"],
            walking_edges=paths["walking_edges"],
            poi_nodes=paths["poi_nodes"],
        ),
        "config": {
            "walking": WalkingConfig(speed_kmh=WALKING_SPEED_KMH),
            "public_transport": PublicTransportConfig(
                data_dir=paths["gtfs_data_dir"],
                anchor_time_secs=time_str_to_secs(start_time),
            ),
        },
        "start_nodes": paths["walking_start_nodes"],
    }


def get_bicycle_public_transport_scenario(
    paths: dict[str, str], start_time: str, **_: str
) -> Scenario:
    return {
        "graph": build_graph(
            walking_nodes=paths["walking_nodes"],
            walking_edges=paths["walking_edges"],
            poi_nodes=paths["poi_nodes"],
            cycling_nodes=paths["cycling_nodes"],
            cycling_edges=paths["cycling_edges"],
            shared_bike_stations=paths["shared_bike_stations"],
            shared_bike_dropoff_zones=paths["shared_bike_dropoff_zones"],
        ),
        "config": {
            "walking": WalkingConfig(speed_kmh=WALKING_SPEED_KMH),
            "shared_micromobile": [shared_bike_config()],
            "public_transport": PublicTransportConfig(
                data_dir=paths["gtfs_data_dir"],
                anchor_time_secs=time_str_to_secs(start_time),
            ),
        },
        "start_nodes": paths["walking_start_nodes"],
    }


def build_paths(base_directory: pathlib.Path, city_name: str) -> dict[str, str]:
    """Resolve every parquet input `run_mcr5` needs, as string paths."""
    graph_dir = base_directory / "graph" / city_name
    start_nodes_dir = base_directory / "start_nodes" / city_name
    return {
        "walking_nodes": str(graph_dir / "walking_nodes.parquet"),
        "walking_edges": str(graph_dir / "walking_edges.parquet"),
        "cycling_nodes": str(graph_dir / "cycling_nodes.parquet"),
        "cycling_edges": str(graph_dir / "cycling_edges.parquet"),
        "car_nodes": str(graph_dir / "car_nodes.parquet"),
        "car_edges": str(graph_dir / "car_edges.parquet"),
        "poi_nodes": str(graph_dir / "poi_nodes.parquet"),
        "shared_bike_stations": str(graph_dir / "shared_bike_stations.parquet"),
        "shared_bike_dropoff_zones": str(graph_dir / "shared_bike_dropoff_zones.geojson"),
        "gtfs_data_dir": str(base_directory / "gtfs_clean" / city_name),
        "walking_start_nodes": str(start_nodes_dir / "walking.parquet"),
        "car_start_nodes": str(start_nodes_dir / "car.parquet"),
    }


def write_filtered_start_nodes(
    src: str, h3_cells: list[str], dst: pathlib.Path
) -> pathlib.Path:
    """Filter a start-node parquet to `h3_cells` and write it to `dst`."""
    df = pl.read_parquet(src).filter(pl.col("h3_cell_id").is_in(h3_cells))
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dst)
    rlog.info("Calculating for {} start nodes".format(len(df)))
    return dst


if __name__ == "__main__":
    city_name = "cologne"

    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)

    setup("DEBUG")
    data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
    base_directory = data_directory / settings["timestamp"]["timestamp"]
    mcr5_output_path = base_directory / f"mcr5_results/{city_name}_reduced_paths"
    mcr5_output_path.mkdir(parents=True, exist_ok=True)

    paths = build_paths(base_directory, city_name)

    scenarios: dict[str, typing.Callable[[], Scenario]] = {}
    enabled = settings["mcr5_types"]["mcr5_types"]
    if "public_transport" in enabled:
        for idx, time in enumerate(settings["public_transport"]["start_times"]):
            scenarios[f"public_transport_{idx}"] = functools.partial(
                get_public_transport_scenario, paths, start_time=time
            )
    if "walking" in enabled:
        scenarios["walking"] = functools.partial(get_walking_scenario, paths)
    if "bicycle" in enabled:
        scenarios["bicycle"] = functools.partial(get_bicycle_scenario, paths)
    if "car" in enabled:
        scenarios["car"] = functools.partial(get_car_scenario, paths)
    if "bicycle_public_transport" in enabled:
        for idx, time in enumerate(settings["public_transport"]["start_times"]):
            scenarios[f"bicycle_public_transport_{idx}"] = functools.partial(
                get_bicycle_public_transport_scenario, paths, start_time=time
            )

    runtimes = {}
    for key, scenario_fn in scenarios.items():
        start = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        rlog.info(f"Building graph and config for {key}")

        scenario = scenario_fn()
        graph = scenario["graph"]
        rlog.info("Graph has {} nodes".format(graph.node_count()))

        loaded_at = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        load_time = loaded_at - start

        output_path = mcr5_output_path / key
        config = build_config(out_dir=str(output_path), **scenario["config"])

        start_nodes = write_filtered_start_nodes(
            scenario["start_nodes"],
            TARGET_H3_CELLS,
            output_path / "_start_nodes.parquet",
        )

        rlog.info(f"Running MCR5 for {key} (parallel over start nodes)")
        run_mcr5(str(start_nodes), config, graph)

        run_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - loaded_at
        total_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - start
        runtimes[key] = {
            "load_time": str(load_time),
            "run_time": str(run_time),
            "total_time": str(total_time),
        }

    with open(mcr5_output_path / "runtimes.json", "w") as f:
        json.dump(runtimes, f)
