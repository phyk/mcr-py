"""Run the MCR5 core loop via the mcr-rust parallel `run_mcr5` orchestrator.

The previous version built `OSMData`/`GeoMeta` in Python, assembled
`initial_steps`/`repeating_steps` through `mcr_py.command.step_config`, and
drove a Python `multiprocessing` pool (`MCR5.run`). The Rust crate now owns
both the per-origin MCR pipeline and the parallel loop over start nodes, so
this notebook only has to:

1. build the routing graph from parquet layers (`build_graph`),
2. assemble a per-scenario `MCRConfig` (`build_config`),
3. hand a start-node parquet to `run_mcr5`, which fans out across origins.

Notes on the API change:
- There is no `max_transfers` any more — the Rust loop runs to convergence.
- The public-transport start time is no longer a `run()` argument; it is the
  `anchor_time_secs` of `PublicTransportConfig`.
- `run_mcr5` writes one parquet per origin (named by its H3 cell) into
  `out_dir`, which is exactly what `20_mcr5_results_calculation.py` reads back.

Expected input parquet schemas (produced by the data pipeline):
- graph node/edge layers: as consumed by `mcr-rust` (`MCRGraphBuilder`).
- start nodes: columns `osm_node_id` (u64), `h3_cell_id` (str).
"""

import functools
import json
import pathlib
import tomllib
import typing
import zoneinfo
from datetime import datetime

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
# Each returns the routing graph, the kwargs for `build_config` (minus
# `out_dir`), and the start-node parquet to fan out over.
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


if __name__ == "__main__":
    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)

    setup(settings["run_type"]["run_type"])
    city_name = "cologne"
    data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
    base_directory = data_directory / settings["timestamp"]["timestamp"]
    mcr5_output_path = base_directory / f"mcr5_results/{city_name}"
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

        rlog.info(f"Running MCR5 for {key} (parallel over start nodes)")
        run_mcr5(scenario["start_nodes"], config, graph)

        run_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - loaded_at
        total_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - start
        runtimes[key] = {
            "load_time": str(load_time),
            "run_time": str(run_time),
            "total_time": str(total_time),
        }

    with open(mcr5_output_path / "runtimes.json", "w") as f:
        json.dump(runtimes, f)
