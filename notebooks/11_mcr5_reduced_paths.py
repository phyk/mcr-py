"""Run the MCR5 core loop for a reduced set of origins, with full paths.

Same migration as ``10_mcr5.py`` (drives mcr-rust's parallel ``run_mcr5``,
configured entirely from `config.toml`'s `[modes.*]`/`[mcr5] scenarios`
settings via `mcr_py.mcr5.scenarios`), but restricted to a handful of H3
start cells — useful for inspecting the per-label traversal paths of a few
origins without computing the whole city.

The old version passed ``disable_paths=False`` to the Python ``MCR5`` class.
The Rust pipeline always writes the traversal path for every surviving label
(``write_bags``), so "reduced paths" now just means "run over a reduced set of
start nodes": we filter the start-node parquet down to ``TARGET_H3_CELLS`` and
hand the filtered file to ``run_mcr5``.
"""

import json
import pathlib
import tomllib
import zoneinfo
from datetime import datetime

import polars as pl
from mcr_py.mcr5.mcr5 import build_config, build_graph, run_mcr5
from mcr_py.mcr5.scenarios import build_paths, build_scenarios
from mcr_py.utils.logger import rlog, setup

# H3 cells to run for. 10 cells sampled at random (seed=42) from cologne's
# walking start-node parquet (`start_nodes/cologne/walking.parquet`).
TARGET_H3_CELLS = [
    "891fa18aba7ffff",
    "891fa19a9dbffff",
    "891fa1894d7ffff",
    "891fa19a1a7ffff",
    "891fa18b22fffff",
    "891fa188213ffff",
    "891fa188283ffff",
    "891fa565b5bffff",
    "891fa19809bffff",
    "891fa188113ffff",
]


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
    mode_settings = settings["modes"]
    combos = settings["mcr5"]["scenarios"]

    runtimes = {}
    for combo in combos:
        for scenario in build_scenarios(combo, mode_settings, paths):
            start = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
            rlog.info(f"Building graph and config for {scenario.key}")

            graph = build_graph(**scenario.graph_kwargs)
            rlog.info("Graph has {} nodes".format(graph.node_count()))

            loaded_at = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin"))
            load_time = loaded_at - start

            output_path = mcr5_output_path / scenario.key
            config = build_config(out_dir=str(output_path), **scenario.config_kwargs)

            start_nodes = write_filtered_start_nodes(
                scenario.start_nodes,
                TARGET_H3_CELLS,
                output_path / "_start_nodes.parquet",
            )

            rlog.info(f"Running MCR5 for {scenario.key} (parallel over start nodes)")
            run_mcr5(str(start_nodes), config, graph)

            run_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - loaded_at
            total_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - start
            runtimes[scenario.key] = {
                "load_time": str(load_time),
                "run_time": str(run_time),
                "total_time": str(total_time),
            }

    with open(mcr5_output_path / "runtimes.json", "w") as f:
        json.dump(runtimes, f)
