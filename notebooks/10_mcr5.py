"""Run the MCR5 core loop via the mcr-rust parallel `run_mcr5` orchestrator.

The previous version built `OSMData`/`GeoMeta` in Python, assembled
`initial_steps`/`repeating_steps` through `mcr_py.command.step_config`, and
drove a Python `multiprocessing` pool (`MCR5.run`). The Rust crate now owns
both the per-origin MCR pipeline and the parallel loop over start nodes, so
this notebook only has to:

1. build the routing graph from parquet layers (`build_graph`),
2. assemble a per-scenario `MCRConfig` (`build_config`),
3. hand a start-node parquet to `run_mcr5`, which fans out across origins.

Mode parameters (speeds, switch times, tariffs) and the scenarios to run
(combinations of modes) live entirely in `config.toml`'s `[modes.*]` tables
and `[mcr5] scenarios` list — see `mcr_py.mcr5.scenarios` for how a combo of
mode names turns into the graph layers and `MCRConfig` a scenario needs.

Notes on the API change:
- There is no `max_transfers` any more — the Rust loop runs to convergence.
- The public-transport start time is no longer a `run()` argument; it is the
  `anchor_time_secs` of `PublicTransportConfig`, derived from each scenario's
  `start_times` entry.
- `run_mcr5` writes one parquet per origin (named by its H3 cell) into
  `out_dir`, which is exactly what `20_mcr5_results_calculation.py` reads back.

Expected input parquet schemas (produced by the data pipeline):
- graph node/edge layers: as consumed by `mcr-rust` (`MCRGraphBuilder`).
- start nodes: columns `osm_node_id` (u64), `h3_cell_id` (str).
"""

import json
import pathlib
import tomllib
import zoneinfo
from datetime import datetime

from mcr_py.mcr5.mcr5 import build_config, build_graph, run_mcr5
from mcr_py.mcr5.scenarios import build_paths, build_scenarios
from mcr_py.utils.logger import rlog, setup

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

            rlog.info(f"Running MCR5 for {scenario.key} (parallel over start nodes)")
            run_mcr5(scenario.start_nodes, config, graph)

            run_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - loaded_at
            total_time = datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")) - start
            runtimes[scenario.key] = {
                "load_time": str(load_time),
                "run_time": str(run_time),
                "total_time": str(total_time),
            }

    with open(mcr5_output_path / "runtimes.json", "w") as f:
        json.dump(runtimes, f)
