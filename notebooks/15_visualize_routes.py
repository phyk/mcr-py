"""Detailed route maps for all MCR5 origin cells (Cologne).

For each sampled H3 origin cell this script builds one HTML file containing:

  - Pruned background: H3 cells and filtered street network covering all nodes
    traversed by any route across the three scenarios.
  - Route paths: per-scenario, per-mode PathLayers (blue = walking access,
    orange = cycling leg, red = car leg) showing the min-time route to each
    reachable POI destination.
  - GTFS connections: stop-to-stop links inside the routed area, one layer per
    vehicle type (train / subway / tram / bus / ...); hover shows line + type.
    Public-transport route legs are coloured by vehicle type and show the line.
  - Start location: amber marker for the origin walking node.
  - POIs: colour-coded markers for every POI in the pruned area; hover a
    marker to see its category.

Layers are toggleable in the control panel (top-right).

Output: data/20260619/visualize/cologne_routes_<cell_id>.html (one per cell)
"""

import pathlib

from mcr_py.visualize.routes import visualize_origin_routes

CELLS = [
    "891fa18aba7ffff",
    "891fa19a9dbffff",
    "891fa1894d7ffff",
    "891fa19a1a7ffff",
    "891fa18b22fffff",
    "891fa188213ffff",
    "891fa188283ffff",
    "891fa565b5bffff",
    "891fa188113ffff",
]

DATA_DIR = pathlib.Path(__file__).parent.parent / "data" / "20260619"
RESULTS_DIR = DATA_DIR / "mcr5_results" / "cologne_reduced_paths"
GRAPH_DIR = DATA_DIR / "graph" / "cologne"
POI_PATH = GRAPH_DIR / "poi_nodes.parquet"
GTFS_DIR = DATA_DIR / "gtfs_clean" / "cologne"
OUTPUT_DIR = DATA_DIR / "visualize"

if __name__ == "__main__":
    for cell_id in CELLS:
        output_path = OUTPUT_DIR / f"cologne_routes_{cell_id}.html"
        print(f"Rendering {cell_id} ...", end=" ", flush=True)
        visualize_origin_routes(
            results_dir=RESULTS_DIR,
            graph_dir=GRAPH_DIR,
            poi_path=POI_PATH,
            cell_id=cell_id,
            output_path=output_path,
            zoom=13,
            gtfs_dir=GTFS_DIR,
        )
        print(f"saved → {output_path.name}")
