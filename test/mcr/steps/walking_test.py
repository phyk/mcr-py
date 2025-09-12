import logging

import polars as pl
from mcr_py.mcr.steps.walking import WalkingStepBuilder


def test_init_walking_step_builder() -> None:
    osm_nodes = pl.DataFrame(
        {
            "rx_node_id": [1, 2, 3, 4, 5],
            "osm_id": [10, 20, 30, 40, 50],
        }
    )
    osm_edges = pl.DataFrame(
        {
            "source_rx_node_id": [1, 2, 3, 4, 5],
            "dest_rx_node_id": [2, 3, 4, 5, 1],
            "length": [1, 2, 3, 4, 5],
        }
    )
    pois = pl.DataFrame(
        {
            "nearest_osm_node": [1, 2, 3, 4, 5],
            "poi_type": ["a", "a", "a", "b", "b"],
        }
    )

    walking_step = WalkingStepBuilder(osm_nodes, osm_edges, pois)

    logging.info(walking_step.walking_edges)
    assert walking_step.walking_edges.equals(
        pl.DataFrame(
            {
                "source_osm": [1, 2, 3, 4, 5],
                "dest_osm": [2, 3, 4, 5, 1],
                "length": [1, 2, 3, 4, 5],
                "travel_time": [7, 14, 21, 29, 36],
                "weights": [[7, 0], [14, 0], [21, 0], [29, 0], [36, 0]],
                "hidden_weights": [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]],
            }
        )
    )
