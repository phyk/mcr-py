import polars as pl
from mcr_py import GraphCache
from mcr_py.mcr.data import (
    TRAVEL_TIME_COLUMN,
    add_weights,
    create_walking_graph,
    to_mlc_edges,
)
from mcr_py.mcr.path import PathType
from mcr_py.mcr.steps.interface import StepBuilder
from mcr_py.mcr.steps.mlc import MLCStep
from mcr_py.osm import osm


class WalkingStep(MLCStep):
    NAME = "walking"
    PATH_TYPE = PathType.WALKING


class WalkingStepBuilder(StepBuilder):
    step = WalkingStep

    # Revert change to osm ids completely
    # Just use osm ids, do never update them
    # Logic behind this:
    # synchronization of node resetting is hard to achieve across multiple modes
    def __init__(
        self,
        osm_nodes: pl.DataFrame,
        osm_edges: pl.DataFrame,
        pois: pl.DataFrame,
    ):
        walking_nodes, walking_edges = create_walking_graph(osm_nodes, osm_edges)

        # self.walking_node_to_resetted_map = walking_nodes.select(
        #     ["osm_id", "id"]
        # ).rows_by_key("osm_id", unique=True)

        # self.resetted_to_walking_node_map = get_reverse_map(
        #     self.walking_node_to_resetted_map
        # )

        self.walking_edges = add_weights(walking_edges, [TRAVEL_TIME_COLUMN])
        self.walking_edges = add_weights(walking_edges, [], hidden=True)

        raw_walking_edges = to_mlc_edges(self.walking_edges)
        self.osm_nodes = osm_nodes

        self.walking_graph_cache = GraphCache()
        self.walking_graph_cache.set_graph(raw_walking_edges)  # type: ignore
        self.add_pois_to_walking_graph(pois)

        self.kwargs = {
            "graph_cache": self.walking_graph_cache,
            # "to_internal": self.walking_node_to_resetted_map,
            # "from_internal": self.resetted_to_walking_node_map,
        }

    def add_pois_to_walking_graph(self, pois: pl.DataFrame) -> None:
        """
        Adds POIs to the walking graph cache.

        Args:
            pois: A dataframe containing POIs. Must have the columns "nearest_osm_node_id" and "type".
        """
        osm_nodes = osm.list_column_to_osm_nodes(self.osm_nodes, pois, "poi_type")
        type_map: dict[str, int] = {}
        for t in pois.get_column("poi_type").unique():
            type_map[t] = len(type_map)

        osm_nodes = osm_nodes.with_columns(
            pl.col("poi_type").replace(type_map).alias("type_internal")
        )

        resetted_walking_node_id_to_type_map = osm_nodes.select(
            pl.col("nearest_osm_node").alias("resetted_walking_node_id"),
            pl.col("type_internal"),
        ).rows_by_key(key="resetted_walking_node_id", unique=True)

        self.walking_graph_cache.set_node_weights(resetted_walking_node_id_to_type_map)
