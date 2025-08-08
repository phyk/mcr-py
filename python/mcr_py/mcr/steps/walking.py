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
from mcr_py.utils.logger import rlog


class WalkingStep(MLCStep):
    NAME = "walking"
    PATH_TYPE = PathType.WALKING


class WalkingStepBuilder(StepBuilder):
    step = WalkingStep

    def __init__(
        self,
        osm_nodes: pl.DataFrame,
        osm_edges: pl.DataFrame,
        pois: pl.DataFrame,
    ):
        osm_nodes = osm_nodes.rename({"rx_node_id": "id"})
        osm_edges = osm_edges.select(
            "source_rx_node_id", "dest_rx_node_id", "length"
        ).rename({"source_rx_node_id": "source_osm", "dest_rx_node_id": "dest_osm"})
        self.walking_nodes, self.walking_edges = create_walking_graph(
            osm_nodes, osm_edges
        )

        from_internal = {
            key: value
            for (key, value) in self.walking_nodes.select("id", "osm_id").rows()
        }
        to_internal = {
            value: key
            for (key, value) in self.walking_nodes.select("id", "osm_id").rows()
        }
        pois = pois.join(
            self.walking_nodes.select("id", "osm_id"),
            left_on="nearest_osm_node",
            right_on="osm_id",
        ).with_columns(pl.col("id").alias("nearest_osm_node"))
        self.walking_edges = add_weights(self.walking_edges, [TRAVEL_TIME_COLUMN])
        self.walking_edges = add_weights(self.walking_edges, [], hidden=True)
        raw_walking_edges = to_mlc_edges(self.walking_edges)

        rlog.debug("MLC Edges created")

        self.walking_graph_cache = GraphCache()
        self.walking_graph_cache.set_graph(raw_walking_edges)  # type: ignore

        rlog.debug("Adding POIs to graph")
        self.add_pois_to_walking_graph(pois)

        self.kwargs = {
            "graph_cache": self.walking_graph_cache,
            "from_internal": from_internal,
            "to_internal": to_internal,
        }

    def add_pois_to_walking_graph(self, pois: pl.DataFrame) -> None:
        """
        Adds POIs to the walking graph cache.

        Args:
            pois: A dataframe containing POIs. Must have the columns "nearest_osm_node" and "type".
        """
        type_map: dict[str, int] = {}
        for t in pois.get_column("poi_type").unique():
            type_map[t] = len(type_map)
        pois = pois.with_columns(
            pl.col("poi_type").replace(type_map).alias("type_internal").cast(pl.UInt8)
        )
        osm_nodes = osm.list_column_to_osm_nodes(
            self.walking_nodes, pois, "type_internal"
        )
        resetted_walking_node_id_to_type_map = {
            key: value[0]
            for key, value in osm_nodes.select(
                pl.col("id"),
                pl.col("type_internal"),
            )
            .rows_by_key(key="id", unique=True)
            .items()
        }

        self.walking_graph_cache.set_node_weights(resetted_walking_node_id_to_type_map)
