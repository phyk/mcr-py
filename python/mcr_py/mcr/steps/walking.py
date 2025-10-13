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
from mcr_py.mcr.steps.mlc import MLCStep, add_pois_to_graph
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
    ) -> None:
        osm_nodes = osm_nodes.rename({"rx_node_id": "id"})
        osm_edges = osm_edges.select("source_rx_node_id", "dest_rx_node_id", "length").rename(
            {"source_rx_node_id": "source_osm", "dest_rx_node_id": "dest_osm"}
        )
        self.walking_nodes, self.walking_edges = create_walking_graph(osm_nodes, osm_edges)

        from_internal = dict(self.walking_nodes.select("id", "osm_id").rows())
        to_internal = {
            value: key for (key, value) in self.walking_nodes.select("id", "osm_id").rows()
        }

        self.walking_edges = add_weights(self.walking_edges, [TRAVEL_TIME_COLUMN])
        self.walking_edges = add_weights(self.walking_edges, [], hidden=True)
        raw_walking_edges = to_mlc_edges(self.walking_edges)

        rlog.debug("MLC Edges created")

        self.walking_graph_cache = GraphCache()
        self.walking_graph_cache.set_graph(raw_walking_edges)  # type: ignore

        rlog.debug("Adding POIs to graph")
        self.walking_nodes = add_pois_to_graph(
            self.walking_nodes, self.walking_graph_cache, pois
        )

        self.kwargs = {
            "graph_cache": self.walking_graph_cache,
            "from_internal": from_internal,
            "to_internal": to_internal,
        }
