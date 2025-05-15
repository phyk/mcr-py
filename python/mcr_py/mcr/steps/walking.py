import polars as pl
from mcr_py import GraphCache
from mcr_py.mcr.data import (
    TRAVEL_TIME_COLUMN,
    add_id_column,
    add_weights,
    create_walking_graph,
    get_reverse_map,
    reset_node_ids,
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
        self.walking_nodes, self.walking_edges = create_walking_graph(
            osm_nodes, osm_edges
        )

        self.walking_nodes = add_id_column(self.walking_nodes)

        self.mapping_id_df = self.walking_nodes.select(
            pl.col(["osm_id", "id"]).unique()
        )
        self.walking_node_to_resetted_map = {
            key: value[0]
            for key, value in self.mapping_id_df.rows_by_key(
                key="osm_id", unique=True
            ).items()
        }
        self.resetted_to_walking_node_map = get_reverse_map(
            self.walking_node_to_resetted_map
        )

        self.walking_edges = reset_node_ids(self.walking_edges, self.mapping_id_df)
        pois = pois.join(
            self.mapping_id_df, left_on="nearest_osm_node", right_on="osm_id"
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
            "to_internal": self.walking_node_to_resetted_map,
            "from_internal": self.resetted_to_walking_node_map,
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
        rlog.debug("Format of osm nodes: {}".format(osm_nodes.columns))
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
