from logging import Logger
from typing import Optional

import polars as pl

import mcr_py.mcr.data
from mcr_py import GraphCache
from mcr_py.mcr.bag import IntermediateBags
from mcr_py.mcr.data import (
    AVG_CAR_SPEED,
    TRAVEL_TIME_COLUMN,
    TRAVEL_TIME_DRIVING_COLUMN,
    WALKING_PREFIX,
    add_weights,
    create_multi_modal_graph,
    to_mlc_edges,
)
from mcr_py.mcr.path import PathManager, PathType
from mcr_py.mcr.steps.interface import StepBuilder
from mcr_py.mcr.steps.mlc import MLCStep
from mcr_py.osm import osm
from mcr_py.utils.logger import Timer


class PersonalCarStep(MLCStep):
    NAME = "personal car"
    PATH_TYPE = PathType.DRIVING_WALKING

    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,  # noqa: FBT001
        disable_paths: bool,  # noqa: FBT001
        graph_cache: GraphCache,
        to_internal: dict,
        from_internal: dict,
    ) -> None:
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.update_label_func = "personal_car"
        self.graph_cache = graph_cache
        self.to_internal = to_internal
        self.from_internal = from_internal
        self.valid_end_nodes = from_internal.keys()

        self.valid_starting_nodes = None

        def nullify_car_hidden_values_cost(
            bags: IntermediateBags,
        ) -> IntermediateBags:
            for bag in bags.values():
                for label in bag:
                    label.hidden_values[0] = 0
            return bags

        self.after_conversion_func = nullify_car_hidden_values_cost


class PersonalCarStepBuilder(StepBuilder):
    step = PersonalCarStep

    def __init__(
        self,
        walking_nodes: pl.DataFrame,
        walking_edges: pl.DataFrame,
        driving_nodes: pl.DataFrame,
        driving_edges: pl.DataFrame,
        pois: pl.DataFrame,
    ) -> None:
        multi_modal_nodes, multi_modal_edges = create_multi_modal_graph(
            walking_nodes, walking_edges, driving_nodes, driving_edges, AVG_CAR_SPEED
        )
        multi_modal_nodes = multi_modal_nodes.with_columns(
            pl.int_range(pl.len(), dtype=pl.UInt64).alias("id")
        )
        multi_modal_edges = (
            multi_modal_edges.join(
                multi_modal_nodes.select(["id", "osm_id"]),
                left_on="source_osm",
                right_on="osm_id",
            )
            .with_columns(pl.col("id").alias("source_osm"))
            .join(
                multi_modal_nodes.select(["id", "osm_id"]),
                left_on="dest_osm",
                right_on="osm_id",
            )
            .with_columns(pl.col("id").alias("dest_osm"))
        )

        from_internal = dict(multi_modal_nodes.select("id", "osm_id").rows())
        to_internal = {
            value: key for (key, value) in multi_modal_nodes.select("id", "osm_id").rows()
        }
        to_internal_walking = {int(key[1:]): value for (key, value) in to_internal.items()}

        self.osm_node_to_mm_car_reset_map = {
            int(k[1:]): v
            for k, v in to_internal.items()
            if k[0] == mcr_py.mcr.data.DRIVING_PREFIX
        }
        self.mm_walking_node_reset_to_osm_node_map = {
            k: int(v[1:]) for k, v in from_internal.items() if v[0] == WALKING_PREFIX
        }

        multi_modal_edges = add_weights(multi_modal_edges, [TRAVEL_TIME_COLUMN])
        multi_modal_edges = add_weights(
            multi_modal_edges, [TRAVEL_TIME_DRIVING_COLUMN], hidden=True
        )

        raw_edges = to_mlc_edges(multi_modal_edges)
        self.osm_nodes = walking_nodes.with_columns(
            pl.col("osm_id").replace(to_internal_walking).alias("id")
        )
        self.mm_graph_cache = GraphCache()
        self.mm_graph_cache.set_graph(raw_edges)  # type: ignore
        self.add_pois_to_mm_graph(pois)

        self.kwargs = {
            "graph_cache": self.mm_graph_cache,
            "to_internal": self.osm_node_to_mm_car_reset_map,
            "from_internal": self.mm_walking_node_reset_to_osm_node_map,
        }

    def add_pois_to_mm_graph(self, pois: pl.DataFrame) -> None:
        """
        Adds POIs to the multi modal graph cache.

        Args:
            pois: A dataframe containing POIs. Must have the columns "nearest_osm_node_id" and "type".
        """
        type_map: dict[str, int] = {}
        for t in pois.get_column("poi_type").unique():
            type_map[t] = len(type_map)
        pois = pois.with_columns(
            pl.col("poi_type").replace(type_map).alias("type_internal").cast(pl.UInt8)
        )
        self.osm_nodes = osm.list_column_to_osm_nodes(self.osm_nodes, pois, "type_internal")

        reset_mm_walking_node_id_to_type_map = {
            key: value[0]
            for key, value in self.osm_nodes.select(
                pl.col("id"),
                pl.col("type_internal"),
            )
            .rows_by_key(key="id", unique=True)
            .items()
        }

        self.mm_graph_cache.set_node_weights(reset_mm_walking_node_id_to_type_map)
