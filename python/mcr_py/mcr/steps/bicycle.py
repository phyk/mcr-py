import pathlib
from logging import Logger
from typing import Optional

import numpy as np
import polars as pl
import polars_st as st

from mcr_py import GraphCache
from mcr_py._mcr_py import add_nearest_node_to_df
from mcr_py.mcr.bag import IntermediateBags
from mcr_py.mcr.data import (
    AVG_BIKING_SPEED,
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
from mcr_py.utils import storage
from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import Timer, rlog


class BicycleStep(MLCStep):
    NAME = "bicycle"
    PATH_TYPE = PathType.CYCLING_WALKING

    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
        graph_cache: GraphCache,
        to_internal: dict,
        from_internal: dict,
        bicycle_transfer_osm_node_ids: np.ndarray,
        update_label_func: str,
    ) -> None:
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.update_label_func = update_label_func
        self.graph_cache = graph_cache
        self.to_internal = to_internal
        self.from_internal = from_internal
        self.valid_starting_nodes = bicycle_transfer_osm_node_ids
        self.valid_end_nodes = from_internal.keys()

        def nullify_bicycle_hidden_values_cost(
            bags: IntermediateBags,
        ) -> IntermediateBags:
            for bag in bags.values():
                for label in bag:
                    label.hidden_values[0] = 0
            return bags

        self.after_conversion_func = nullify_bicycle_hidden_values_cost


class BicycleStepBuilder(StepBuilder):
    step = BicycleStep

    def __init__(
        self,
        update_label_func: str,
        bicycle_location_path: pathlib.Path,
        geo_meta: GeoMeta,
        walking_nodes: pl.DataFrame,
        walking_edges: pl.DataFrame,
        cycling_nodes: pl.DataFrame,
        cycling_edges: pl.DataFrame,
        pois: pl.DataFrame,
    ) -> None:
        bicycle_locations = None
        if bicycle_location_path != "":
            bicycle_locations = storage.read_df(bicycle_location_path)
            bicycle_locations = bicycle_locations.with_columns(
                st.point(pl.concat_arr(["lon", "lat"])).alias("geometry")
            )
            bicycle_locations = geo_meta.crop_gdf(bicycle_locations)
            # max distance = 1000,
            bicycle_locations = add_nearest_node_to_df(bicycle_locations, cycling_nodes, 4839)
        else:
            rlog.warning("No bicycle locations provided - will use random locations")

        if bicycle_locations is not None:
            cycling_nodes = mark_bicycles(cycling_nodes, bicycle_locations)
        else:
            cycling_nodes = mark_bicycles_random(cycling_nodes, 100)

        bicycle_transfer_osm_node_ids = cycling_nodes.filter(pl.col("has_bicycle")).get_column(
            "osm_id"
        )

        multi_modal_nodes, multi_modal_edges = create_multi_modal_graph(
            walking_nodes, walking_edges, cycling_nodes, cycling_edges, AVG_BIKING_SPEED
        )

        # (
        #     multi_modal_nodes,
        #     multi_modal_edges,
        #     self.multi_modal_node_to_resetted_map,
        # ) = reset_node_ids(multi_modal_nodes, multi_modal_edges)

        # self.resetted_to_multi_modal_node_map = get_reverse_map(
        #     self.multi_modal_node_to_resetted_map
        # )

        # self.osm_node_to_mm_bicycle_resetted_map = {
        #     int(k[1:]): v
        #     for k, v in self.multi_modal_node_to_resetted_map.items()
        #     if k[0] == DRIVING_PREFIX
        # }
        # self.mm_walking_node_resetted_to_osm_node_map = {
        #     k: int(v[1:])
        #     for k, v in self.resetted_to_multi_modal_node_map.items()
        #     if v[0] == WALKING_PREFIX
        # }

        multi_modal_edges = add_weights(multi_modal_edges, [TRAVEL_TIME_COLUMN])
        multi_modal_edges = add_weights(
            multi_modal_edges, [TRAVEL_TIME_DRIVING_COLUMN], hidden=True
        )

        to_mlc_edges(multi_modal_edges)
        self.osm_nodes = walking_nodes
        self.mm_graph_cache = GraphCache()
        # self.mm_graph_cache.set_graph(raw_edges)
        self.add_pois_to_mm_graph(pois)

        self.kwargs = {
            "graph_cache": self.mm_graph_cache,
            # "to_internal": self.osm_node_to_mm_bicycle_resetted_map,
            # "from_internal": self.mm_walking_node_resetted_to_osm_node_map,
            "bicycle_transfer_osm_node_ids": bicycle_transfer_osm_node_ids,
            "update_label_func": update_label_func,
        }

    def add_pois_to_mm_graph(self, pois: pl.DataFrame) -> None:
        """
        Adds POIs to the multi modal graph cache.

        Args:
            pois: A dataframe containing POIs. Must have the columns "nearest_osm_node_id" and "type".
        """
        self.osm_nodes = osm.list_column_to_osm_nodes(self.osm_nodes, pois, "poi_type")
        self.type_map: dict[str, int] = {}
        for t in pois.get_column("poi_type").unique():
            self.type_map[t] = len(self.type_map)

        self.osm_nodes = self.osm_nodes.with_columns(
            pl.col("poi_type").replace(self.type_map).alias("type_internal")
        )

        resetted_mm_walking_node_id_to_type_map = self.osm_nodes.select(
            (WALKING_PREFIX + pl.col("nearest_osm_node").cast(pl.String)).alias(
                "resetted_mm_walking_node_id"
            ),
            pl.col("type_internal"),
        ).rows_by_key(key="resetted_mm_walking_node_id", unique=True)

        self.mm_graph_cache.set_node_weights(resetted_mm_walking_node_id_to_type_map)


def mark_bicycles(
    nodes: pl.DataFrame,
    bicycle_locations: pl.DataFrame,
) -> pl.DataFrame:
    nodes = nodes.with_columns(
        pl.col("osm_id")
        .is_in(bicycle_locations.get_column("nearest_osm_node"))
        .alias("has_bicycle")
    )

    return nodes


def mark_bicycles_random(nodes: pl.DataFrame, n: int) -> pl.DataFrame:
    nodes = nodes.with_columns(
        pl.col("osm_id").is_in(nodes.get_column("osm_id").sample(n)).alias("has_bicycle")
    )

    return nodes
