from logging import Logger
from typing import Callable, Collection, Optional

import polars as pl

import mcr_py as mcr_py
import mcr_py.osm.osm
from mcr_py import GraphCache
from mcr_py.mcr.bag import (
    IntermediateBags,
    convert_mlc_bags_to_intermediate_bags,
)
from mcr_py.mcr.data import ACCURACY_MULTIPLIER
from mcr_py.mcr.path import PathManager, PathType
from mcr_py.mcr.steps.interface import Step
from mcr_py.utils.logger import Timer


def add_pois_to_graph(
    nodes: pl.DataFrame, graph_cache: GraphCache, pois: pl.DataFrame
) -> pl.DataFrame:
    """
    Adds POIs to the walking graph cache.

    Args:
        nodes: Dataframe containing columns "id" and "osm_id".
        graph_cache: The graph cache to add POIs to.
        pois: A dataframe containing POIs. Must have the columns "nearest_osm_node" and "type".
    """
    pois = pois.join(
        nodes.select("id", "osm_id"),
        left_on="nearest_osm_node",
        right_on="osm_id",
    ).with_columns(pl.col("id").alias("nearest_osm_node"))

    type_map: dict[str, int] = {}
    for t in pois.get_column("poi_type").unique():
        type_map[t] = len(type_map)
    pois = pois.with_columns(
        pl.col("poi_type").replace(type_map).alias("type_internal").cast(pl.UInt8)
    )
    osm_nodes = mcr_py.osm.osm.list_column_to_osm_nodes(nodes, pois, "type_internal")
    reset_node_id_to_type_map = {
        key: value[0]
        for key, value in osm_nodes.select(
            pl.col("id"),
            pl.col("type_internal"),
        )
        .rows_by_key(key="id", unique=True)
        .items()
    }

    graph_cache.set_node_weights(reset_node_id_to_type_map)
    return osm_nodes


class MLCStepError(Exception):
    def __init__(self, node_id: int, *args: object) -> None:
        self.node_id = node_id
        super().__init__(*args)


class MLCStep(Step):
    NAME = "mlc"
    PATH_TYPE = PathType.UNDEFINED

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
        self.graph_cache = graph_cache
        self.to_internal = to_internal
        self.from_internal = from_internal

        self.update_label_func: Optional[str] = None
        self.valid_starting_nodes: Optional[Collection] = None
        self.valid_end_nodes: Optional[Collection] = None

        self.after_conversion_func: Optional[
            Callable[[IntermediateBags], IntermediateBags]
        ] = None

    def run(self, input_bags: IntermediateBags, offset: int = 0) -> IntermediateBags:
        if not input_bags:
            msg = "No input bags"
            raise ValueError(msg)

        with self.timer.info(f"Preparing input for {self.NAME} step"):
            prepared_input_bags = self.prepare_input(input_bags)
            if not prepared_input_bags:
                self.logger.warning(
                    "No valid starting node reached by previous step - aborting %s step",
                    self.NAME,
                )
                return {}

        with self.timer.info(f"Running {self.NAME} step"):
            self.logger.debug(
                "Prepared bags format %s:\n%s", type(prepared_input_bags), prepared_input_bags
            )
            raw_result_bags = mcr_py.run_mlc_with_bags(
                self.graph_cache,
                prepared_input_bags,
                update_label_func=self.update_label_func,
                disable_paths=self.disable_paths,
                enable_limit=self.enable_limit,
                accuracy=ACCURACY_MULTIPLIER,
            )
        with self.timer.info(f"Extracting {self.NAME} step bags"):
            converted_result_bags = self.convert_bags(
                raw_result_bags, path_index_offset=offset
            )
            self.logger.debug(
                "Extracted %s bags from %s step", len(converted_result_bags), self.NAME
            )

        return converted_result_bags

    def prepare_input(self, bags: IntermediateBags) -> IntermediateBags:
        if self.valid_starting_nodes is not None:
            bags = {
                node_id: bag
                for node_id, bag in bags.items()
                if node_id in self.valid_starting_nodes
            }

        try:
            bags = {
                self.to_internal[node_id]: [
                    label.to_mlc_label(
                        self.to_internal[node_id],
                    )
                    for label in labels
                ]
                for node_id, labels in bags.items()
            }
        except KeyError as e:
            raise MLCStepError(
                e.args[0],
                f"Node {e.args[0]} not found in graph cache - aborting {self.NAME} step. Current number of Bags is {len(bags)}",
            ) from e

        return bags

    def convert_bags(
        self,
        bags: dict,
        path_index_offset: int = 0,
    ) -> IntermediateBags:
        if self.valid_end_nodes is not None:
            bags = {
                node_id: labels
                for node_id, labels in bags.items()
                if node_id in self.valid_end_nodes
            }

        converted_bags = convert_mlc_bags_to_intermediate_bags(
            bags,
            translate_node_id=lambda node_id: self.from_internal[node_id],
        )

        if self.path_manager:
            self.path_manager.extract_all_paths_from_bags(
                converted_bags,
                self.PATH_TYPE,
                path_index_offset=path_index_offset,
            )

        if self.after_conversion_func:
            converted_bags = self.after_conversion_func(converted_bags)

        return converted_bags
