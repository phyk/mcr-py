from logging import Logger
from typing import Optional

import polars as pl

from mcr_py._mcr_py import add_nearest_node_to_df
from mcr_py.mcr.bag import (
    IntermediateBags,
    convert_mc_raptor_bags_to_intermediate_bags,
)
from mcr_py.mcr.label import (
    IntermediateLabel,
    McRAPTORLabel,
    McRAPTORLabelWithPath,
)
from mcr_py.mcr.path import PathManager, PathType
from mcr_py.mcr.steps.interface import Step, StepBuilder
from mcr_py.raptor.bag import Bag
from mcr_py.raptor.mcraptor_single import McRaptorSingle
from mcr_py.utils import storage
from mcr_py.utils.logger import Timed, Timer

McRAPTORInputBags = dict[str, list[IntermediateLabel]]


class PublicTransportStep(Step):
    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
        structs_dict: dict,
        osm_node_to_stop_map: dict[int, str],
        stop_to_osm_node_map: dict[str, int],
    ) -> None:
        self.logger = logger
        self.timer = timer
        self.path_manager = path_manager
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        self.structs_dict = structs_dict
        self.osm_node_to_stop_map = osm_node_to_stop_map
        self.stop_to_osm_node_map = stop_to_osm_node_map

    def run(self, input_bags: IntermediateBags, offset: int = 0) -> IntermediateBags:
        with self.timer.info("Preparing input for MCRAPTOR step"):
            prepared_input_bags = self.prepare_public_transport_step_input(input_bags)
            if len(prepared_input_bags) == 0:
                self.logger.warning(
                    "Not a single stop is reached by the previous step - aborting MCRAPTOR step"
                )
                return {}

        with self.timer.info("Running MCRAPTOR step"):
            mc_raptor = McRaptorSingle(
                self.structs_dict,
                default_transfer_time=60,
                label_class=(McRAPTORLabel if self.disable_paths else McRAPTORLabelWithPath),
            )
            raw_public_transport_result_bags = mc_raptor.run(prepared_input_bags)  # type: ignore

        with self.timer.info("Extracting MCRAPTOR step bags"):
            raw_public_transport_result_bags = self.convert_public_transport_bags(
                raw_public_transport_result_bags, path_index_offset=offset
            )
            self.logger.debug(
                f"Extracted {len(raw_public_transport_result_bags)} bags from MCRAPTOR step"
            )
            if len(raw_public_transport_result_bags) == 0:
                self.logger.warning("No MCRAPTOR bags found")

        return raw_public_transport_result_bags

    # converts bags with node ids to bags with stop ids
    def prepare_public_transport_step_input(self, bags: IntermediateBags) -> McRAPTORInputBags:
        def translate_osm_node_id_to_stop_id(
            walking_node_id: int,
        ) -> str | None:
            if walking_node_id in self.osm_node_to_stop_map:
                stop_id = str(self.osm_node_to_stop_map[walking_node_id])
                return stop_id
            return None

        mc_raptor_bags = {
            node_id: bag
            for node_id, bag in bags.items()
            if translate_osm_node_id_to_stop_id(node_id) is not None
        }
        mc_raptor_bags_string: McRAPTORInputBags = {
            translate_osm_node_id_to_stop_id(node_id): Bag.from_labels(
                [
                    label.to_mc_raptor_label(
                        translate_osm_node_id_to_stop_id(node_id),  # type: ignore
                    )
                    for label in labels
                ]
            )
            for node_id, labels in mc_raptor_bags.items()
        }

        return mc_raptor_bags_string

    def convert_public_transport_bags(
        self, bags: dict[str, Bag], path_index_offset: int
    ) -> IntermediateBags:
        """
        Converts the bags from the McRAPTOR step to the intermediate bags
        format.
        The stop ids will be translated to the nearest osm node ids.

        :param bags: The bags resulting from MLC on a multi modal graph.
        :param path_index_offset: Used to properly build the path and should
            be set to the length of the path before the step.
        """
        mc_raptor_result_bags = {
            self.stop_to_osm_node_map[str(stop_id)]: bag for stop_id, bag in bags.items()
        }
        mc_raptor_result_bags = convert_mc_raptor_bags_to_intermediate_bags(
            mc_raptor_result_bags,
            min_path_length=path_index_offset + 1,
        )
        if self.path_manager:
            self.path_manager.extract_all_paths_from_bags(
                mc_raptor_result_bags,
                PathType.PUBLIC_TRANSPORT,
                path_index_offset=path_index_offset,
            )

        return mc_raptor_result_bags


class PublicTransportStepBuilder(StepBuilder):
    step = PublicTransportStep

    def __init__(
        self, structs_path: str, stops_path: str, walking_nodes: pl.DataFrame
    ) -> None:
        structs_dict = storage.read_any_dict(structs_path)
        with Timed.info("Reading stops"):
            self.stops_df = storage.read_df(stops_path)

        self.stops_df = add_nearest_node_to_df(self.stops_df, walking_nodes, 4839)

        stop_to_osm_node_map: dict[str, int] = self.stops_df.select(
            pl.col("stop_id", "nearest_osm_node")
        ).rows_by_key("nearest_osm_node", unique=True)
        osm_node_to_stop_map: dict[int, str] = {v: k for k, v in stop_to_osm_node_map.items()}
        self.kwargs = {
            "structs_dict": structs_dict,
            "osm_node_to_stop_map": osm_node_to_stop_map,
            "stop_to_osm_node_map": stop_to_osm_node_map,
        }
