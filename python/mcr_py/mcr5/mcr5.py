import logging
import os
import pathlib
import pickle
import time
from multiprocessing import Process, Queue

import polars as pl
import psutil
from tqdm.auto import tqdm

from mcr_py.mcr.config import MCRConfig
from mcr_py.mcr.mcr import MCR, StepBuilderMatrix
from mcr_py.mcr.output import OutputFormat
from mcr_py.utils import key
from mcr_py.utils.logger import (
    copy_settings_to_root_logger,
    make_string_stream_logger,
    rlog,
)


class MCR5:
    def __init__(
        self,
        initial_steps: StepBuilderMatrix,
        repeating_steps: StepBuilderMatrix,
        min_free_memory: float = 3.0,
        max_processes: int = key.DEFAULT_N_PROCESSES,
        disable_paths: bool = True,  # noqa: FBT001, FBT002
    ) -> None:
        self.initial_steps = initial_steps
        self.repeating_steps = repeating_steps
        self.min_free_memory = min_free_memory
        self.max_processes = max_processes
        self.disable_paths = disable_paths

    def run(
        self,
        location_mappings: pl.DataFrame,
        start_time: str,
        output_dir: str,
        max_transfers: int = 2,
        verbose: bool = False,  # noqa: FBT001, FBT002
    ) -> list[tuple[str, Exception]]:
        """
        Run a MCR5 analysis for each location mapping.
        Returns a list of tuples, which represent errors that occurred during the analysis.
        The first element of the tuple is the h3 cell, the second element is the exception.
        """
        processes = []
        errors = Queue(maxsize=-1)

        p_id_hex_id_map = {}

        # ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        errors_list = []
        progress_bar = tqdm(location_mappings, desc="Starting")
        for idx, (osm_node_id, h3_cell) in enumerate(location_mappings.rows()):
            while (
                self.get_active_process_count(processes) >= self.max_processes
                or get_available_memory() < self.min_free_memory
            ):
                errors_list.extend([errors.get() for _ in range(errors.qsize())])
                if errors.full():
                    msg = "Error queue is full."
                    raise Exception(msg)
                if verbose:
                    self.print_status(processes, progress_bar)
                time.sleep(1)
            p = Process(
                target=self.run_mcr,
                kwargs={
                    "errors": errors,
                    "h3_cell": h3_cell,
                    "osm_node_id": osm_node_id,
                    "initial_steps": self.initial_steps,
                    "repeating_steps": self.repeating_steps,
                    "start_time": start_time,
                    "max_transfers": max_transfers,
                    "output_dir": output_dir,
                    "disable_paths": self.disable_paths,
                },
            )

            p.start()
            processes.append(p)
            p_id_hex_id_map[p.pid] = h3_cell
            if verbose and idx % 50 == 0:
                self.print_status(processes, progress_bar)

        while self.get_active_process_count(processes) > 0:
            if verbose:
                self.print_status(processes, progress_bar)
            errors_list.extend([errors.get() for _ in range(errors.qsize())])
            if errors.full():
                msg = "Error queue is full."
                raise Exception(msg)
            time.sleep(1)
        progress_bar.update(len(location_mappings) - progress_bar.n)
        progress_bar.close()

        for p in processes:
            p.join()

        rlog.info("All processes finished.")

        while not errors.empty():
            errors_list.append(errors.get())
        errors = errors_list
        if len(errors) > 0:
            rlog.warning(f"{len(errors)} errors occurred during the analysis.")

        with open(os.path.join(output_dir, "errors.pkl"), "wb") as f:
            pickle.dump(errors, f)

        return errors

    def run_mcr(
        self,
        errors: Queue,
        h3_cell: str,
        osm_node_id: int,
        initial_steps: StepBuilderMatrix,
        repeating_steps: StepBuilderMatrix,
        start_time: str,
        max_transfers: int,
        output_dir: pathlib.Path,
        disable_paths: bool = True,  # noqa: FBT001, FBT002
    ) -> None:
        output = (
            output_dir / f"{h3_cell}.feather"
            if disable_paths
            else output_dir / f"{h3_cell}.pkl"
        )
        logger_copy, log_stream = make_string_stream_logger(f"mcr5-{h3_cell}", logging.DEBUG)
        copy_settings_to_root_logger(logger_copy)
        mcr_config = MCRConfig(
            logger=logger_copy, disable_paths=disable_paths, enable_limit=True
        )
        try:
            mcr_runner = MCR(
                initial_steps,
                repeating_steps,
                mcr_config,
                output_format=OutputFormat.DF_FEATHER
                if disable_paths
                else OutputFormat.CLASS_PICKLE,
            )

            mcr_runner.run(
                start_node_id=osm_node_id,
                start_time=start_time,
                max_transfers=max_transfers,
                output_path=output,
            )
        except BaseException as e:
            log_stream_value = log_stream.getvalue()
            errors.put(
                {
                    "h3_cell": h3_cell,
                    "osm_node_id": osm_node_id,
                    "start_time": start_time,
                    "max_transfers": max_transfers,
                    "output_path": output,
                    "error": e.__repr__(),  # the exception object might not be picklable
                    "logs": log_stream_value,
                },
            )

    def print_status(
        self,
        processes: list[Process],
        progress_bar: tqdm,
    ) -> None:
        available_memory = pretty_bytes(get_available_memory())
        active_processes_count = self.get_active_process_count(processes)

        started_processes = len(processes)
        finished_processes = started_processes - active_processes_count

        progress_bar.update(finished_processes - progress_bar.n)
        progress_bar.set_description(
            f"Available memory: {available_memory} | active: {active_processes_count}         ",
        )

    def get_active_process_count(self, processes: list[Process]) -> int:
        return sum(p.is_alive() for p in processes)


def get_available_memory() -> int:
    return psutil.virtual_memory().available


def pretty_bytes(b: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if b < 1024:
            return f"{b:.2f}{unit}"
        b /= 1024
    return f"{b:.2f}EiB"
