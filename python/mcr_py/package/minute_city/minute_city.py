import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import polars_st as st
import polars as pl
from tqdm.auto import tqdm

from mcr_py.package.utils.logger import Timed
from mcr_py.package.minute_city import profile


def add_pois_to_labels(labels: pl.DataFrame, pois: st.GeoDataFrame) -> pl.DataFrame:
    poi_types = list(pois["type"].unique())
    for t in poi_types:
        pois[t] = (pois["type"] == t).astype(int)

    labels = labels.join(
        pois[["nearest_osm_node_id"] + poi_types],
        left_on="target_id_osm",
        right_on="nearest_osm_node_id",
    )

    return labels


def get_profiles_df(
    labels_with_pois: st.GeoDataFrame,
    types: list[str],
    disable_tqdm: bool = False,
    leave_tqdm: bool = True,
) -> pl.DataFrame:
    """
    Calculates the profiles for the given labels.
    """
    with Timed.debug("Grouping labels"):
        grouped = labels_with_pois.groupby("start_id_hex")
        n_groups = len(grouped)

    partial_worker = partial(profile.profile_calculation_worker, types)

    profiles = {}
    with (
        Timed.debug("Calculating profiles"),
        ProcessPoolExecutor(max_workers=multiprocessing.cpu_count() - 2) as executor,
    ):
        pbar = tqdm(total=n_groups, disable=disable_tqdm, leave=leave_tqdm)
        for result in executor.map(partial_worker, grouped):
            pbar.update(1)
            if result is not None:
                name, prof = result
                profiles[name] = prof
        pbar.close()

    with Timed.debug("Creating profiles dataframe"):
        start_time: int = labels_with_pois["time"].min()  # type: ignore
        profiles_df = profile.build_profiles_df(profiles, start_time)

        # tuning
        profiles_df = profile.add_any_column_is_different_column(profiles_df)
        profiles_df = profile.add_required_cost_for_optimum_column(profiles_df)
        profiles_df = profile.add_optimum_column(profiles_df)

    return profiles_df
