import polars as pl
from tqdm.auto import tqdm

from mcr_py.minute_city import profile
from mcr_py.utils.logger import Timed, rlog


def add_pois_to_labels(labels: pl.LazyFrame, pois: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add POIs as boolean columns to the labels frame

    Args:
        labels (pl.LazyFrame): Result labels
        pois (pl.LazyFrame): POIs

    Returns:
        pl.LazyFrame: Labels with POIs as poi_type columns
    """
    poi_types = pois.collect().get_column("poi_type").unique().to_list()
    for t in poi_types:
        pois = pois.with_columns((pl.col("poi_type") == t).alias(t).cast(pl.Int8()))
    pois = pois.select("nearest_osm_node", *poi_types).unique()

    labels = labels.join(
        pois,
        how="inner",
        left_on="target_id_osm",
        right_on="nearest_osm_node",
    )

    return labels


def get_profiles_df(
    labels_with_pois: pl.DataFrame,
    poi_types: list[str],
    disable_tqdm: bool = False,  # noqa: FBT001, FBT002
    leave_tqdm: bool = True,  # noqa: FBT001, FBT002
) -> pl.DataFrame:
    """
    Calculates the profiles for the given labels.
    """
    with Timed.debug("Grouping labels"):
        grouped = labels_with_pois.group_by("start_id_hex")
        n_groups = labels_with_pois.get_column("start_id_hex").n_unique()

    profiles: dict[str, list[tuple[int, int]]] = {}
    with Timed.debug("Calculating profiles"):
        pbar = tqdm(total=n_groups, disable=disable_tqdm, leave=leave_tqdm)
        # for result in executor.map(partial_worker, grouped):
        for group in grouped:
            result = profile.profile_calculation_worker(poi_types=poi_types, args=group)  # type: ignore
            pbar.update(1)
            if result is not None:
                name, prof = result
                profiles[name] = prof
        pbar.close()

    with Timed.debug("Creating profiles dataframe"):
        start_time: int = labels_with_pois.get_column("time").min()  # type: ignore
        rlog.debug(next(iter(profiles.items())))
        profiles_df = profile.build_profiles_df(profiles, start_time)

        # tuning
        profiles_df = profile.add_any_column_is_different_column(profiles_df)
        profiles_df = profile.add_optimum_column(profiles_df)

    return profiles_df
