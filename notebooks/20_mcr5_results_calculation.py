import itertools
import os
import pathlib

import mcr_py.helper_functions
import mcr_py.utils.logger
import polars as pl
import tomllib
from mcr_py.mcr5.labels import read_labels_for_nodes
from mcr_py.minute_city import minute_city
from mcr_py.minute_city.profile import fill_columns_by_left
from tqdm import tqdm


def calculate_unit_metrics(profiles_df: pl.DataFrame) -> pl.DataFrame:
    profiles_df = profiles_df.with_columns(
        (pl.col("required_cost_for_optimal") / pl.lit(100)).alias(
            "required_cost_for_optimal_in_euro"
        ),
        (pl.col("optimal") / pl.lit(60)).alias("optimal_in_minutes"),
    )
    return profiles_df


def trim_trailing_numbers(string: str) -> str:
    has_trailing_numbers = string[-1].isdigit()
    if not has_trailing_numbers:
        return string
    return "_".join(string.split("_")[:-1])


def reorder_columns(profiles_df: pl.DataFrame) -> pl.DataFrame:
    cost_columns = []
    other_columns = []

    for column in profiles_df.columns:
        if column.startswith("cost_"):
            cost_columns.append(column)
        else:
            other_columns.append(column)

    cost_columns.sort()
    new_columns = other_columns + cost_columns

    # Reorder the DataFrame columns
    profiles_df = profiles_df.select(new_columns)

    return profiles_df


if __name__ == "__main__":
    city_name = "cologne"
    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)
    mcr_py.utils.logger.setup(settings["run_type"]["run_type"])

    data_directory = pathlib.Path(__file__).parent.parent.resolve() / "data"
    base_directory = data_directory / settings["timestamp"]["timestamp"]
    cache_path = base_directory / "cache/"
    osm_path = base_directory / "osm_raw"
    geometa_path = base_directory / f"cache/{city_name}_geometa.pkl"
    mcr5_output_path = base_directory / f"mcr5_results/{city_name}"

    geo_meta, geo_data = mcr_py.helper_functions.load_auxiliary_classes(
        geo_meta_path=geometa_path,
        city_id=settings["city"][city_name]["german_alt"],
        osm_path=osm_path,
        cache_path=cache_path,
    )

    labels_per_scenario = {}
    mcr_py.utils.logger.rlog.info("Reading MCR5 results")
    for entry in os.scandir(mcr5_output_path):
        if not entry.is_dir():
            continue

        # Reduce labels to target nodes that are in the POIs
        labels = read_labels_for_nodes(
            entry.path,
            geo_data.pois.with_columns(pl.col("nearest_osm_node").alias("osm_node_id")).lazy(),
        )

        labels = minute_city.add_pois_to_labels(labels, geo_data.pois.lazy())
        labels_per_scenario[entry.name] = labels.collect(engine="streaming")  # type: ignore
    mcr_py.utils.logger.rlog.info("Reading MCR5 results done")
    poi_types = geo_data.pois.get_column("poi_type").unique().to_list()

    # Calculate the optimal profile per scenario
    profiles_df_per_scenario = {}
    for scenario, labels in tqdm(labels_per_scenario.items()):
        core_scenario = trim_trailing_numbers(scenario)
        scenario_df = minute_city.get_profiles_df(labels, poi_types, disable_tqdm=False)
        scenario_df = scenario_df.with_columns(
            scenario=pl.lit(scenario), core_scenario=pl.lit(core_scenario)
        )
        profiles_df_per_scenario[scenario] = scenario_df
    profiles_df: pl.DataFrame = pl.concat(profiles_df_per_scenario.values())
    profiles_df = calculate_unit_metrics(profiles_df)
    profiles_df = reorder_columns(profiles_df)
    profiles_df = fill_columns_by_left(profiles_df)
    profiles_df.write_parquet(
        mcr5_output_path / "profiles_tariffs.parquet", compression="snappy"
    )

    # Calculate the optimal profile per scenario and poi_type
    profiles_df_per_scenario_per_type = {t: {} for t in poi_types}
    for t, (scenario, labels) in tqdm(
        list(itertools.product(poi_types, labels_per_scenario.items()))
    ):
        core_scenario = trim_trailing_numbers(scenario)
        scenario_df = minute_city.get_profiles_df(labels, [t], disable_tqdm=True)
        scenario_df = scenario_df.with_columns(
            scenario=pl.lit(scenario), category=pl.lit(t), core_scenario=pl.lit(core_scenario)
        )
        profiles_df_per_scenario_per_type[t][scenario] = scenario_df
    profiles_df_categories = pl.concat(
        [df for dfs in profiles_df_per_scenario_per_type.values() for df in dfs.values()]
    )
    profiles_df_categories = calculate_unit_metrics(profiles_df_categories)
    profiles_df_categories = reorder_columns(profiles_df_categories)
    profiles_df_categories = fill_columns_by_left(profiles_df_categories)
    profiles_df_categories.write_parquet(
        mcr5_output_path / "profiles_categories_tariffs.parquet", compression="snappy"
    )
