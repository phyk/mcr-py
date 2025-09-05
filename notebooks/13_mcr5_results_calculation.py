import os
import pathlib

import mcr_py.utils.logger
import polars as pl
import tomllib
from mcr_py.mcr5.labels import read_labels_for_nodes
from mcr_py.minute_city import minute_city
from mcr_py.utils.cache import load_auxiliary_classes
from tqdm import tqdm

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

    geo_meta, geo_data = load_auxiliary_classes(
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

        labels = read_labels_for_nodes(
            entry.path,
            geo_data.pois.with_columns(pl.col("nearest_osm_node").alias("osm_node_id")).lazy(),
        )

        labels = minute_city.add_pois_to_labels(labels, geo_data.pois.lazy())
        labels_per_scenario[entry.name] = labels.collect(engine="streaming")  # type: ignore
    mcr_py.utils.logger.rlog.info("Reading MCR5 results done")
    poi_types = geo_data.pois.get_column("poi_type").unique().to_list()
    profiles_df_per_scenario = {}
    for scenario, labels in tqdm(labels_per_scenario.items()):
        scenario_df = minute_city.get_profiles_df(labels, poi_types, disable_tqdm=False)
        scenario_df = scenario_df.with_columns(scenario=pl.lit(scenario))
        profiles_df_per_scenario[scenario] = scenario_df
    profiles_df: pl.DataFrame = pl.concat(profiles_df_per_scenario.values())
    profiles_df.write_parquet(mcr5_output_path / "profiles.parquet", compression="snappy")
