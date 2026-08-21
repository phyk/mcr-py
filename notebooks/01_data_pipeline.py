"""Prepare every input the mcr-rust `run_mcr5` orchestrator needs for a city.

This is the data-preparation stage that precedes `10_mcr5.py`. The previous
version built an in-memory `OSMData`/rustworkx graph for the old Python MLC
runtime; the Rust crate now owns graph assembly and routing, so this pipeline's
only job is to write the parquet/geojson layers that `MCRGraphBuilder`
(`mcr-rust/src/network.rs`), `read_start_nodes` (`input.rs`) and
`TransitData::load` (`gtfs.rs`) read back.

For each city it produces, under ``data/<timestamp>/``:

``graph/<city>/``
    ``walking_nodes.parquet``  ``osm_id:u64, lat:f64, long:f64``
    ``walking_edges.parquet``  ``source_osm:u64, dest_osm:u64, length:f64``
    ``cycling_{nodes,edges}.parquet``
    ``car_{nodes,edges}.parquet``     (osmtools' "driving" network)
    ``poi_nodes.parquet``      ``osm_id, lat, long, nearest_osm_node, dist_to_nearest, poi_type``
``start_nodes/<city>/``
    ``walking.parquet``        ``osm_node_id:u64, h3_cell_id:str`` (+ diagnostics)
    ``car.parquet``
``gtfs_clean/<city>/``
    ``stops.parquet``          ``stop_idx:u32, stop_lat:f64, stop_lon:f64``
    ``trips.parquet``          ``trip_idx:u32, route_idx:u32``
    ``stop_times.parquet``     ``trip_idx, stop_idx, stop_sequence, arrival_secs, departure_secs`` (all u32)

All OSM extraction goes through the osmtools crate via the `mcr_py` bindings
(`load_osm_walking`/`load_osm_cycling`/`load_osm_driving`/`load_osm_pois`/
`load_osm_boundary`); the PBF is downloaded once per city with
`download_osm_data`.
"""

import datetime
import io
import json
import pathlib
import tomllib
import typing
import zoneinfo

import mcr_py
import mcr_py.utils.cache
import mcr_py.utils.geometa
import mcr_py.utils.logger
from mcr_py.gtfs.pipeline import prepare_gtfs
from mcr_py.osm.extraction import extract_networks, extract_pois
from mcr_py.osm.start_nodes import build_start_nodes
import polars as pl
from fsspec.implementations.http import HTTPFileSystem
from mcr_py.utils.geometa import GeoMeta, build_geometa

# H3 resolution for the start-node mapping. Matches the old `OSMData` default;
# `run_mcr5` fans out over exactly one start node per occupied cell.
H3_RESOLUTION = 9
# GTFS service window. Kept wide on purpose: the Rust PT step anchors trips at a
# clock time and runs to convergence, so we keep every cropped trip rather than
# pinning a single service day.
GTFS_TIME_START = "01.01.1970-00:00:00 +0200"
GTFS_TIME_END = "01.01.2050-00:00:00 +0200"



def fetch_gbfs_to_parquet(gbfs_url: str, target_path: pathlib.Path) -> None:
    """Snapshot a GBFS free-bike-status feed to parquet (raw, optional)."""
    fs = HTTPFileSystem()
    with fs.open(gbfs_url) as file:
        content = json.load(file)
        df = pl.read_json(io.StringIO(json.dumps(content["data"]["bikes"])))
    target_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(target_path)


def load_data_for_city(
    data_directory: pathlib.Path,
    city_name: str,
    city_name_german: str,
    city_name_german_alt: str,
    admin_level: int,
    crs_sink_name: str,
    gtfs_timestamp: str,
    timestamp: str,
    gbfs_url: typing.Union[str, None] = None,
    now: typing.Union[str, None] = None,
) -> None:
    """Produce every mcr-rust input layer for one city under `timestamp`."""
    if now is None:
        now = datetime.datetime.now(tz=zoneinfo.ZoneInfo("Europe/Berlin")).strftime(
            "%Y%m%d-%H%M%S"
        )

    base = data_directory / timestamp
    cache_path = base / "cache"
    graph_dir = base / "graph" / city_name
    start_nodes_dir = base / "start_nodes" / city_name
    gtfs_out_dir = base / "gtfs_clean" / city_name
    osm_path = base / "osm_raw"
    geometa_path = cache_path / f"{city_name}_geometa.json"
    gtfs_raw_zip = data_directory / f"gtfs_raw/{gtfs_timestamp}/latest.zip"
    gtfs_crop_zip = base / f"gtfs_clean/{city_name}.zip"
    gbfs_path = base / f"gbfs_raw/{city_name}_{now}.parquet"

    mcr_py.utils.logger.setup("INFO")
    cache_path.mkdir(parents=True, exist_ok=True)
    mcr_py.utils.cache.overwrite_tempdir(cache_path)

    osm_path.mkdir(parents=True, exist_ok=True)
    with mcr_py.utils.logger.Timed.info(f"Downloading PBF for {city_name_german_alt}"):
        mcr_py.download_osm_data(city_name_german_alt, str(osm_path), mcr_py.DownloadMode.Reuse)
    geometa = build_geometa(
        city_name_german,
        city_name_german_alt,
        admin_level,
        crs_sink_name,
        geometa_path,
        osm_path,
    )

    node_dfs, walking_graph = extract_networks(
        geometa, city_name_german_alt, osm_path, cache_path, graph_dir
    )
    extract_pois(
        geometa,
        city_name_german_alt,
        osm_path,
        cache_path,
        graph_dir,
        node_dfs["walking"].select(["osm_id", "lat", "long"]),
    )
    build_start_nodes(
        node_dfs, geometa, H3_RESOLUTION, city_name_german_alt.lower(), start_nodes_dir, walking_graph
    )

    prepare_gtfs(
        gtfs_raw_zip,
        gtfs_crop_zip,
        geometa,
        gtfs_out_dir,
        time_start=datetime.datetime.strptime(GTFS_TIME_START, "%d.%m.%Y-%H:%M:%S %z"),
        time_end=datetime.datetime.strptime(GTFS_TIME_END, "%d.%m.%Y-%H:%M:%S %z"),
    )

    if gbfs_url is not None:
        fetch_gbfs_to_parquet(gbfs_url, gbfs_path)

    mcr_py.utils.logger.rlog.info(f"Finished preparing data for {city_name_german}")


if __name__ == "__main__":
    with open(pathlib.Path(__file__).parent.resolve() / "config.toml", "rb") as f:
        settings = tomllib.load(f)

    data_path = pathlib.Path(__file__).parent.parent.resolve() / "data"
    for city in settings["city"]:
        load_data_for_city(
            data_directory=data_path,
            city_name=city,
            city_name_german=settings["city"][city]["german"],
            city_name_german_alt=settings["city"][city]["german_alt"],
            timestamp=settings["timestamp"]["timestamp"],
            admin_level=settings["city"][city]["admin_level"],
            crs_sink_name=settings["geography"]["crs_sink_name"],
            gtfs_timestamp=settings["gtfs"]["timestamp"],
            gbfs_url=settings["city"][city].get("gbfs_url", None),
        )
