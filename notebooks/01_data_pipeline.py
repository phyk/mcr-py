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
    ``routes.parquet``         ``route_idx:u32, fare_cents:u32``
    ``trips.parquet``          ``trip_idx:u32, route_idx:u32``
    ``stop_times.parquet``     ``trip_idx, stop_idx, stop_sequence, arrival_secs, departure_secs`` (all u32)

All OSM extraction goes through the osmtools crate via the `mcr_py` bindings
(`load_osm_walking`/`load_osm_cycling`/`load_osm_driving`/`load_osm_pois`); the
PBF is downloaded once per city with `download_osm_data`.
"""

import datetime
import io
import json
import pathlib
import tomllib
import typing
import zoneinfo

import mcr_py
import mcr_py.gtfs.clean
import mcr_py.gtfs.crop
import mcr_py.overpass.query
import mcr_py.utils.cache
import mcr_py.utils.geometa
import mcr_py.utils.key
import mcr_py.utils.logger
import polars as pl
import polars_h3 as plh3
import polars_st as st
from fsspec.implementations.http import HTTPFileSystem
from mcr_py.utils.geometa import Buffering, GeoMeta

# H3 resolution for the start-node mapping. Matches the old `OSMData` default;
# `run_mcr5` fans out over exactly one start node per occupied cell.
H3_RESOLUTION = 9
# Fare written to every route. `TransitData::load` does not consume it (fare is
# charged per stops travelled), but the column keeps the schema aligned with
# `mcr-rust/scripts/download_delijn_gtfs.py`.
DEFAULT_FARE_CENTS = 300
CRS_SOURCE = "EPSG:4326"
# GTFS service window. Kept wide on purpose: the Rust PT step anchors trips at a
# clock time and runs to convergence, so we keep every cropped trip rather than
# pinning a single service day.
GTFS_TIME_START = "01.01.1970-00:00:00 +0200"
GTFS_TIME_END = "01.01.2050-00:00:00 +0200"


def crs_to_srid(crs: str) -> int:
    """Turn an ``EPSG:1234`` string into the integer SRID 1234."""
    return int(crs.split(":")[-1])


def build_geometa(
    city_name_german: str,
    admin_level: int,
    crs_sink_name: str,
    geometa_path: pathlib.Path,
) -> GeoMeta:
    """Fetch the city boundary from Overpass and persist a `GeoMeta`."""
    boundary_polygon = mcr_py.overpass.query.fetch_boundary_polygon(
        city_name_german, admin_level
    )
    geometa = GeoMeta.create(boundary_polygon, CRS_SOURCE, crs_sink_name)
    geometa.save(geometa_path)
    return geometa


def ensure_pbf_downloaded(city_id: str, osm_path: pathlib.Path) -> None:
    """Download the city PBF once; reused by every network extraction below."""
    osm_path.mkdir(parents=True, exist_ok=True)
    pbf_file = osm_path / f"{city_id.lower()}.osm.pbf"
    if pbf_file.exists():
        mcr_py.utils.logger.rlog.info(f"Reusing existing PBF at {pbf_file}")
        return
    with mcr_py.utils.logger.Timed.info(f"Downloading PBF for {city_id}"):
        mcr_py.download_osm_data(city_id, str(osm_path))


def extract_networks(
    geometa: GeoMeta,
    city_id: str,
    osm_path: pathlib.Path,
    cache_path: pathlib.Path,
    graph_dir: pathlib.Path,
) -> dict[str, pl.DataFrame]:
    """Extract walking/cycling/driving networks and write them for mcr-rust.

    Returns the node DataFrames keyed by mcr-rust layer name (``walking`` /
    ``car``) so the start-node mapping can be computed without re-reading.
    """
    graph_dir.mkdir(parents=True, exist_ok=True)
    cache_path.mkdir(parents=True, exist_ok=True)
    hull = geometa.get_convex_hull_coord_list()
    archive = str(osm_path)
    out = str(cache_path)

    with mcr_py.utils.logger.Timed.info("Extracting walking network"):
        walking_nodes, walking_edges = mcr_py.load_osm_walking(
            city_id, hull, archive, out, download=False
        )
        walking_nodes.write_parquet(graph_dir / "walking_nodes.parquet")
        walking_edges.write_parquet(graph_dir / "walking_edges.parquet")

    with mcr_py.utils.logger.Timed.info("Extracting cycling network"):
        cycling_nodes, cycling_edges = mcr_py.load_osm_cycling(
            city_id,
            hull,
            reverse_edges=True,
            archive_path=archive,
            outpath=out,
            download=False,
        )
        cycling_nodes.write_parquet(graph_dir / "cycling_nodes.parquet")
        cycling_edges.write_parquet(graph_dir / "cycling_edges.parquet")

    with mcr_py.utils.logger.Timed.info("Extracting driving network"):
        driving_nodes, driving_edges = mcr_py.load_osm_driving(
            city_id, hull, archive, out, download=False
        )
        # mcr-rust's car layer is osmtools' driving network.
        driving_nodes.write_parquet(graph_dir / "car_nodes.parquet")
        driving_edges.write_parquet(graph_dir / "car_edges.parquet")

    return {"walking": walking_nodes, "car": driving_nodes}


def extract_pois(
    geometa: GeoMeta,
    city_id: str,
    osm_path: pathlib.Path,
    cache_path: pathlib.Path,
    graph_dir: pathlib.Path,
    walking_nodes: pl.DataFrame,
) -> None:
    """Extract POIs, snapping each to its nearest walking node.

    POIs are matched against `walking_nodes`, so every ``nearest_osm_node`` is
    guaranteed to exist in the assembled graph — `add_poi_to_walking_graph`
    panics otherwise.
    """
    with mcr_py.utils.logger.Timed.info("Extracting POIs"):
        pois = mcr_py.load_osm_pois(
            city_id,
            geometa.get_bounding_box_as_coord_list(),
            str(osm_path),
            str(cache_path),
            download=False,
            nodes_to_match_df=walking_nodes,
            nodes_to_match_path=None,
        )
        pois.write_parquet(graph_dir / "poi_nodes.parquet")


def calculate_start_nodes(
    nodes: pl.DataFrame, geometa: GeoMeta, resolution: int
) -> pl.DataFrame:
    """Pick one representative OSM node per H3 cell inside the city boundary.

    For every occupied cell we keep the node closest to the cell centre (in the
    projected sink CRS), then drop cells whose centre falls outside the
    unbuffered city boundary. Output columns are what `read_start_nodes`
    requires (``osm_node_id``, ``h3_cell_id``) plus diagnostics.
    """
    srid = crs_to_srid(geometa.crs_target)
    boundary = pl.lit([geometa.get_convex_hull_coord_list(buffering=Buffering.UNBUFFERED)])
    return (
        nodes.lazy()
        .with_columns(
            plh3.latlng_to_cell(
                pl.col("lat"), pl.col("long"), resolution, return_dtype=pl.String
            ).alias("h3_cell_id"),
            st.point(pl.concat_arr("long", "lat")).st.set_srid(4326).alias("node_pt"),
        )
        .with_columns(
            plh3.cell_to_lat("h3_cell_id").alias("center_lat"),
            plh3.cell_to_lng("h3_cell_id").alias("center_lon"),
        )
        .with_columns(
            st.point(pl.concat_arr("center_lon", "center_lat"))
            .st.set_srid(4326)
            .alias("center_pt")
        )
        .with_columns(
            st.to_srid("node_pt", srid=srid)
            .st.distance(st.to_srid("center_pt", srid=srid))
            .alias("dist")
        )
        .group_by("h3_cell_id")
        .agg(pl.all().sort_by("dist").first())
        .filter(
            st.point(pl.concat_arr("center_lon", "center_lat"))
            .st.set_srid(4326)
            .st.within(st.polygon(boundary).st.set_srid(4326))
        )
        .select(
            pl.col("osm_id").alias("osm_node_id"),
            pl.col("dist"),
            pl.col("lat"),
            pl.col("long").alias("lon"),
            pl.col("center_lat"),
            pl.col("center_lon"),
            pl.col("h3_cell_id"),
        )
        .collect()
    )


def build_start_nodes(
    node_dfs: dict[str, pl.DataFrame],
    geometa: GeoMeta,
    resolution: int,
    start_nodes_dir: pathlib.Path,
) -> None:
    """Write the walking and car start-node parquets."""
    start_nodes_dir.mkdir(parents=True, exist_ok=True)
    for layer, nodes in node_dfs.items():
        with mcr_py.utils.logger.Timed.info(f"Mapping start nodes ({layer})"):
            mapping = calculate_start_nodes(nodes, geometa, resolution)
            mapping.write_parquet(start_nodes_dir / f"{layer}.parquet")
            mcr_py.utils.logger.rlog.info(
                f"{layer}: {len(mapping)} start nodes across H3 cells"
            )


def gtfs_time_to_secs_expr(column: str) -> pl.Expr:
    """Parse a GTFS ``HH:MM:SS`` clock string into seconds since midnight.

    Hours may exceed 23 (services past midnight), so this stays string-based.
    `.str.head(2)` on the seconds field tolerates a fractional tail that polars
    may append when a column was inferred as ``Time``.
    """
    parts = pl.col(column).cast(pl.String).str.strip_chars().str.split(":")
    return (
        parts.list.get(0).cast(pl.Int64) * 3600
        + parts.list.get(1).cast(pl.Int64) * 60
        + parts.list.get(2).str.head(2).cast(pl.Int64)
    ).cast(pl.UInt32)


def gtfs_to_indexed_parquet(dfs: dict[str, pl.DataFrame], out_dir: pathlib.Path) -> None:
    """Convert cleaned GTFS DataFrames into the integer-indexed parquet layout.

    Mirrors `mcr-rust/scripts/download_delijn_gtfs.py`: stops/routes/trips get
    dense ``*_idx`` columns and stop times are expanded to seconds. Route ids
    are taken from the cleaned (direction/path-split) trips.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    trips_df = dfs[mcr_py.utils.key.TRIPS_KEY]
    stop_times_df = dfs[mcr_py.utils.key.STOP_TIMES_KEY]
    stops_df = dfs[mcr_py.utils.key.STOPS_KEY]

    stops_out = (
        stops_df.select("stop_id", "stop_lat", "stop_lon")
        .unique(subset="stop_id", maintain_order=True)
        .sort("stop_id")
        .with_row_index("stop_idx")
        .with_columns(pl.col("stop_idx").cast(pl.UInt32))
    )

    routes_idx = (
        trips_df.select("route_id")
        .unique(maintain_order=True)
        .sort("route_id")
        .with_row_index("route_idx")
        .with_columns(pl.col("route_idx").cast(pl.UInt32))
    )

    trips_idx = (
        trips_df.select("trip_id", "route_id")
        .unique(subset="trip_id", maintain_order=True)
        .join(routes_idx, on="route_id", how="inner")
        .sort("trip_id")
        .with_row_index("trip_idx")
        .with_columns(pl.col("trip_idx").cast(pl.UInt32))
    )

    routes_out = routes_idx.select(
        "route_idx", pl.lit(DEFAULT_FARE_CENTS).cast(pl.UInt32).alias("fare_cents")
    )
    trips_out = trips_idx.select("trip_idx", "route_idx")

    stop_times_out = (
        stop_times_df.select(
            "trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"
        )
        .join(trips_idx.select("trip_id", "trip_idx"), on="trip_id", how="inner")
        .join(stops_out.select("stop_id", "stop_idx"), on="stop_id", how="inner")
        .with_columns(
            pl.coalesce(
                gtfs_time_to_secs_expr("arrival_time"),
                gtfs_time_to_secs_expr("departure_time"),
            ).alias("arrival_secs"),
            pl.coalesce(
                gtfs_time_to_secs_expr("departure_time"),
                gtfs_time_to_secs_expr("arrival_time"),
            ).alias("departure_secs"),
            pl.col("stop_sequence").cast(pl.UInt32),
        )
        .select("trip_idx", "stop_idx", "stop_sequence", "arrival_secs", "departure_secs")
        .drop_nulls()
        .sort(["trip_idx", "stop_sequence"])
    )

    stops_out.select("stop_idx", "stop_lat", "stop_lon").write_parquet(
        out_dir / "stops.parquet"
    )
    routes_out.write_parquet(out_dir / "routes.parquet")
    trips_out.write_parquet(out_dir / "trips.parquet")
    stop_times_out.write_parquet(out_dir / "stop_times.parquet")
    mcr_py.utils.logger.rlog.info(
        f"GTFS: {len(stops_out)} stops, {len(routes_out)} routes, "
        f"{len(trips_out)} trips, {len(stop_times_out)} stop times"
    )


def prepare_gtfs(
    gtfs_raw_zip: pathlib.Path,
    gtfs_crop_zip: pathlib.Path,
    geometa: GeoMeta,
    gtfs_out_dir: pathlib.Path,
) -> None:
    """Crop the raw GTFS to the city, clean it, and write indexed parquets."""
    time_start = datetime.datetime.strptime(GTFS_TIME_START, "%d.%m.%Y-%H:%M:%S %z")
    time_end = datetime.datetime.strptime(GTFS_TIME_END, "%d.%m.%Y-%H:%M:%S %z")

    gtfs_crop_zip.parent.mkdir(parents=True, exist_ok=True)
    with mcr_py.utils.logger.Timed.info("Cropping GTFS to city boundary"):
        mcr_py.gtfs.crop.crop(
            gtfs_raw_zip,
            gtfs_crop_zip,
            geometa,
            time_start=time_start,
            time_end=time_end,
        )
    with mcr_py.utils.logger.Timed.info("Cleaning GTFS data"):
        dfs = mcr_py.gtfs.clean.clean(gtfs_crop_zip)
    with mcr_py.utils.logger.Timed.info("Writing indexed GTFS parquets"):
        gtfs_to_indexed_parquet(dfs, gtfs_out_dir)


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

    geometa = build_geometa(city_name_german, admin_level, crs_sink_name, geometa_path)

    ensure_pbf_downloaded(city_name_german_alt, osm_path)
    node_dfs = extract_networks(geometa, city_name_german_alt, osm_path, cache_path, graph_dir)
    extract_pois(
        geometa, city_name_german_alt, osm_path, cache_path, graph_dir, node_dfs["walking"]
    )
    build_start_nodes(node_dfs, geometa, H3_RESOLUTION, start_nodes_dir)

    prepare_gtfs(gtfs_raw_zip, gtfs_crop_zip, geometa, gtfs_out_dir)

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
