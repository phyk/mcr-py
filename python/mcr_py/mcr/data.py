import os
import pathlib
from enum import Enum
from typing import Optional, Tuple, TypeVar

import polars as pl
import polars_h3 as plh3
import polars_st as st
import rustworkx as rx

from mcr_py._mcr_py import (
    load_osm_cycling,
    load_osm_driving,
    load_osm_pois,
    load_osm_walking,
)
from mcr_py.osm import graph
from mcr_py.utils.geometa import Buffering, GeoMeta
from mcr_py.utils.logger import Timed, rlog

ACCURACY = 2
ACCURACY_MULTIPLIER = 10 ** (ACCURACY - 1)

AVG_WALKING_SPEED = 1.4  # m/s
AVG_BIKING_SPEED = 4.0  # m/s
AVG_CAR_SPEED = 11.0  # m/s

N_TOTAL_WEIGHTS = 2  # time, cost
N_TOTAL_HIDDEN_WEIGHTS = 2  # biking time, public transport stops


class NetworkType(Enum):
    WALKING = "walking"
    CYCLING = "cycling"
    DRIVING = "driving"


class RedownloadMode(Enum):
    REDOWNLOAD = "redownload"
    OVERWRITE_NO_REDOWNLOAD = "overwrite"
    REUSE = "reuse"


class OSMData:
    def __init__(
        self,
        geo_meta: GeoMeta,
        city_id: str = "",
        osm_path: pathlib.Path = pathlib.Path(),
        cache_path: pathlib.Path = pathlib.Path(),
        resolution: int = 9,
        additional_network_types: Optional[list[NetworkType]] = None,
        redownload: RedownloadMode = RedownloadMode.REUSE,
    ) -> None:
        if additional_network_types is None:
            additional_network_types = []
        self.geo_meta = geo_meta
        self.city_id = city_id
        self.osm_path = osm_path
        self.cache_path = cache_path
        self.location_mapping = {}

        with Timed.info("Loading OSM walking"):
            self.osm_nodes, self.osm_edges, self.nxgraph = self.read_walking(
                redownload
                if (osm_path / f"{city_id.lower()}.osm.pbf").exists()
                else RedownloadMode.REDOWNLOAD
            )

        with Timed.info("Loading OSM POIs"):
            self.pois = self.read_pois(redownload)

        with Timed.info("Loading location mapping"):
            # This uses the pruned network
            self.resolution = resolution
            self.location_mapping[NetworkType.WALKING] = self.calculate_location_mapping(
                self.osm_nodes
            )

        self.additional_networks: dict[
            NetworkType, tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph]
        ] = {}

        for network_type in additional_network_types:
            with Timed.info(f"Loading OSM {network_type.value}"):
                (
                    osm_nodes,
                    osm_edges,
                    nxgraph,
                ) = self.read_network(network_type.value, redownload)
                self.additional_networks[network_type] = (
                    osm_nodes,
                    osm_edges,
                    nxgraph,
                )

            with Timed.info(f"Loading location mapping {network_type.value}"):
                self.location_mapping[network_type] = self.calculate_location_mapping(
                    osm_nodes
                )

    def read_walking(
        self, redownload: RedownloadMode
    ) -> tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph]:
        nodes_path = self.cache_path / f"{self.city_id.lower()}_walking_nodes.parquet"
        edges_path = self.cache_path / f"{self.city_id.lower()}_walking_edges.parquet"
        if (
            redownload != RedownloadMode.REUSE
            or not os.path.exists(nodes_path)
            or not os.path.exists(edges_path)
        ):
            (nodes, edges) = load_osm_walking(
                self.city_id,
                self.geo_meta.get_convex_hull_coord_list(),
                str(self.osm_path),
                str(self.cache_path),
                download=redownload == RedownloadMode.REDOWNLOAD,
            )
        else:
            nodes = pl.read_parquet(nodes_path)
            edges = pl.read_parquet(edges_path)

        nodes, edges, rxgraph = graph.create_rx_graph(nodes, edges)
        nodes, edges, rxgraph = graph.crop_graph_to_largest_component(rxgraph, nodes, edges)

        return nodes, edges, rxgraph

    def read_network(
        self, network_type: str, renewed: RedownloadMode
    ) -> tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph]:
        nodes_path = self.cache_path / f"{self.city_id.lower()}_{network_type}_nodes.parquet"
        edges_path = self.cache_path / f"{self.city_id.lower()}_{network_type}_edges.parquet"
        if (
            renewed != RedownloadMode.REUSE
            or not os.path.exists(nodes_path)
            or not os.path.exists(edges_path)
        ):
            match network_type:
                case "cycling":
                    (nodes, edges) = load_osm_cycling(
                        city_name=self.city_id,
                        geometry_vec=self.geo_meta.get_convex_hull_coord_list(),
                        reverse_edges=True,
                        archive_path=str(self.osm_path),
                        outpath=str(self.cache_path),
                        download=renewed == RedownloadMode.REDOWNLOAD,
                    )
                case "driving":
                    (nodes, edges) = load_osm_driving(
                        city_name=self.city_id,
                        geometry_vec=self.geo_meta.get_convex_hull_coord_list(),
                        archive_path=str(self.osm_path),
                        outpath=str(self.cache_path),
                        download=renewed == RedownloadMode.REDOWNLOAD,
                    )
                case _:
                    msg = "{} is not a valid network type".format(network_type)
                    raise ValueError(msg)
        else:
            nodes = pl.read_parquet(nodes_path)
            edges = pl.read_parquet(edges_path)

        nodes, edges, rxgraph = graph.create_rx_graph(nodes, edges)
        nodes, edges, rxgraph = graph.crop_graph_to_largest_component(rxgraph, nodes, edges)

        return nodes, edges, rxgraph

    def read_pois(self, renewed: RedownloadMode) -> pl.DataFrame:
        pois_path = self.cache_path / f"{self.city_id.lower()}_pois_nodes.parquet"
        if renewed != RedownloadMode.REUSE or not os.path.exists(pois_path):
            pois = load_osm_pois(
                city_name=self.city_id,
                geometry_vec=self.geo_meta.get_bounding_box_as_coord_list(),
                archive_path=str(self.osm_path),
                outpath=str(self.cache_path),
                download=renewed == RedownloadMode.REDOWNLOAD,
                nodes_to_match_df=self.osm_nodes,
            )  # type: ignore
        else:
            pois = pl.read_parquet(pois_path)
        return pois

    def calculate_location_mapping(self, osm_nodes: pl.DataFrame) -> pl.DataFrame:
        return (
            osm_nodes.lazy()
            .with_columns(
                plh3.latlng_to_cell(
                    pl.col("lat"),
                    pl.col("long"),
                    self.resolution,
                    return_dtype=pl.String,
                ).alias("h3_cell"),
                st.point(pl.concat_arr("long", "lat")).st.set_srid(4326).alias("point_lnglat"),
            )
            .with_columns(
                st.point(
                    pl.concat_arr(
                        plh3.cell_to_lng(pl.col("h3_cell")),
                        plh3.cell_to_lat(pl.col("h3_cell")),
                    )
                )
                .st.set_srid(4326)
                .alias("h3_cell_lnglat")
            )
            .with_columns(
                st.to_srid("point_lnglat", srid=4839)
                .st.distance(st.to_srid("h3_cell_lnglat", srid=4839))
                .alias("distance_to_cell"),
            )
            .group_by("h3_cell")
            .agg(pl.all().sort_by("distance_to_cell").first())
            .select("osm_id", "h3_cell")
            .filter(
                st.point(
                    pl.concat_arr(
                        plh3.cell_to_lng(pl.col("h3_cell")),
                        plh3.cell_to_lat(pl.col("h3_cell")),
                    )
                )
                .st.set_srid(4326)
                .st.within(
                    st.polygon(
                        pl.lit(
                            [
                                self.geo_meta.get_convex_hull_coord_list(
                                    buffering=Buffering.UNBUFFERED
                                )
                            ]
                        )
                    ).st.set_srid(4326)
                ),
            )
            .collect()
        )


def create_walking_graph(
    osm_nodes: pl.DataFrame, osm_edges: pl.DataFrame
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    walking_edges = add_travel_time(osm_edges, AVG_WALKING_SPEED)

    return osm_nodes, walking_edges


DRIVING_PREFIX = "D"
WALKING_PREFIX = "W"


def create_multi_modal_graph(
    walking_osm_nodes: pl.DataFrame,
    driving_osm_nodes: pl.DataFrame,
    driving_osm_edges: pl.DataFrame,
    avg_driving_speed: float,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    # bike start
    driving_osm_nodes = prefix_id(driving_osm_nodes, DRIVING_PREFIX, "osm_id", save_old=True)
    driving_osm_edges = prefix_id(driving_osm_edges, DRIVING_PREFIX, "source_osm")
    driving_osm_edges = prefix_id(driving_osm_edges, DRIVING_PREFIX, "dest_osm")

    driving_osm_edges = add_travel_time(driving_osm_edges, avg_driving_speed)
    driving_osm_edges = driving_osm_edges.with_columns(
        pl.col(TRAVEL_TIME_COLUMN).alias(TRAVEL_TIME_DRIVING_COLUMN)
    )
    # bike end

    # walking start
    walking_osm_nodes = prefix_id(walking_osm_nodes, WALKING_PREFIX, "osm_id", save_old=True)
    # walking end

    transfer_edges = create_transfer_edges(walking_osm_nodes, driving_osm_nodes)

    multi_modal_nodes = combine_nodes(walking_osm_nodes, driving_osm_nodes)
    multi_modal_edges = combine_edges(driving_osm_edges, transfer_edges, multi_modal_nodes)

    return multi_modal_nodes, multi_modal_edges


TRAVEL_TIME_COLUMN = "travel_time"
TRAVEL_TIME_DRIVING_COLUMN = "travel_time_driving"


def add_travel_time(edges: pl.DataFrame, speed: float) -> pl.DataFrame:
    edges = edges.with_columns(
        (pl.col("length") / speed * ACCURACY_MULTIPLIER)
        .round()
        .cast(pl.UInt64)
        .alias(TRAVEL_TIME_COLUMN)
    )
    return edges


def combine_edges(
    bike_edges: pl.DataFrame,
    transfer_edges: pl.DataFrame,
    multi_modal_nodes: pl.DataFrame,
) -> pl.DataFrame:
    edges = pl.concat(
        [
            bike_edges.drop(["source_rx_node_id", "dest_rx_node_id"]),
            transfer_edges,
        ],
        how="diagonal",
    )

    edges = (
        edges.join(
            multi_modal_nodes.select(pl.col("id").alias("source_id"), pl.col("osm_id")),
            how="left",
            left_on="source_osm",
            right_on="osm_id",
        )
        .join(
            multi_modal_nodes.select(pl.col("id").alias("dest_id"), pl.col("osm_id")),
            how="left",
            left_on="dest_osm",
            right_on="osm_id",
        )
        .with_columns(
            pl.col("source_id").alias("source_osm"), pl.col("dest_id").alias("dest_osm")
        )
    )
    if len(edges.drop_nans().drop_nulls()) != len(edges):
        msg = "Error in concatenating edges"
        raise ValueError(msg)

    return edges


def combine_nodes(walking_nodes: pl.DataFrame, driving_nodes: pl.DataFrame) -> pl.DataFrame:
    if "has_bicycle" in driving_nodes.columns:
        walking_nodes = walking_nodes.with_columns(
            pl.lit(False).alias("has_bicycle"),  # noqa: FBT003
        )
    df = pl.concat([walking_nodes, driving_nodes], how="diagonal")
    df = df.with_row_index(name="id").drop("rx_node_id")
    if len(df.drop_nans().drop_nulls()) != len(df):
        msg = "Error in concatenating nodes"
        raise ValueError(msg)
    return df


A = TypeVar("A")
B = TypeVar("B")


def get_reverse_map(d: dict[A, B]) -> dict[B, A]:
    return {v: k for k, v in d.items()}


def add_id_column(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_row_index(name="id")


def prefix_id(
    gdf: pl.DataFrame,
    prefix: str,
    column: str,
    save_old: bool = False,  # noqa: FBT001, FBT002
) -> pl.DataFrame:
    if save_old:
        gdf = gdf.with_columns(pl.col(column).alias(f"{column}_old"))
    gdf = gdf.with_columns(pl.lit(prefix).alias(column) + pl.col(column).cast(pl.String))

    return gdf


def create_transfer_edges(
    walking_nodes: pl.DataFrame, driving_nodes: pl.DataFrame
) -> pl.DataFrame:
    intersection_node_ids = walking_nodes.with_columns(
        pl.col("osm_id").alias("walking_id"),
        pl.col("osm_id").str.strip_chars_start(WALKING_PREFIX).cast(pl.UInt64),
    ).join(
        driving_nodes.with_columns(
            pl.col("osm_id").alias("driving_id"),
            pl.col("osm_id").str.strip_chars_start(DRIVING_PREFIX).cast(pl.UInt64),
        ),
        on="osm_id",
    )
    rlog.debug(f"Found {len(intersection_node_ids)} intersection nodes")
    transfer_edges = intersection_node_ids.select(
        pl.col("driving_id").cast(pl.String).alias("source_osm"),
        pl.col("walking_id").cast(pl.String).alias("dest_osm"),
        pl.lit(0.0).alias("length"),
        pl.lit(0).cast(pl.UInt64).alias("travel_time"),
        pl.lit(0).cast(pl.UInt64).alias("travel_time_driving"),
    )

    return transfer_edges


def add_weights(edges: pl.DataFrame, columns: list[str], hidden: bool = False) -> pl.DataFrame:  # noqa: FBT001, FBT002
    col_name = "hidden_weights" if hidden else "weights"
    n_padding = N_TOTAL_HIDDEN_WEIGHTS if hidden else N_TOTAL_WEIGHTS

    edges = edges.with_columns(
        (
            pl.concat_list(
                pl.col(columns),
                *[pl.lit(0, dtype=pl.UInt64) for _ in range(n_padding - len(columns))],
            )
        ).alias(col_name)
    )

    return edges


def to_mlc_edges(edges: pl.DataFrame) -> list[tuple]:
    return edges.select(
        pl.col("source_osm"), pl.col("dest_osm"), pl.col(["weights", "hidden_weights"])
    ).rows()
