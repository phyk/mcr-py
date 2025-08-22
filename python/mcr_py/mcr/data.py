import os
from enum import Enum
from typing import Tuple, TypeVar

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
from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import Timed, rlog

ACCURACY = 1
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
    OVERWRITE_NOREDOWNLOAD = "overwrite"
    REUSE = "reuse"


class OSMData:
    def __init__(
        self,
        geo_meta: GeoMeta,
        city_id: str = "",
        osm_path: str = "",
        cache_path: str = "",
        resolution: int = 8,
        additional_network_types: list[NetworkType] = [],
        redownload: RedownloadMode = RedownloadMode.REUSE,
    ):
        self.geo_meta = geo_meta
        self.city_id = city_id
        self.osm_path = osm_path
        self.cache_path = cache_path

        if not os.path.exists(f"{osm_path}/{city_id.lower()}.osm.pbf"):
            redownload = RedownloadMode.REDOWNLOAD

        with Timed.info("Loading OSM walking"):
            self.osm_nodes, self.osm_edges, self.nxgraph = self.read_walking(redownload)

        with Timed.info("Loading OSM POIs"):
            self.pois = self.read_pois(redownload)

        with Timed.info("Loading location mapping"):
            # This uses the pruned network
            self.resolution = resolution
            self.calculate_location_mapping()

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

    def read_walking(self, redownload: RedownloadMode):
        nodes_path = f"{self.cache_path}/{self.city_id.lower()}_walking_nodes.parquet"
        edges_path = f"{self.cache_path}/{self.city_id.lower()}_walking_edges.parquet"
        if (
            redownload != RedownloadMode.REUSE
            or not os.path.exists(nodes_path)
            or not os.path.exists(edges_path)
        ):
            (nodes, edges) = load_osm_walking(
                self.city_id,
                self.geo_meta.get_convex_hull_coord_list(),
                self.osm_path,
                self.cache_path,
                download=redownload == RedownloadMode.REDOWNLOAD,
            )
        else:
            nodes = pl.read_parquet(nodes_path)
            edges = pl.read_parquet(edges_path)

        nodes, edges, rxgraph = graph.create_rx_graph(nodes, edges)
        nodes, edges, rxgraph = graph.crop_graph_to_largest_component(
            rxgraph, nodes, edges
        )

        return nodes, edges, rxgraph

    def read_network(
        self, network_type: str, renewed: RedownloadMode
    ) -> tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph]:
        nodes_path = (
            f"{self.cache_path}/{self.city_id.lower()}_{network_type}_nodes.parquet"
        )
        edges_path = (
            f"{self.cache_path}/{self.city_id.lower()}_{network_type}_edges.parquet"
        )
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
                        archive_path=self.osm_path,
                        outpath=self.cache_path,
                        download=renewed == RedownloadMode.REDOWNLOAD,
                    )
                case "driving":
                    (nodes, edges) = load_osm_driving(
                        city_name=self.city_id,
                        geometry_vec=self.geo_meta.get_convex_hull_coord_list(),
                        archive_path=self.osm_path,
                        outpath=self.cache_path,
                        download=renewed == RedownloadMode.REDOWNLOAD,
                    )
                case _:
                    raise ValueError(
                        "{} is not a valid network type".format(network_type)
                    )
        else:
            nodes = pl.read_parquet(nodes_path)
            edges = pl.read_parquet(edges_path)

        nodes, edges, rxgraph = graph.create_rx_graph(nodes, edges)
        nodes, edges, rxgraph = graph.crop_graph_to_largest_component(
            rxgraph, nodes, edges
        )

        return nodes, edges, rxgraph

    def read_pois(self, renewed: RedownloadMode) -> pl.DataFrame:
        pois_path = f"{self.cache_path}/{self.city_id.lower()}_pois_nodes.parquet"
        if renewed != RedownloadMode.REUSE or not os.path.exists(pois_path):
            pois = load_osm_pois(
                city_name=self.city_id,
                geometry_vec=self.geo_meta.get_bounding_box_as_coord_list(),
                archive_path=self.osm_path,
                outpath=self.cache_path,
                download=renewed == RedownloadMode.REDOWNLOAD,
                nodes_to_match_df=self.osm_nodes,
            )  # type: ignore
        else:
            pois = pl.read_parquet(pois_path)
        return pois

    def calculate_location_mapping(self):
        self.location_mapping = (
            self.osm_nodes.lazy()
            .with_columns(
                plh3.latlng_to_cell(
                    pl.col("lat"),
                    pl.col("long"),
                    self.resolution,
                    return_dtype=pl.String,
                ).alias("h3_cell"),
                st.point(pl.concat_arr("long", "lat"))
                .st.set_srid(4326)
                .alias("point_lnglat"),
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
                            [self.geo_meta.get_convex_hull_coord_list(use_buffer=False)]
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
    walking_osm_edges: pl.DataFrame,
    driving_osm_nodes: pl.DataFrame,
    driving_osm_edges: pl.DataFrame,
    avg_driving_speed: float,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    # bike start
    driving_osm_nodes = prefix_id(
        driving_osm_nodes, DRIVING_PREFIX, "osm_id", save_old=True
    )
    driving_osm_edges = prefix_id(driving_osm_edges, DRIVING_PREFIX, "source_osm")
    driving_osm_edges = prefix_id(driving_osm_edges, DRIVING_PREFIX, "dest_osm")

    driving_osm_edges = add_travel_time(driving_osm_edges, avg_driving_speed)
    driving_osm_edges = driving_osm_edges.with_columns(
        pl.col(TRAVEL_TIME_COLUMN).alias(TRAVEL_TIME_DRIVING_COLUMN)
    )
    # bike end

    # walking start
    walking_osm_nodes = prefix_id(
        walking_osm_nodes, WALKING_PREFIX, "osm_id", save_old=True
    )
    walking_osm_edges = prefix_id(walking_osm_edges, WALKING_PREFIX, "source_osm")
    walking_osm_edges = prefix_id(walking_osm_edges, WALKING_PREFIX, "dest_osm")

    walking_osm_edges = add_travel_time(walking_osm_edges, AVG_WALKING_SPEED)
    # walking end

    transfer_edges = create_transfer_edges(walking_osm_nodes, driving_osm_nodes)

    multi_modal_edges = combine_edges(
        walking_osm_edges, driving_osm_edges, transfer_edges
    )
    multi_modal_nodes = pl.concat([walking_osm_nodes, driving_osm_nodes])
    return multi_modal_nodes, multi_modal_edges


TRAVEL_TIME_COLUMN = "travel_time"
TRAVEL_TIME_DRIVING_COLUMN = "travel_time_driving"


def add_travel_time(edges: pl.DataFrame, speed: float) -> pl.DataFrame:
    edges = edges.with_columns((pl.col("length") / speed).alias(TRAVEL_TIME_COLUMN))
    return edges


def combine_edges(
    walking_edges: pl.DataFrame,
    bike_edges: pl.DataFrame,
    transfer_edges: pl.DataFrame,
) -> pl.DataFrame:
    edges = pl.concat([walking_edges, bike_edges, transfer_edges], how="vertical")

    # fill travel_time for transfer edges and
    # travel_time_bike for walking and transfer edges
    edges = edges.fill_nan(0)

    return edges


A = TypeVar("A")
B = TypeVar("B")


def get_reverse_map(d: dict[A, B]) -> dict[B, A]:
    return {v: k for k, v in d.items()}


def add_id_column(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_row_index(name="id")


def reset_node_ids(df: pl.DataFrame, id_df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.join(id_df, left_on="source_osm", right_on="osm_id")
        .with_columns(pl.col("id").alias("source_osm"))
        .join(id_df, left_on="dest_osm", right_on="osm_id")
        .with_columns(pl.col("id").alias("dest_osm"))
    )


def prefix_id(
    gdf: pl.DataFrame, prefix: str, column: str, save_old=False
) -> pl.DataFrame:
    if save_old:
        gdf = gdf.with_columns(pl.col(column).alias(f"{column}_old"))
    gdf = gdf.select(prefix + pl.col(column).cast(pl.String))

    return gdf


def create_transfer_edges(walking_nodes: pl.DataFrame, driving_nodes: pl.DataFrame):
    intersection_node_ids = walking_nodes.select(
        pl.col("osm_id").alias("walking_id")
    ).join(
        driving_nodes.select(pl.col("osm_id").alias("driving_id")),
        left_on="walking_id",
        right_on="driving_id",
    )
    rlog.debug(f"Found {len(intersection_node_ids)} intersection nodes")
    transfer_edges = intersection_node_ids.select(
        "D" + pl.col("driving_id").alias("source_osm").cast(pl.String),
        "W" + pl.col("walking_id").alias("dest_osm").cast(pl.String),
        pl.lit(0).alias("length"),
    )

    return transfer_edges


def add_weights(edges: pl.DataFrame, columns: list[str], hidden=False) -> pl.DataFrame:
    col_name = "hidden_weights" if hidden else "weights"
    n_padding = N_TOTAL_HIDDEN_WEIGHTS if hidden else N_TOTAL_WEIGHTS

    mid_seperator = "," if len(columns) > 0 and n_padding > 0 else ""
    expr = pl.lit("(")
    if len(columns) > 0:
        expr += pl.concat_str(
            (pl.col(columns).round(1) * 1).cast(int).cast(str), separator=","
        ).alias(col_name)
    edges = edges.with_columns(
        (
            expr
            + pl.lit(mid_seperator + ",".join(["0"] * (n_padding - len(columns))) + ")")
        ).alias(col_name)
    )

    return edges


def to_mlc_edges(edges: pl.DataFrame) -> list[tuple]:
    # type: ignore
    return edges.select(
        pl.col("source_osm"), pl.col("dest_osm"), pl.col(["weights", "hidden_weights"])
    ).rows()
