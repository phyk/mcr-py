from typing import List, Optional, Tuple

import polars as pl

class WalkingConfig:
    def __init__(self, speed_kmh: int, time_only_dominance: bool = False) -> None: ...

class PrivateModeConfig:
    def __init__(
        self,
        speed_kmh: int,
        switch_time_s: int,
        price_function_base: int,
    ) -> None: ...

class PriceFunction:
    def __init__(
        self,
        unlock_fee: int,
        interval_minutes: int,
        price_per_interval: int,
        first_interval_free: bool,
    ) -> None: ...

class SharedMicromobileConfig:
    def __init__(
        self,
        speed_kmh: int,
        switch_time_s: int,
        price_function: PriceFunction,
        mode: str = "shared_bicycle",
    ) -> None: ...

class PublicTransportConfig:
    def __init__(
        self,
        data_dir: str,
        max_snap_distance_m: float = 20.0,
        anchor_time_secs: int = 28800,
        flex_window_secs: int = 600,
        short_trip_fare_cents: int = 220,
        short_trip_max_stops: int = 4,
        long_trip_fare_cents: int = 320,
        switch_time_secs: int = 0,
    ) -> None: ...

class MCRConfig:
    def __init__(
        self,
        out_dir: str,
        walking: Optional[WalkingConfig] = None,
        cycling: Optional[PrivateModeConfig] = None,
        car: Optional[PrivateModeConfig] = None,
        shared_micromobile: Optional[List[SharedMicromobileConfig]] = None,
        public_transport: Optional[PublicTransportConfig] = None,
        enable_limit: bool = False,
    ) -> None: ...

class MCRGraph:
    def node_count(self) -> int: ...

class MCRGraphBuilder:
    def __init__(self) -> None: ...
    def add_walking_nodes(self, path: str) -> "MCRGraphBuilder": ...
    def add_walking_edges(self, path: str) -> "MCRGraphBuilder": ...
    def add_cycling_nodes(self, path: str) -> "MCRGraphBuilder": ...
    def add_cycling_edges(self, path: str) -> "MCRGraphBuilder": ...
    def add_car_nodes(self, path: str) -> "MCRGraphBuilder": ...
    def add_car_edges(self, path: str) -> "MCRGraphBuilder": ...
    def add_poi_nodes(self, path: str) -> "MCRGraphBuilder": ...
    def add_shared_bike_stations(self, path: str) -> "MCRGraphBuilder": ...
    def add_shared_bike_dropoff_zones(self, path: str) -> "MCRGraphBuilder": ...
    def add_shared_scooter_stations(self, path: str) -> "MCRGraphBuilder": ...
    def add_shared_scooter_dropoff_zones(self, path: str) -> "MCRGraphBuilder": ...
    def build(self) -> MCRGraph: ...

class DownloadMode:
    LocalOnly: "DownloadMode"
    Overwrite: "DownloadMode"
    Reuse: "DownloadMode"
    Error: "DownloadMode"

def run_mcr5(start_nodes: str, config: MCRConfig, graph: MCRGraph) -> None: ...
def log_something() -> None: ...
def load_osm_cycling(
    city_name: str,
    geometry_vec: List[Tuple[float, float]],
    reverse_edges: bool,
    archive_path: str,
    outpath: str,
    mode: DownloadMode,
) -> Tuple[pl.DataFrame, pl.DataFrame]: ...
def load_osm_walking(
    city_name: str,
    geometry_vec: List[Tuple[float, float]],
    archive_path: str,
    outpath: str,
    mode: DownloadMode,
) -> Tuple[pl.DataFrame, pl.DataFrame]: ...
def load_osm_driving(
    city_name: str,
    geometry_vec: List[Tuple[float, float]],
    archive_path: str,
    outpath: str,
    mode: DownloadMode,
) -> Tuple[pl.DataFrame, pl.DataFrame]: ...
def download_osm_data(city_name: str, archive_path: str, mode: DownloadMode) -> str: ...
def load_osm_boundary(
    city_name: str,
    name_filter: str,
    admin_level: str,
    archive_path: str,
    mode: DownloadMode,
) -> List[Tuple[List[Tuple[float, float]], List[List[Tuple[float, float]]]]]: ...
def load_osm_pois(
    city_name: str,
    geometry_vec: List[Tuple[float, float]],
    archive_path: str,
    outpath: str,
    mode: DownloadMode,
    nodes_to_match_df: Optional[pl.DataFrame] = None,
    nodes_to_match_path: Optional[str] = None,
) -> pl.DataFrame: ...
def add_nearest_node_to_df(
    geo_df: pl.DataFrame,
    nodes_to_match: pl.DataFrame,
    target_crs: int,
) -> pl.DataFrame: ...
