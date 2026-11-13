"""Snap GTFS stops to walking-network nodes (the matching itself runs in Rust)."""

import pathlib

import polars as pl

from mcr_py._mcr_py import match_gtfs_stops_to_nodes
from mcr_py.utils.logger import rlog

STOP_NODES_FILE = "stop_nodes.parquet"


def write_stop_nodes(
    gtfs_dir: pathlib.Path,
    walking_nodes: pl.DataFrame,
    max_snap_distance_m: float,
) -> pathlib.Path:
    """Write ``<gtfs_dir>/stop_nodes.parquet`` (stop_idx, osm_id, snap_dist_m).

    Each stop of ``<gtfs_dir>/stops.parquet`` is matched to its nearest node in
    ``walking_nodes`` (columns ``osm_id``, ``lat``, ``long``) within
    ``max_snap_distance_m``; stops with no node in range are left out. mcr-rust
    reads this file at routing time instead of matching on every load.
    """
    stops = pl.read_parquet(gtfs_dir / "stops.parquet")
    matched = match_gtfs_stops_to_nodes(
        stops.select("stop_idx", "stop_lat", "stop_lon"),
        walking_nodes.select(
            pl.col("osm_id").cast(pl.UInt64), "lat", "long"
        ),
        max_snap_distance_m,
    )
    out = gtfs_dir / STOP_NODES_FILE
    matched.write_parquet(out)
    rlog.info(
        f"GTFS: snapped {len(matched)}/{len(stops)} stops to walking nodes "
        f"(max {max_snap_distance_m} m)"
    )
    return out
