"""Thin Python facade over mcr-rust's parallel `run_mcr5` orchestrator.

The previous implementation spawned one `multiprocessing.Process` per H3 cell
and ran a Python-side MCR pipeline that called into the old `mlc` crate per
step. The Rust crate now owns both the per-origin pipeline and the parallel
loop over start nodes for a single configuration; this module only re-exports
the binding surface and a small convenience builder.
"""

from __future__ import annotations

import os
from typing import Optional, Sequence

from mcr_py._mcr_py import (
    MCRConfig,
    MCRGraph,
    MCRGraphBuilder,
    PriceFunction,
    PrivateModeConfig,
    PublicTransportConfig,
    SharedMicromobileConfig,
    WalkingConfig,
    run_mcr5,
)

__all__ = [
    "MCRConfig",
    "MCRGraph",
    "MCRGraphBuilder",
    "PriceFunction",
    "PrivateModeConfig",
    "PublicTransportConfig",
    "SharedMicromobileConfig",
    "WalkingConfig",
    "build_graph",
    "build_config",
    "run_mcr5",
]


def build_graph(
    walking_nodes: str,
    walking_edges: str,
    poi_nodes: str,
    cycling_nodes: Optional[str] = None,
    cycling_edges: Optional[str] = None,
    car_nodes: Optional[str] = None,
    car_edges: Optional[str] = None,
    shared_bike_stations: Optional[str] = None,
    shared_bike_dropoff_zones: Optional[str] = None,
    shared_scooter_stations: Optional[str] = None,
    shared_scooter_dropoff_zones: Optional[str] = None,
) -> MCRGraph:
    builder = MCRGraphBuilder()
    builder.add_walking_nodes(walking_nodes)
    builder.add_walking_edges(walking_edges)
    builder.add_poi_nodes(poi_nodes)
    if cycling_nodes is not None:
        builder.add_cycling_nodes(cycling_nodes)
    if cycling_edges is not None:
        builder.add_cycling_edges(cycling_edges)
    if car_nodes is not None:
        builder.add_car_nodes(car_nodes)
    if car_edges is not None:
        builder.add_car_edges(car_edges)
    if shared_bike_stations is not None:
        builder.add_shared_bike_stations(shared_bike_stations)
    if shared_bike_dropoff_zones is not None:
        builder.add_shared_bike_dropoff_zones(shared_bike_dropoff_zones)
    if shared_scooter_stations is not None:
        builder.add_shared_scooter_stations(shared_scooter_stations)
    if shared_scooter_dropoff_zones is not None:
        builder.add_shared_scooter_dropoff_zones(shared_scooter_dropoff_zones)
    return builder.build()


def build_config(
    out_dir: str,
    walking: Optional[WalkingConfig] = None,
    cycling: Optional[PrivateModeConfig] = None,
    car: Optional[PrivateModeConfig] = None,
    shared_micromobile: Optional[Sequence[SharedMicromobileConfig]] = None,
    public_transport: Optional[PublicTransportConfig] = None,
    enable_limit: bool = False,
) -> MCRConfig:
    os.makedirs(out_dir, exist_ok=True)
    return MCRConfig(
        out_dir=out_dir,
        walking=walking,
        cycling=cycling,
        car=car,
        shared_micromobile=list(shared_micromobile)
        if shared_micromobile is not None
        else None,
        public_transport=public_transport,
        enable_limit=enable_limit,
    )
