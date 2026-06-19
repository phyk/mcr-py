"""Builds mcr-rust scenarios (mode combinations) from `config.toml` settings.

`[mcr5] scenarios` is a list of mode-name combos; each combo runs together in
one MCR pass. Modes map 1:1 onto mcr-rust's `MCRConfig` slots, except
`public_transport` (which expands into one scenario per `start_times` entry)
and `shared_bicycle`/`shared_scooter` (which both populate the
`shared_micromobile` list, so a combo can carry both at once).
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any

from mcr_py.mcr5.mcr5 import (
    PriceFunction,
    PrivateModeConfig,
    PublicTransportConfig,
    SharedMicromobileConfig,
    WalkingConfig,
)

MODE_NAMES = (
    "walking",
    "private_bike",
    "private_car",
    "shared_bicycle",
    "shared_scooter",
    "public_transport",
)

# Graph layers (`build_graph` kwargs) each mode needs beyond the universal
# walking/POI layer that every scenario loads.
_GRAPH_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "private_bike": ("cycling_nodes", "cycling_edges"),
    "private_car": ("car_nodes", "car_edges"),
    "shared_bicycle": (
        "cycling_nodes",
        "cycling_edges",
        "shared_bike_stations",
        "shared_bike_dropoff_zones",
    ),
    "shared_scooter": (
        "cycling_nodes",
        "cycling_edges",
        "shared_scooter_stations",
        "shared_scooter_dropoff_zones",
    ),
}


def time_str_to_secs(time_str: str) -> int:
    """Convert a ``HH:MM:SS`` clock time into seconds since midnight."""
    hours, minutes, seconds = (int(part) for part in time_str.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def build_paths(base_directory: pathlib.Path, city_name: str) -> dict[str, str]:
    """Resolve every parquet/geojson input `run_mcr5` needs, as string paths."""
    graph_dir = base_directory / "graph" / city_name
    start_nodes_dir = base_directory / "start_nodes" / city_name
    return {
        "walking_nodes": str(graph_dir / "walking_nodes.parquet"),
        "walking_edges": str(graph_dir / "walking_edges.parquet"),
        "cycling_nodes": str(graph_dir / "cycling_nodes.parquet"),
        "cycling_edges": str(graph_dir / "cycling_edges.parquet"),
        "car_nodes": str(graph_dir / "car_nodes.parquet"),
        "car_edges": str(graph_dir / "car_edges.parquet"),
        "poi_nodes": str(graph_dir / "poi_nodes.parquet"),
        "shared_bike_stations": str(graph_dir / "shared_bike_stations.parquet"),
        "shared_bike_dropoff_zones": str(graph_dir / "shared_bike_dropoff_zones.geojson"),
        "shared_scooter_stations": str(graph_dir / "shared_scooter_stations.parquet"),
        "shared_scooter_dropoff_zones": str(
            graph_dir / "shared_scooter_dropoff_zones.geojson"
        ),
        "gtfs_data_dir": str(base_directory / "gtfs_clean" / city_name),
        "walking_start_nodes": str(start_nodes_dir / "walking.parquet"),
        "car_start_nodes": str(start_nodes_dir / "car.parquet"),
    }


def _walking_config(settings: dict[str, Any]) -> WalkingConfig:
    return WalkingConfig(speed_kmh=settings["speed_kmh"])


def _private_mode_config(settings: dict[str, Any]) -> PrivateModeConfig:
    return PrivateModeConfig(
        speed_kmh=settings["speed_kmh"],
        switch_time_s=settings["switch_time_s"],
        price_function_base=settings["price_function_base"],
    )


def _shared_micromobile_config(settings: dict[str, Any], mode: str) -> SharedMicromobileConfig:
    price_function = PriceFunction(
        unlock_fee=settings["unlock_fee"],
        interval_minutes=settings["interval_minutes"],
        price_per_interval=settings["price_per_interval"],
        first_interval_free=settings["first_interval_free"],
    )
    return SharedMicromobileConfig(
        speed_kmh=settings["speed_kmh"],
        switch_time_s=settings["switch_time_s"],
        price_function=price_function,
        mode=mode,
    )


def _public_transport_config(
    settings: dict[str, Any], gtfs_data_dir: str, start_time: str
) -> PublicTransportConfig:
    return PublicTransportConfig(
        data_dir=gtfs_data_dir,
        anchor_time_secs=time_str_to_secs(start_time),
        max_snap_distance_m=settings["max_snap_distance_m"],
        flex_window_secs=settings["flex_window_secs"],
        short_trip_fare_cents=settings["short_trip_fare_cents"],
        short_trip_max_stops=settings["short_trip_max_stops"],
        long_trip_fare_cents=settings["long_trip_fare_cents"],
        switch_time_secs=settings["switch_time_secs"],
    )


@dataclass
class Scenario:
    key: str
    graph_kwargs: dict[str, str]
    config_kwargs: dict[str, Any]
    start_nodes: str


def build_scenarios(
    combo: list[str],
    mode_settings: dict[str, dict[str, Any]],
    paths: dict[str, str],
) -> list[Scenario]:
    """Expand one `[mcr5] scenarios` entry into one or more `Scenario`s.

    Most combos produce exactly one `Scenario`; a combo containing
    `public_transport` produces one per configured start time, keyed
    ``<combo>_<idx>`` (matching the trailing-index convention
    `20_mcr5_results_calculation.py` already strips via `trim_trailing_numbers`).
    """
    modes = [mode for mode in combo if mode != "walking"]
    unknown = sorted(set(modes) - set(MODE_NAMES))
    if unknown:
        message = f"Unknown mode(s) in scenario {combo}: {unknown}"
        raise ValueError(message)

    graph_kwargs = {
        "walking_nodes": paths["walking_nodes"],
        "walking_edges": paths["walking_edges"],
        "poi_nodes": paths["poi_nodes"],
    }
    for mode in modes:
        for layer in _GRAPH_REQUIREMENTS.get(mode, ()):
            graph_kwargs[layer] = paths[layer]

    config_kwargs: dict[str, Any] = {"walking": _walking_config(mode_settings["walking"])}
    shared_micromobile: list[SharedMicromobileConfig] = []
    for mode in modes:
        if mode == "private_bike":
            config_kwargs["cycling"] = _private_mode_config(mode_settings["private_bike"])
        elif mode == "private_car":
            config_kwargs["car"] = _private_mode_config(mode_settings["private_car"])
        elif mode == "shared_bicycle":
            shared_micromobile.append(
                _shared_micromobile_config(mode_settings["shared_bicycle"], "shared_bicycle")
            )
        elif mode == "shared_scooter":
            shared_micromobile.append(
                _shared_micromobile_config(mode_settings["shared_scooter"], "shared_scooter")
            )
    if shared_micromobile:
        config_kwargs["shared_micromobile"] = shared_micromobile

    start_nodes = (
        paths["car_start_nodes"] if "private_car" in modes else paths["walking_start_nodes"]
    )
    key = "+".join(combo)

    if "public_transport" not in modes:
        return [
            Scenario(
                key=key,
                graph_kwargs=graph_kwargs,
                config_kwargs=config_kwargs,
                start_nodes=start_nodes,
            )
        ]

    pt_settings = mode_settings["public_transport"]
    return [
        Scenario(
            key=f"{key}_{idx}",
            graph_kwargs=dict(graph_kwargs),
            config_kwargs={
                **config_kwargs,
                "public_transport": _public_transport_config(
                    pt_settings, paths["gtfs_data_dir"], start_time
                ),
            },
            start_nodes=start_nodes,
        )
        for idx, start_time in enumerate(pt_settings["start_times"])
    ]
