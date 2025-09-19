import pathlib
import typing

from mcr_py.mcr.data import NetworkType, OSMData
from mcr_py.mcr.steps.bicycle import BicycleStepBuilder
from mcr_py.mcr.steps.car import PersonalCarStepBuilder
from mcr_py.mcr.steps.interface import StepBuilder
from mcr_py.mcr.steps.public_transport import PublicTransportStepBuilder
from mcr_py.mcr.steps.walking import WalkingStepBuilder
from mcr_py.minute_city import minute_city
from mcr_py.utils.logger import Timed

CAR_CONFIG = "car"
BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG = "bicycle_public_transport"
BICYCLE_CONFIG = "bicycle"
WALKING_CONFIG = "walking"
PUBLIC_TRANSPORT_CONFIG = "public_transport"

ALL_CONFIGS = [
    WALKING_CONFIG,
    BICYCLE_CONFIG,
    PUBLIC_TRANSPORT_CONFIG,
    CAR_CONFIG,
    BICYCLE_AND_PUBLIC_TRANSPORT_CONFIG,
]

type StepBuilderMatrix = tuple[
    typing.Sequence[typing.Sequence[StepBuilder]],
    typing.Sequence[typing.Sequence[StepBuilder]],
]


def get_car_only_config_with_data(geo_meta, geo_data):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    driving_nodes, driving_edges, _ = geo_data.additional_networks[NetworkType.DRIVING]
    car_step = PersonalCarStepBuilder(
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        driving_nodes,  # type: ignore
        driving_edges,  # type: ignore
        pois,
    )

    initial_steps = []
    repeating_steps = [[car_step]]
    return initial_steps, repeating_steps


def get_bicycle_public_transport_config_with_data(
    geo_meta,
    geo_data,
    bicycle_price_function: str,
    bicycle_location_path: pathlib.Path,
    structs_path: pathlib.Path,
    stops_path: pathlib.Path,
):
    with Timed.info("Fetching POI for runtime optimization"):
        pois = minute_city.fetch_pois_for_area(geo_meta.boundary, geo_data.osm_nodes)  # type: ignore

    cycling_nodes, cycling_edges, _ = geo_data.additional_networks[NetworkType.CYCLING]
    bicycle_step = BicycleStepBuilder(
        bicycle_price_function,
        bicycle_location_path,
        geo_meta,
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        cycling_nodes,  # type: ignore
        cycling_edges,  # type: ignore
        pois,
    )

    public_transport_step = PublicTransportStepBuilder(
        structs_path,
        stops_path,
        geo_data.nxgraph,
    )

    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        pois,
    )

    initial_steps = [[walking_step]]
    repeating_steps = [
        [bicycle_step, public_transport_step],
        [walking_step],
    ]

    return initial_steps, repeating_steps


def get_bicycle_only_config_with_data(
    geo_data: OSMData,
    bicycle_price_function: str,
    bicycle_location_path: pathlib.Path,
) -> StepBuilderMatrix:
    cycling_nodes, cycling_edges, _ = geo_data.additional_networks[NetworkType.CYCLING]
    bicycle_step = BicycleStepBuilder(
        bicycle_price_function,
        bicycle_location_path,
        "geo_meta",  # type: ignore
        geo_data.osm_nodes,  # type: ignore
        geo_data.osm_edges,  # type: ignore
        cycling_nodes,  # type: ignore
        cycling_edges,  # type: ignore
        geo_data.pois,
    )

    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        geo_data.pois,
    )

    initial_steps = [[walking_step]]
    repeating_steps = [
        [bicycle_step],
        [walking_step],
    ]

    return initial_steps, repeating_steps


def get_walking_only_config_with_data(geo_data: OSMData) -> StepBuilderMatrix:
    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        geo_data.pois,
    )

    initial_steps = [[walking_step]]
    repeating_steps = []

    return initial_steps, repeating_steps


def get_public_transport_only_config_with_data(
    geo_data: OSMData,
    structs_path: pathlib.Path,
    stops_path: pathlib.Path,
) -> StepBuilderMatrix:
    walking_step = WalkingStepBuilder(
        geo_data.osm_nodes,
        geo_data.osm_edges,
        geo_data.pois,
    )
    public_transport_step = PublicTransportStepBuilder(
        structs_path,
        stops_path,
        geo_data.osm_nodes,
    )

    initial_steps = [[walking_step]]
    repeating_steps = [[public_transport_step], [walking_step]]

    return initial_steps, repeating_steps
