import os

import typer
from typing_extensions import Annotated

from mcr_py.utils import storage
from mcr_py.utils.footpaths import GenerationMethod
from mcr_py.utils.footpaths import generate as direct_generate
from mcr_py.utils.key import (
    COMPLETE_GTFS_CLEAN_COMMAND_NAME,
    FOOTPATHS_KEY,
    STOPS_KEY,
)
from mcr_py.utils.logger import Timed

CLEAN_STOPS_FILENAME = storage.get_df_filename_for_name(STOPS_KEY)

CITY_ID_HELP = """
City ID used for pyrosm, see pyrosm "Available datasets" for more information.
The area of the dataset associated with the city ID should be at least as \
large as the area (convex hull) of the stops.
Required if '--osm' is not provided.
"""

OSM_HELP = """
OSM pbf file.
The area of the dataset should be at least as large as the area (convex hull) \
of the stops.
Required if '--city-id' is not provided.
"""

STOPS_HELP = f"""
A path that should point to either {CLEAN_STOPS_FILENAME} or a directory \
containing {CLEAN_STOPS_FILENAME}, as given by the output of the \
{COMPLETE_GTFS_CLEAN_COMMAND_NAME} command.
"""

DEFAULT_MAX_WALKING_DURATION = 10 * 60
DEFAULT_AVG_WALKING_SPEED = 1.4


def generate(
    output: Annotated[str, typer.Option(help="Output file in pickle format.")],
    stops: Annotated[str, typer.Option(help=STOPS_HELP)],
    geo_meta_path: Annotated[
        str,
        typer.Option(
            help="Path to GeoMeta object.",
        ),
    ],
    avg_walking_speed: Annotated[
        float,
        typer.Option(
            help="Average walking speed in meters per second.",
        ),
    ] = DEFAULT_AVG_WALKING_SPEED,
    max_walking_duration: Annotated[
        int,
        typer.Option(
            help="Maximum walking duration in seconds.",
        ),
    ] = DEFAULT_MAX_WALKING_DURATION,
    city_id: Annotated[
        str,
        typer.Option(
            help=CITY_ID_HELP,
        ),
    ] = "",
    osm: Annotated[str, typer.Option(help=OSM_HELP)] = "",
    method: Annotated[
        str,
        typer.Option(
            help=f"Method to use for generating footpaths ({', '.join(GenerationMethod.all())})."
        ),
    ] = GenerationMethod.RUSTWORKX.name,
) -> None:
    validate_flags(
        city_id,
        osm,
        stops,
        avg_walking_speed,
        max_walking_duration,
        output,
    )

    parsed_method = GenerationMethod.from_str(method)
    with Timed.info("Generating footpaths"):
        footpaths = direct_generate(
            city_id,
            osm,
            stops,
            avg_walking_speed,
            parsed_method,
        )

    storage.write_any_dict({FOOTPATHS_KEY: footpaths}, output)


def validate_flags(
    city_id: str,
    osm: str,
    stops: str,
    avg_walking_speed: float,
    max_walking_duration: int,
    output: str,
) -> None:
    if not city_id and not osm:
        msg = "Either '--city-id' or '--osm' must be provided."
        raise typer.BadParameter(
            msg,
        )

    if osm and not os.path.isfile(osm):
        msg = f"File '{osm}' does not exist."
        raise typer.BadParameter(msg)

    if not os.path.isfile(stops):
        msg = f"File '{stops}' does not exist."
        raise typer.BadParameter(msg)

    if avg_walking_speed <= 0:
        msg = f"Average walking speed must be positive, got {avg_walking_speed}."
        raise typer.BadParameter(
            msg,
        )

    if max_walking_duration <= 0:
        msg = f"Maximum walking duration must be positive, got {max_walking_duration}."
        raise typer.BadParameter(
            msg,
        )
