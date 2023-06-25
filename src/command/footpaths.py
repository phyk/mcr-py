from typing_extensions import Annotated
from pyrosm.data import os
import typer

from package import storage
from package.footpaths import generate as direct_generate
from package.key import COMPLETE_GTFS_CLEAN_COMMAND_NAME, STOPS_KEY, FOOTPATHS_KEY

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


def generate(
    output: Annotated[str, typer.Option(help="Output file in pickle format.")],
    stops: Annotated[str, typer.Option(help=STOPS_HELP)],
    avg_walking_speed: Annotated[
        float,
        typer.Option(
            help="Average walking speed in meters per second.",
        ),
    ] = 1.4,
    max_walking_duration: Annotated[
        int,
        typer.Option(
            help="Maximum walking duration in seconds.",
        ),
    ] = 10
    * 60,
    city_id: Annotated[
        str,
        typer.Option(
            help=CITY_ID_HELP,
        ),
    ] = "",
    osm: Annotated[str, typer.Option(help=OSM_HELP)] = "",
):
    validate_flags(
        city_id,
        osm,
        stops,
        avg_walking_speed,
        max_walking_duration,
        output,
    )
    footpaths = direct_generate(
        city_id,
        osm,
        stops,
        avg_walking_speed,
        max_walking_duration,
    )

    storage.write_any_dict({FOOTPATHS_KEY: footpaths}, output)


def validate_flags(
    city_id: str,
    osm: str,
    stops: str,
    avg_walking_speed: float,
    max_walking_duration: int,
    output: str,
):
    if not city_id and not osm:
        raise typer.BadParameter(
            "Either '--city-id' or '--osm' must be provided.",
        )

    if osm and not os.path.isfile(osm):
        raise typer.BadParameter(f"File '{osm}' does not exist.")

    if not os.path.isfile(stops):
        raise typer.BadParameter(f"File '{stops}' does not exist.")

    if avg_walking_speed <= 0:
        raise typer.BadParameter(
            f"Average walking speed must be positive, got {avg_walking_speed}.",
        )

    if max_walking_duration <= 0:
        raise typer.BadParameter(
            f"Maximum walking duration must be positive, got {max_walking_duration}.",
        )
