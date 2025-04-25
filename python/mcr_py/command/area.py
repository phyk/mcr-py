import mcr_py.utils.geometa
from typing_extensions import Annotated
import typer
import mcr_py.overpass.query


def create_area(
    city_name_german: Annotated[
        str, typer.Argument(help="Name used to find OSM entity of area")
    ],
    admin_level: Annotated[int, typer.Argument(help="OSM admin level")],
    crs_target: Annotated[
        str, typer.Argument(help="The local coordinate reference system")
    ],
    geometa_path: Annotated[
        str, typer.Option(help="The path for the geometa state file")
    ] = "data/",
):
    crs = "EPSG:4326"
    boundary_polygon = mcr_py.overpass.query.fetch_boundary_polygon(
        city_name_german, admin_level
    )
    geometa = mcr_py.utils.geometa.GeoMeta.create(boundary_polygon, crs, crs_target)
    geometa.save(geometa_path)
