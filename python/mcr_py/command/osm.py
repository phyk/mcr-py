import typer
from mcr_py.package import key
from mcr_py.package.osm import osm
from typing_extensions import Annotated
from mcr_py.package.geometa import GeoMeta

app = typer.Typer()


@app.command(
    name=key.OSM_LIST_COMMAND_NAME,
    help="List all available OSM data",
)
def list_command(
    selector: Annotated[
        str, typer.Option(help="Selector in dot notation, e.g. '.regions.africa'")
    ] = "",
):
    osm.list_available(selector)


@app.command(
    name=key.OSM_PREP_COMMAND_NAME,
    help='Preload OSM protobuf file'
)
def prep_osm_networks(
    city_id: Annotated[
        str, typer.Argument(help="OSM area id. This has to contain the target area.")
    ],
    additional_networks: Annotated[list[str], typer.Argument(help="Networks to parse from OSM")],
    geometa_path: Annotated[str, typer.Argument(help="Path to the geometa state file")],
    location: Annotated[
        str, typer.Option(help="Target directory for saving the protobuf file")
    ] = "/tmp/pyrosm"
):
    geometa = GeoMeta.load(geometa_path)
    reader = osm.get_osm_reader_for_city_id_or_osm_path(city_id=city_id, osm_path=f'{location}/{city_id}.osm.pbf')
    for network_type in additional_networks:
        osm.get_graph_for_city_cropped_to_boundary(reader, geometa, network_type=network_type)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main():
    pass
