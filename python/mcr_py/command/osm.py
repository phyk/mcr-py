import typer
from mcr_py.package import key
from mcr_py.package.osm import osm
from typing_extensions import Annotated

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


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main():
    pass
