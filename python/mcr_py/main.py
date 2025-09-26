import typer
from typing_extensions import Annotated

from .command import footpaths, raptor
from .utils import key, logger

app = typer.Typer(pretty_exceptions_show_locals=False)
app.command(key.FOOTPATHS_COMMAND_NAME)(footpaths.generate)
app.command(key.RAPTOR_COMMAND_NAME)(raptor.raptor)


@app.callback(invoke_without_command=True, no_args_is_help=True)
def main(log_level: Annotated[str, typer.Option(help="Log level.")] = "INFO") -> None:
    logger.setup(log_level.upper())


if __name__ == "__main__":
    app()
