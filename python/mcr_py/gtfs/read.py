import os

import polars as pl
from rich.console import Console
from rich.table import Table

from mcr_py.gtfs import archive
from mcr_py.utils import key, storage
from mcr_py.utils.logger import Timed, rlog


def print_stops(path: str) -> None:
    """
    Reads and prints the stops DataFrame from the specified path.

    :param path: str - The path to a zip file or directory containing stops data.
    """
    with Timed.info("Reading stops"):
        stops_df = get_stops_df(path)
    print_dataframe(stops_df)


def get_stops_df(path: str) -> pl.DataFrame:
    """
    Retrieves the stops DataFrame from a zip file or directory.

    :param path: str - The path to a zip file or directory containing stops data.
    :returns: pl.DataFrame - The DataFrame containing stop information.
    :raises ValueError: If the path is neither a zip file nor a directory.
    """
    if path.endswith(".zip"):
        rlog.debug("Reading stops from zip file")
        dfs = archive.read_dfs(path)
        return dfs[key.STOPS_KEY]

    if not os.path.isdir(path):
        msg = "Path is neither a zip file nor a directory"
        raise ValueError(msg)

    rlog.debug("Reading stops from directory")

    return storage.read_df(os.path.join(path, storage.get_df_filename_for_name(key.STOPS_KEY)))


def print_dataframe(df: pl.DataFrame) -> None:
    """
    Prints the stops DataFrame in a formatted table.

    :param df: pl.DataFrame - The DataFrame containing stop information to print.
    """
    table = Table(title="Stops")
    table.add_column("Stop ID")
    table.add_column("Stop Name")
    table.add_column("Google maps link")
    for sid, sname, slat, slon in df.iter_rows():
        table.add_row(
            format_value(sid),  # type: ignore
            format_value(sname),  # type: ignore
            format_value(f"https://maps.google.com/?q={slat},{slon}"),  # type: ignore
        )

    console = Console()
    console.print(table)


def format_value(value) -> str:
    """
    Formats a value for display as a string.

    :param value: Any - The value to format.
    :returns: str - The string representation of the value.
    """
    return str(value)
