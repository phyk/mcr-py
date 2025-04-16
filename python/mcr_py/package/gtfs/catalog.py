import os
from mcr_py.package.utils import storage
import requests
import polars as pl
from rich import print
from rich.console import Console
from rich.table import Table
from typing_extensions import Any

from mcr_py.package.utils import key
from mcr_py.package.utils.logger import Timed, rlog

CATALOG_PATH = storage.get_tmp_path(
    key.TMP_GTFS_DIR_NAME, key.TMP_GTFS_CATALOG_FILE_NAME
)

COL_DATA_TYPE = "data_type"
COL_COUNTRY_CODE = "location.country_code"
COL_SUBDIVISION_NAME = "location.subdivision_name"
COL_MUNICIPALITY = "location.municipality"
COL_PROVIDER = "provider"
COL_NAME = "name"
COL_DOWNLOAD_URL = "urls.direct_download"
COL_AUTH_TYPE = "urls.authentication_type"

RELEVANT_COLUMNS = [
    COL_DATA_TYPE,
    COL_COUNTRY_CODE,
    COL_SUBDIVISION_NAME,
    COL_MUNICIPALITY,
    COL_PROVIDER,
    COL_NAME,
    COL_DOWNLOAD_URL,
    COL_AUTH_TYPE,
]
ID_COLOR = "magenta"


def list_catalog(country_code: str, subdivision_name: str, municipality: str):
    """
    Lists all available GTFS feeds based on the specified filters.

    :param country_code: str - The country code to filter the catalog.
    :param subdivision_name: str - The subdivision name to filter the catalog.
    :param municipality: str - The municipality name to filter the catalog.
    """
    catalog = get_catalog()
    catalog = filter_catalog(catalog, country_code, subdivision_name, municipality)

    print_catalog(catalog)
    print(f"Total: [bold]{len(catalog)}[/bold]\n")
    print(
        f"[i] Use [bold]{key.GTFS_UPPER_COMMAND_NAME} {key.GTFS_DOWNLOAD_COMMAND_NAME} <[{ID_COLOR}]ID[/]>[/bold] to download a GTFS feed."
    )


def get_catalog() -> pl.DataFrame:
    """
    Retrieves the GTFS catalog as a DataFrame, downloading it if necessary.

    :returns: pl.DataFrame - The DataFrame containing the GTFS catalog.
    """
    if not os.path.exists(CATALOG_PATH):
        rlog.info("Downloading GTFS catalog...")
        download_catalog()
    catalog = pl.read_csv(CATALOG_PATH)
    catalog = catalog.select(RELEVANT_COLUMNS).with_columns(
        pl.Series("index", range(0, len(catalog)))
    )
    catalog = pre_filter_catalog(catalog)
    catalog = catalog.fill_nan(pl.lit(""))
    return catalog


def download_catalog():
    """
    Downloads the GTFS catalog from the specified URL and saves it to the local path.
    """
    request = requests.get(key.GTFS_CATALOG_URL)
    os.makedirs(os.path.dirname(CATALOG_PATH), exist_ok=True)
    with open(CATALOG_PATH, "wb") as f:
        f.write(request.content)


def pre_filter_catalog(catalog: pl.DataFrame) -> pl.DataFrame:
    """
    Filters the catalog to include only entries of type 'gtfs' and without specific authentication types.

    :param catalog: pl.DataFrame - The DataFrame containing the GTFS catalog.
    :returns: pl.DataFrame - The filtered catalog DataFrame.
    """
    return catalog.filter(
        (pl.col(COL_DATA_TYPE) == "gtfs") & (~pl.col(COL_AUTH_TYPE).is_in([1, 2]))
    )


def filter_catalog(
    catalog: pl.DataFrame, country_code: str, subdivision_name: str, municipality: str
) -> pl.DataFrame:
    """
    Filters the catalog DataFrame based on country code, subdivision name, and municipality.

    :param catalog: pl.DataFrame - The DataFrame containing the GTFS catalog.
    :param country_code: str - The country code to filter the catalog.
    :param subdivision_name: str - The subdivision name to filter the catalog.
    :param municipality: str - The municipality name to filter the catalog.
    :returns: pl.DataFrame - The filtered catalog DataFrame.
    """
    if country_code:
        catalog = catalog.filter(
            pl.col(COL_COUNTRY_CODE).str.contains(f"(?i){country_code}")
        )
    if subdivision_name:
        catalog = catalog.filter(
            pl.col(COL_SUBDIVISION_NAME).str.contains(f"(?i){subdivision_name}")
        )
    if municipality:
        catalog = catalog.filter(
            pl.col(COL_MUNICIPALITY).str.contains(f"(?i){municipality}")
        )
    return catalog


def print_catalog(catalog: pl.DataFrame):
    """
    Prints the GTFS catalog in a formatted table.

    :param catalog: pl.DataFrame - The DataFrame containing the GTFS catalog to print.
    """
    if len(catalog) == 0:
        print("[i] No GTFS feeds found.[/i]")
        return

    table = Table(title="GTFS Catalog", show_lines=True)

    index_max_length = max(
        max(catalog.get_column("index").cast(pl.String).str.len_chars()), len("ID")
    )
    country_code_max_length = max(
        catalog.get_column(COL_COUNTRY_CODE).str.len_chars().max(),  # type: ignore
        len("Code"),
    )
    subdivision_max_length = max(
        catalog.get_column(COL_SUBDIVISION_NAME).str.len_chars().max(),  # type: ignore
        len("Subdivision"),
    )
    municipality_max_length = max(
        int(catalog.get_column(COL_MUNICIPALITY).str.len_chars().max()),  # type: ignore
        len("Municipality"),
    )

    table.add_column("ID", style=ID_COLOR, width=index_max_length)
    table.add_column("Code", style="cyan", width=country_code_max_length)
    table.add_column("Subdivision", width=subdivision_max_length)
    table.add_column("Municipality", width=municipality_max_length)
    table.add_column("Provider")

    for index, cc, sn, mu, pr in catalog.select(
        [
            "index",
            COL_COUNTRY_CODE,
            COL_SUBDIVISION_NAME,
            COL_MUNICIPALITY,
            COL_PROVIDER,
        ]
    ).iter_rows():
        table.add_row(
            format_value(index),
            format_value(cc),  # type: ignore
            format_value(sn),  # type: ignore
            format_value(mu),  # type: ignore
            format_value(pr),  # type: ignore
        )

    console = Console()
    console.print(table)


def format_value(value: Any) -> str:
    """
    Formats a value for display, replacing None with a dash.

    :param value: Any - The value to format.
    :returns: str - The formatted string representation of the value.
    """
    formatted_value = str(value) or "-"
    return formatted_value


def download(id: int, output: str):
    """
    Downloads a GTFS feed based on the specified ID and saves it to the output path.

    :param id: int - The ID of the GTFS feed to download.
    :param output: str - The path where the downloaded feed will be saved.
    """
    catalog = get_catalog()
    catalog = catalog.filter(pl.col("index") == id)

    if len(catalog) == 0:
        rlog.error(f"GTFS feed with ID {id} not found.")
        return

    row = catalog.row(0, named=True)
    url = row[COL_DOWNLOAD_URL]
    if not url:
        rlog.error(f"GTFS feed with ID {id} has no download URL.")
        return

    with Timed.info(f"Downloading GTFS feed with ID {id}"):
        storage.download_file(url, output)
