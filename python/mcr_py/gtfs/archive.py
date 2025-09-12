import pathlib
import zipfile

import polars as pl

from mcr_py.utils.key import (
    STOP_TIMES_KEY,
    STOPS_KEY,
    TRIPS_KEY,
)
from mcr_py.utils.logger import rlog


def get_gtfs_filename(name: str) -> str:
    """
    Generates the GTFS filename for a given name.

    :param name: str - The base name of the GTFS file.
    :returns: str - The formatted GTFS filename with .txt extension.
    """
    return f"{name}.txt"


STOPS_FILE = get_gtfs_filename(STOPS_KEY)
TRIPS_FILE = get_gtfs_filename(TRIPS_KEY)
STOP_TIMES_FILE = get_gtfs_filename(STOP_TIMES_KEY)
CALENDAR_FILE = get_gtfs_filename("calendar")
ROUTES_FILE = get_gtfs_filename("routes")


EXPECTED_FILES = [
    STOPS_FILE,
    TRIPS_FILE,
    STOP_TIMES_FILE,
    CALENDAR_FILE,
    ROUTES_FILE,
]


def read_dfs(gtfs_zip_path: pathlib.Path) -> dict[str, pl.DataFrame]:
    """
    Reads GTFS zip file and returns a dictionary of dataframes.

    :param gtfs_zip_path: str - The path to the GTFS zip file.
    :returns: dict[str, pl.DataFrame] - A dictionary where keys are file names and values are DataFrames.
    :raises Exception: If an expected file is not found in the zip file.
    """
    dfs = {}

    with zipfile.ZipFile(gtfs_zip_path, "r") as zip_ref:
        contained = zip_ref.namelist()

        for expected_file in EXPECTED_FILES:
            if expected_file not in contained:
                msg = f"Expected file {expected_file} not in zip file"
                raise Exception(msg)

        for file in EXPECTED_FILES:
            df = read_file(zip_ref, file)
            if "stop_id" in df.columns:
                df = df.with_columns(pl.col("stop_id").cast(pl.String))
            name = file.split(".")[0]
            dfs[name] = df

    return dfs


def read_file(zip_ref: zipfile.ZipFile, file: str) -> pl.DataFrame:
    """
    Reads a single file from the GTFS zip and returns it as a DataFrame.

    :param zip_ref: zipfile.ZipFile - The reference to the opened zip file.
    :param file: str - The name of the file to read from the zip.
    :returns: pl.DataFrame - The DataFrame containing the data from the file.
    """
    with zip_ref.open(file) as f:
        rlog.debug(f"Reading {file}")
        df = pl.read_csv(f, infer_schema_length=10000)  # type: ignore
        return df


def write_dfs(dfs: dict[str, pl.DataFrame], output: pathlib.Path) -> None:
    """
    Writes a dictionary of dataframes to a GTFS zip file.

    :param dfs: dict[str, pl.DataFrame] - The dictionary of DataFrames to write.
    :param output: str - The path where the output zip file will be saved.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w") as zip_ref:
        for name, df in dfs.items():
            file = get_gtfs_filename(name)
            write_file(zip_ref, file, df)


def write_file(zip_ref: zipfile.ZipFile, file: str, df: pl.DataFrame) -> None:
    """
    Writes a DataFrame to a file within the GTFS zip.

    :param zip_ref: zipfile.ZipFile - The reference to the opened zip file.
    :param file: str - The name of the file to write to the zip.
    :param df: pl.DataFrame - The DataFrame to write to the file.
    """
    with zip_ref.open(file, "w") as f:
        df.write_csv(f)
