import os
import pathlib
import pickle

import polars as pl
import requests
from typing_extensions import Any

from mcr_py.utils import key


def write_dfs_dict(dfs_dict: dict[str, pl.DataFrame], output_path: pathlib.Path) -> None:
    """
    Writes a dictionary of Polars DataFrames to separate Parquet files in the specified output directory.

    :param dfs_dict: dict[str, pl.DataFrame] - A dictionary where keys are names and values are DataFrames to be written.
    :param output_path: str - The directory path where the Parquet files will be saved.
    """
    output_path.mkdir(parents=True, exist_ok=True)

    for name, df in dfs_dict.items():
        filename = get_df_filename_for_name(name)
        df.write_parquet(output_path / filename)


def write_df(df: pl.DataFrame, output_path: str) -> None:
    """
    Writes a single Polars DataFrame to a Parquet file at the specified output path.

    :param df: pl.DataFrame - The DataFrame to be written to a Parquet file.
    :param output_path: str - The file path where the Parquet file will be saved.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)


def get_df_filename_for_name(name: str) -> str:
    """
    Generates a filename for a DataFrame based on its name, appending the '.parquet' extension.

    :param name: str - The name of the DataFrame.
    :returns: str - The generated filename with '.parquet' extension.
    """
    return f"{name}.parquet"


def read_df(path: pathlib.Path) -> pl.DataFrame:
    """
    Reads a Parquet file into a Polars DataFrame using a predefined schema.

    :param path: str - The file path to the Parquet file.
    :returns: pl.DataFrame - The DataFrame read from the Parquet file.
    """
    return pl.read_parquet(path)  # type: ignore


def write_any_dict(data: dict[str, Any], output_path: pathlib.Path) -> None:
    """
    Serializes and writes a dictionary to a file using pickle.

    :param data: dict[str, Any] - The dictionary to be serialized and written.
    :param output_path: str - The file path where the serialized dictionary will be saved.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as f:
        pickle.dump(data, f)


def read_any_dict(path: pathlib.Path) -> dict[str, Any]:
    """
    Reads and deserializes a dictionary from a file using pickle.

    :param path: str - The file path to the serialized dictionary.
    :returns: dict[str, Any] - The deserialized dictionary.
    """
    with open(path, "rb") as f:
        return pickle.load(f)  # noqa: S301


def get_tmp_path(*paths: str) -> str:
    """
    Constructs a temporary file path by joining specified paths with predefined directory location and name.

    :param paths: str - Variable length argument list for path components to be joined.
    :returns: str - The constructed temporary file path.
    """
    return os.path.join(key.TMP_DIR_LOCATION, key.ROOT_TMP_DIR_NAME, *paths)


def download_file(url: str, path: pathlib.Path) -> None:
    """
    Downloads a file from a specified URL and saves it to a specified path.

    :param url: str - The URL from which the file is to be downloaded.
    :param path: str - The file path where the downloaded file will be saved.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        f.write(requests.get(url, timeout=20).content)
