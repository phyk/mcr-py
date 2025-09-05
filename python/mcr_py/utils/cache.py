import hashlib
import os
import pathlib

import polars as pl
from shapely.geometry import Polygon

from mcr_py.utils import storage

templir = storage.get_tmp_path()


def overwrite_tempdir(path: pathlib.Path) -> None:
    """
    Overwrites the global temporary directory path.

    :param path: str - The new path to set as the temporary directory.
    """
    global templir
    templir = path


def hash_df(df: pl.DataFrame) -> int:
    """
    Computes the SHA-256 hash of a DataFrame by hashing its schema and row data.

    :param df: pl.DataFrame - The DataFrame to hash.
    :returns: int - The computed hash as an integer.
    """
    hasher = hashlib.sha256()
    for c, t in df.schema.items():
        hasher.update(c.encode())
        hasher.update(str(t).encode())
    for h in df.hash_rows(42):
        hasher.update(h.to_bytes(64, "big"))
    return int(hasher.hexdigest(), 16)


def hash_str(s: str) -> int:
    """
    Computes the SHA-256 hash of a string.

    :param s: str - The string to hash.
    :returns: int - The computed hash as an integer.
    """
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16)


def hash_polygon(polygon: Polygon) -> int:
    """
    Computes the hash of a Polygon object.

    :param polygon: Polygon - The Polygon to hash.
    :returns: int - The computed hash as an integer.
    """
    return hash_str(str(polygon))


def combine_hashes(hashes: list[int]) -> int:
    """
    Combines multiple hashes into a single hash using SHA-256.

    :param hashes: list[int] - A list of integer hashes to combine.
    :returns: int - The combined hash as an integer.
    """
    return int(
        hashlib.sha256("".join([str(h) for h in hashes]).encode("utf-8")).digest().hex(),
        16,
    )


def cache_gdf(df: pl.DataFrame, hash_value: int, identifier: str) -> None:
    """
    Caches a DataFrame to a file using its hash and an identifier.

    :param df: pl.DataFrame - The DataFrame to cache.
    :param hash_value: int - The hash of the DataFrame.
    :param identifier: str - An identifier to include in the cached file name.
    """
    if not os.path.exists(templir):
        os.mkdir(templir)
    path = os.path.join(templir, f"{identifier}_{hash_value}")
    df.write_parquet(path)


def read_gdf(hash_value: int, identifier: str) -> pl.DataFrame:
    """
    Reads a cached DataFrame from a file using its hash and an identifier.

    :param hash: int - The hash of the DataFrame.
    :param identifier: str - An identifier to locate the cached file.
    :returns: pl.DataFrame - The read DataFrame.
    """
    path = os.path.join(templir, f"{identifier}_{hash_value}")
    return pl.read_parquet(path)


def cache_entry_exists(hash_value: int, identifier: str) -> bool:
    """
    Checks if a cached entry exists for a given hash and identifier.

    :param hash: int - The hash of the DataFrame.
    :param identifier: str - An identifier to check for the cached file.
    :returns: bool - True if the cached entry exists, False otherwise.
    """
    path = os.path.join(templir, f"{identifier}_{hash_value}")
    return os.path.exists(path)
