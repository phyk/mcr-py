import os

import polars as pl
import polars_st as st
import pytest
from mcr_py.utils.cache import (  # Replace 'your_module' with the actual module name
    cache_entry_exists,
    cache_gdf,
    combine_hashes,
    hash_df,
    hash_polygon,
    hash_str,
    overwrite_tempdir,
    read_gdf,
)
from shapely.geometry import Polygon


# Sample GeoDataFrame for testing
@pytest.fixture
def sample_gdf():
    # Create a simple GeoDataFrame for testing
    return st.GeoDataFrame({"geometry": [Polygon([(0, 0), (1, 1), (1, 0)])], "value": [1]})


@pytest.fixture
def temp_dir(tmp_path):
    # Use pytest's built-in temporary directory fixture
    return tmp_path


def test_hash_gdf(sample_gdf):
    gdf_hash = hash_df(sample_gdf)
    assert isinstance(gdf_hash, int)


def test_hash_str():
    test_string = "test"
    string_hash = hash_str(test_string)
    assert isinstance(string_hash, int)
    assert string_hash == hash_str(test_string)  # Ensure consistent hashing


def test_hash_polygon():
    polygon = Polygon([(0, 0), (1, 1), (1, 0)])
    polygon_hash = hash_polygon(polygon)
    assert isinstance(polygon_hash, int)
    assert polygon_hash == hash_polygon(polygon)  # Ensure consistent hashing


def test_combine_hashes():
    hashes = [1, 2, 3]
    combined_hash = combine_hashes(hashes)
    assert isinstance(combined_hash, int)


def test_cache_gdf(temp_dir, sample_gdf):
    identifier = "test_identifier"
    gdf_hash = hash_df(sample_gdf)

    overwrite_tempdir(str(temp_dir))  # Set the temporary directory
    cache_gdf(sample_gdf, gdf_hash, identifier)

    # Check if the file exists
    cached_file_path = os.path.join(temp_dir, f"{identifier}_{gdf_hash}")
    assert os.path.exists(cached_file_path)


def test_read_gdf(temp_dir, sample_gdf):
    identifier = "test_identifier"
    gdf_hash = hash_df(sample_gdf)

    overwrite_tempdir(str(temp_dir))  # Set the temporary directory
    cache_gdf(sample_gdf, gdf_hash, identifier)

    # Read the GeoDataFrame back
    read_gdf_result = read_gdf(gdf_hash, identifier)
    assert isinstance(read_gdf_result, pl.DataFrame)
    print(read_gdf_result)
    assert read_gdf_result.select(st.geom().st.bounds()).row(0) == ([0.0, 0.0, 1.0, 1.0],)


def test_cache_entry_exists(temp_dir, sample_gdf):
    identifier = "test_identifier"
    gdf_hash = hash_df(sample_gdf)

    overwrite_tempdir(str(temp_dir))  # Set the temporary directory
    cache_gdf(sample_gdf, gdf_hash, identifier)

    assert cache_entry_exists(gdf_hash, identifier) is True
    assert cache_entry_exists(99999, identifier) is False  # Non-existent hash
