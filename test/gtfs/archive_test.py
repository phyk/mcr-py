import os
import zipfile

import polars as pl
import pytest
from mcr_py.gtfs.archive import get_gtfs_filename, read_dfs, write_dfs


@pytest.fixture
def gtfs_zip(testdata_path):
    # Create a path to the GTFS zip file
    return os.path.join(testdata_path, "gtfs.zip")


def test_get_gtfs_filename() -> None:
    assert get_gtfs_filename("stops") == "stops.txt"
    assert get_gtfs_filename("trips") == "trips.txt"
    assert get_gtfs_filename("stop_times") == "stop_times.txt"


def test_read_dfs(gtfs_zip) -> None:
    dfs = read_dfs(gtfs_zip)

    # Check that the expected DataFrames are returned
    assert isinstance(dfs, dict)
    assert "stops" in dfs
    assert "trips" in dfs
    assert "stop_times" in dfs
    assert "calendar" in dfs
    assert "routes" in dfs

    # Validate the structure of the DataFrames
    assert isinstance(dfs["stops"], pl.DataFrame)
    assert isinstance(dfs["trips"], pl.DataFrame)
    assert isinstance(dfs["stop_times"], pl.DataFrame)
    assert isinstance(dfs["calendar"], pl.DataFrame)
    assert isinstance(dfs["routes"], pl.DataFrame)


def test_read_dfs_missing_file(gtfs_zip, tmp_path) -> None:
    # Create a zip file with a missing expected file for testing
    with zipfile.ZipFile(gtfs_zip, "r") as zip_ref:
        # Create a temporary zip file without one of the expected files
        with zipfile.ZipFile(tmp_path / "test_missing.zip", "w") as temp_zip:
            for file in zip_ref.namelist():
                if file != "stops.txt":  # Intentionally omit the stops file
                    temp_zip.writestr(file, zip_ref.read(file))

    with pytest.raises(Exception, match="Expected file stops.txt not in zip file"):
        read_dfs(tmp_path / "test_missing.zip")


def test_write_dfs(gtfs_zip, tmp_path) -> None:
    # Read existing dataframes from the zip file
    dfs = read_dfs(gtfs_zip)

    # Write the dataframes to a new zip file
    output_zip = tmp_path / "test_output.zip"
    write_dfs(dfs, output_zip)

    # Verify that the new zip file contains the expected files
    with zipfile.ZipFile(output_zip, "r") as zip_ref:
        contained = zip_ref.namelist()
        for expected_file in [
            "stops.txt",
            "trips.txt",
            "stop_times.txt",
            "calendar.txt",
            "routes.txt",
        ]:
            assert expected_file in contained

    # Clean up the output zip file
    os.remove(output_zip)
