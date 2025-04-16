import pytest
from unittest.mock import patch
import polars as pl
from mcr_py.package.utils import key
from mcr_py.package.gtfs.read import (
    print_stops,
    get_stops_df,
    print_dataframe,
    format_value,
)  # Replace `your_module` with the actual module name

# Sample DataFrame for testing
sample_df = pl.DataFrame(
    {
        "stop_id": ["1", "2"],
        "stop_name": ["Stop A", "Stop B"],
        "lat": [12.34, 56.78],
        "lon": [98.76, 54.32],
    }
)


@patch("mcr_py.package.gtfs.archive.read_dfs", return_value={key.STOPS_KEY: sample_df})
def test_get_stops_df_zip(mock_read_dfs):
    _ = get_stops_df("test.zip")
    mock_read_dfs.assert_called_once_with("test.zip")


@patch("mcr_py.package.gtfs.archive.read_dfs", return_value={"stops": sample_df})
def test_get_stops_df_directory(mock_read_df):
    _ = get_stops_df("test_directory.zip")
    mock_read_df.assert_called_once_with("test_directory.zip")


def test_get_stops_df_invalid_path():
    with pytest.raises(ValueError, match="Path is neither a zip file nor a directory"):
        get_stops_df("invalid_path")


@patch("mcr_py.package.gtfs.read.get_stops_df", return_value=sample_df)
def test_print_stops(mock_get_stops_df):
    print_stops("test.zip")
    mock_get_stops_df.assert_called_once_with("test.zip")


# Test for print_dataframe
@patch("rich.console.Console.print")
def test_print_dataframe(mock_print):
    print_dataframe(sample_df)
    # Check that the print function was called with the correct table format
    mock_print.assert_called_once()


# Test for format_value
def test_format_value():
    assert format_value(123) == "123"
    assert format_value("test") == "test"
    assert format_value(None) == "None"
