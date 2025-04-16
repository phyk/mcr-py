import pathlib
import polars as pl
from unittest.mock import patch
from mcr_py.package.gtfs.catalog import (
    CATALOG_PATH,
    list_catalog,
    get_catalog,
    download_catalog,
    pre_filter_catalog,
    filter_catalog,
    format_value,
    download,
)


def test_get_catalog():
    catalog = get_catalog()
    assert isinstance(catalog, pl.DataFrame)
    assert catalog.shape[0] == 840  # Expecting 3 rows in the mock data
    assert "data_type" in catalog.columns


def test_pre_filter_catalog():
    catalog = get_catalog()
    filtered_catalog = pre_filter_catalog(catalog)
    assert filtered_catalog.shape[0] == 840  # All entries should be kept


def test_filter_catalog():
    catalog = get_catalog()
    filtered_catalog = filter_catalog(catalog, "US", "", "")
    assert filtered_catalog.shape[0] == 651  # Only US entries should be kept

    filtered_catalog = filter_catalog(catalog, "", "California", "")
    assert filtered_catalog.shape[0] == 154  # Only California entries should be kept

    filtered_catalog = filter_catalog(catalog, "", "", "Los Angeles")
    assert filtered_catalog.shape[0] == 3  # Only Los Angeles entry should be kept


def test_format_value():
    assert format_value(pl.Null) == "Null"
    assert format_value("") == "-"
    assert format_value("Test") == "Test"


def test_download_catalog():
    download_catalog()
    assert pathlib.Path(CATALOG_PATH).exists()


def test_list_catalog(capsys):
    list_catalog("US", "", "")
    captured = capsys.readouterr()
    assert "Total: 651" in captured.out  # Expecting 2 entries for US


@patch("mcr_py.package.utils.storage.download_file")
@patch("mcr_py.package.gtfs.catalog.get_catalog")
def test_download(mock_get_catalog, mock_download_file, capsys):
    mock_catalog = pl.DataFrame(
        {"index": [0], "urls.direct_download": ["http://example.com/feed_a"]}
    )
    mock_get_catalog.return_value = mock_catalog

    download(0, "output_path")
    mock_download_file.assert_called_once_with(
        "http://example.com/feed_a", "output_path"
    )
