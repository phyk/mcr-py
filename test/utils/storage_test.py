import os
import pickle
from unittest.mock import patch

import mcr_py.utils.storage
import polars as pl
import pytest


@pytest.fixture(scope="session")
def test_dict():
    return {"a": "a", "b": 1.5}


@pytest.fixture(scope="session")
def test_df():
    return pl.DataFrame(
        {
            "foo": [1, 2, 3],
            "bar": [6.0, 7.0, 8.0],
            "ham": ["a", "b", "c"],
        }
    )


def test_get_tmp_path() -> None:
    assert mcr_py.utils.storage.get_tmp_path() == "/tmp/mcr-py"
    assert mcr_py.utils.storage.get_tmp_path("blabla") == "/tmp/mcr-py/blabla"


@patch("requests.get")
def test_download_file(mock_get, tmp_path) -> None:
    url = "http://example.com/test.txt"
    path = os.path.join(tmp_path, "test.txt")
    mock_get.return_value.content = b"file content"

    mcr_py.utils.storage.download_file(url, path)

    assert os.path.exists(path)
    with open(path, "rb") as f:
        assert f.read() == b"file content"


def test_write_dfs_dict(tmp_path, test_df) -> None:
    dfs_dict = {"test": test_df}
    mcr_py.utils.storage.write_dfs_dict(dfs_dict, tmp_path)

    filename = mcr_py.utils.storage.get_df_filename_for_name("test")
    assert os.path.exists(os.path.join(tmp_path, filename))


def test_write_df(tmp_path, test_df) -> None:
    output_path = os.path.join(tmp_path, "test.parquet")
    mcr_py.utils.storage.write_df(test_df, output_path)

    assert os.path.exists(output_path)


def test_get_df_filename_for_name() -> None:
    name = "test"
    expected_filename = "test.parquet"
    assert mcr_py.utils.storage.get_df_filename_for_name(name) == expected_filename


def test_read_df(tmp_path, test_df) -> None:
    path = os.path.join(tmp_path, "test.parquet")
    test_df.write_parquet(path)

    _ = mcr_py.utils.storage.read_df(path)


def test_write_any_dict(tmp_path, test_dict) -> None:
    output_path = os.path.join(tmp_path, "test.pkl")
    mcr_py.utils.storage.write_any_dict(test_dict, output_path)

    assert os.path.exists(output_path)


def test_read_any_dict(tmp_path, test_dict) -> None:
    output_path = os.path.join(tmp_path, "test.pkl")
    with open(output_path, "wb") as f:
        pickle.dump(test_dict, f)

    result = mcr_py.utils.storage.read_any_dict(output_path)
    assert result == test_dict
