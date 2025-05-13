import pytest
import polars as pl
from unittest.mock import patch
from mcr_py.utils.footpaths import (
    generate,
    GenerationMethod,
)  # Replace 'your_module' with the actual module name


@pytest.fixture
def mock_data():
    # Mock data for nodes, edges, and stops
    nodes = pl.DataFrame(
        {
            "osm_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "long": [
                50.95082420629814,
                50.94788724437913,
                50.94473937866948,
                50.944306400295005,
                50.946735585299706,
                50.94555227853371,
                50.94991721673625,
                50.951705692912014,
                50.95226859486215,
                50.95206669530157,
            ],
            "lat": [
                6.912789559592028,
                6.90965014627875,
                6.912218504273028,
                6.914029522915655,
                6.916940125566271,
                6.919886618643858,
                6.9237266503584465,
                6.926592441418052,
                6.925031390065072,
                6.923008239592917,
            ],
        }
    )
    nodes = nodes.with_columns(pl.col("osm_id").cast(pl.UInt64))
    edges = pl.DataFrame(
        {
            "source_osm": [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 3, 2, 1],
            "dest_osm": [2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6],
            "length": [
                1.5,
                2.0,
                2.5,
                1.0,
                1.8,
                2.2,
                1.3,
                2.4,
                1.6,
                1.7,
                2.1,
                1.9,
                2.3,
                1.4,
                1.8,
            ],  # Fixed lengths for each edge
        }
    )
    stops_df = pl.DataFrame(
        {
            "stop_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "stop_lon": [
                50.952914472192305,
                50.95386602541586,
                50.951875256766186,
                50.95082420629814,
                50.94788724437913,
                50.94473937866948,
                50.944306400295005,
                50.946735585299706,
                50.94555227853371,
                50.94991721673625,
            ],
            "stop_lat": [
                6.923878775814018,
                6.921391875103581,
                6.919528164440663,
                6.912789559592028,
                6.90965014627875,
                6.912218504273028,
                6.914029522915655,
                6.916940125566271,
                6.919886618643858,
                6.9237266503584465,
            ],
        }
    )
    return nodes, edges, stops_df


@patch("mcr_py.utils.storage.read_df")
def test_generate_rustworkx(
    mock_read_df,
    mock_data,
):
    # Arrange
    nodes, edges, stops_df = mock_data
    mock_read_df.side_effect = [nodes, edges, stops_df]  # Mock return values

    footpaths = generate(
        city_name="SampleCity",
        cache_path="/path/to/cache",
        stops_path="/path/to/stops.csv",
        avg_walking_speed=1.4,
        method=GenerationMethod.RUSTWORKX,
    )
    print(footpaths)
    # Assert
    result = {
        1: {4: 1, 5: 2, 6: 2, 7: 4, 8: 3, 9: 2, 10: 4, 3: 4},
        3: {4: 6, 5: 1, 6: 1, 7: 2, 8: 2, 9: 4, 10: 3, 1: 5},
        4: {5: 1, 6: 1, 7: 2, 8: 2, 9: 1, 10: 3, 1: 5, 3: 3},
        5: {4: 5, 6: 6, 7: 1, 8: 1, 9: 3, 10: 2, 1: 4, 3: 5},
        6: {4: 5, 5: 6, 7: 1, 8: 0, 9: 2, 10: 2, 1: 4, 3: 5},
        7: {4: 9, 5: 4, 6: 4, 8: 5, 9: 1, 10: 7, 1: 8, 3: 3},
        8: {4: 4, 5: 5, 6: 5, 7: 7, 9: 5, 10: 1, 1: 3, 3: 7},
        9: {4: 8, 5: 3, 6: 3, 7: 5, 8: 4, 10: 5, 1: 7, 3: 2},
        10: {4: 2, 5: 4, 6: 4, 7: 5, 8: 5, 9: 4, 1: 1, 3: 6},
    }
    assert all(
        [
            footpaths[node][other_node] == result[node][other_node]  # type: ignore
            for node in footpaths
            for other_node in footpaths[node]
        ]
    )
    mock_read_df.assert_called()  # Ensure read_df was called


def test_generation_method_from_str():
    # Test valid method conversion
    assert GenerationMethod.from_str("rustworkx") == GenerationMethod.RUSTWORKX
    assert GenerationMethod.from_str("FAST_PATH") == GenerationMethod.FAST_PATH

    # Test invalid method conversion
    with pytest.raises(ValueError, match="Unknown generation method: invalid_method"):
        GenerationMethod.from_str("invalid_method")


def test_generation_method_all():
    # Test all available methods
    assert GenerationMethod.all() == ["RUSTWORKX", "FAST_PATH"]
