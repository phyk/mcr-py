import pytest
from unittest.mock import patch, MagicMock
from mcr_py.utils.footpaths import (
    generate,
    GenerationMethod,
)  # Replace 'your_module' with the actual module name


@pytest.fixture
def mock_data():
    # Mock data for nodes, edges, and stops
    nodes = MagicMock()
    edges = MagicMock()
    stops_df = MagicMock()
    return nodes, edges, stops_df


@patch("mcr_py.utils.storage.read_df")
@patch("mcr_py.osm.graph.create_rx_graph")
@patch("mcr_py.osm.graph.shortest_paths")
@patch("mcr_py.utils.logger.Timed.info")
@patch("mcr_py._mcr_py.add_nearest_node_to_df")
def test_generate_rustworkx(
    mock_add_nearest_node_to_df,
    mock_timed_info,
    mock_shortest_paths,
    mock_create_rx_graph,
    mock_read_df,
    mock_data,
):
    # Arrange
    nodes, edges, stops_df = mock_data
    mock_read_df.side_effect = [nodes, edges, stops_df]  # Mock return values
    mock_create_rx_graph.return_value = (
        nodes,
        edges,
        MagicMock(),
    )  # Mock graph creation
    mock_add_nearest_node_to_df.return_value = stops_df  # Mock nearest node addition
    mock_shortest_paths.return_value = {1: {2: 300, 3: 450}}  # Mock distance data

    # Act
    footpaths = generate(
        city_name="SampleCity",
        cache_path="/path/to/cache",
        stops_path="/path/to/stops.csv",
        avg_walking_speed=1.4,
        method=GenerationMethod.RUSTWORKX,
    )

    # Assert
    assert footpaths == {
        "stop_id_1": {
            "stop_id_2": 214,
            "stop_id_3": 321,
        }  # Replace with expected output
    }
    mock_read_df.assert_called()  # Ensure read_df was called
    mock_create_rx_graph.assert_called()  # Ensure graph creation was called
    mock_shortest_paths.assert_called()  # Ensure shortest paths was called


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
