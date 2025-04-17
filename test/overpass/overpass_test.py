import pytest
from unittest.mock import patch, MagicMock
from shapely.geometry import Polygon
from mcr_py.overpass.query import (
    order_ways_and_nodes,
    fetch_boundary_polygon,
)  # Replace 'your_module' with the actual module name


@pytest.fixture
def mock_overpass_response():
    """Fixture to create a mock response for the Overpass API."""
    # Create a mock response for the Overpass API
    mock_response = MagicMock()
    node1 = MagicMock(lat=50.9375, lon=6.9603)
    node2 = MagicMock(lat=50.9376, lon=6.9604)
    node3 = MagicMock(lat=50.9377, lon=6.9605)
    node4 = MagicMock(lat=50.938, lon=6.9605)
    mock_response.ways = [
        MagicMock(id=1, nodes=[node1, node2]),
        MagicMock(id=2, nodes=[node2, node3]),
        MagicMock(id=3, nodes=[node3, node4]),
        MagicMock(id=4, nodes=[node4, node1]),
    ]
    return mock_response


def test_order_ways_and_nodes(mock_overpass_response):
    """Test the order_ways_and_nodes function."""
    ordered_nodes = order_ways_and_nodes(mock_overpass_response)

    expected_ordered_nodes = [
        (50.938, 6.9605),
        (50.9375, 6.9603),
        (50.9376, 6.9604),
        (50.9377, 6.9605),
        (50.938, 6.9605),
    ]

    assert ordered_nodes == expected_ordered_nodes


@patch("overpy.Overpass.query")
def test_fetch_boundary_polygon(mock_query, mock_overpass_response):
    """Test the fetch_boundary_polygon function."""
    mock_query.return_value = mock_overpass_response

    # Call the function with a test case
    boundary_polygon = fetch_boundary_polygon("Koeln", 6)

    # Check if the returned polygon is of type Polygon
    assert isinstance(boundary_polygon, Polygon)

    # Check the coordinates of the polygon
    expected_coords = [
        (6.9605, 50.938),
        (6.9603, 50.9375),
        (6.9604, 50.9376),
        (6.9605, 50.9377),
        (6.9605, 50.938),
    ]
    print(list(boundary_polygon.exterior.coords))
    assert list(boundary_polygon.exterior.coords) == expected_coords
