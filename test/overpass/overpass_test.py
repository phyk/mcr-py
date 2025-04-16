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
    mock_response.ways = [
        MagicMock(
            id=1,
            nodes=[
                MagicMock(lat=50.9375, lon=6.9603),
                MagicMock(lat=50.9376, lon=6.9604),
            ],
        ),
        MagicMock(
            id=2,
            nodes=[
                MagicMock(lat=50.9376, lon=6.9604),
                MagicMock(lat=50.9377, lon=6.9605),
            ],
        ),
        MagicMock(
            id=3,
            nodes=[
                MagicMock(lat=50.9377, lon=6.9605),
                MagicMock(lat=50.9378, lon=6.9606),
            ],
        ),
    ]
    return mock_response


def test_order_ways_and_nodes(mock_overpass_response):
    """Test the order_ways_and_nodes function."""
    ordered_nodes = order_ways_and_nodes(mock_overpass_response)

    expected_ordered_nodes = [
        (50.9375, 6.9603),  # First way
        (50.9376, 6.9604),  # Second way
        (50.9377, 6.9605),  # Third way
        (50.9378, 6.9606),  # Last node
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
        (6.9603, 50.9375),  # First way
        (6.9604, 50.9376),  # Second way
        (6.9605, 50.9377),  # Third way
        (6.9606, 50.9378),  # Last node
    ]
    assert list(boundary_polygon.exterior.coords) == expected_coords
