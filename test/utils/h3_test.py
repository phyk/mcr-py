from unittest.mock import patch

import folium
import polars as pl
from mcr_py.utils.h3 import (
    add_h3_cell_id_to_df,
    add_legend_to_map,
    plot_h3_cells_discrete_colors_on_folium,
    plot_h3_cells_on_folium,
)


def test_add_h3_cell_id_to_df() -> None:
    """
    Tests the add_h3_cell_id_to_df function by verifying the addition of H3 cell ID column.
    """
    df = pl.DataFrame({"lat": [37.7749], "lon": [-122.4194]})
    resolution = 9

    result_df = add_h3_cell_id_to_df(df, resolution)
    lat, lon, cell_id = result_df.row(0)
    assert lat == 37.7749
    assert lon == -122.4194
    assert cell_id == 617700169957507071


def test_plot_h3_cells_discrete_colors_on_folium() -> None:
    """
    Tests the plot_h3_cells_discrete_colors_on_folium function by verifying folium polygon creation.
    """
    h3_cells = {"8428309ffffffff": "A"}
    color_scheme = {"A": "red"}
    folium_map = folium.Map()

    plot_h3_cells_discrete_colors_on_folium(h3_cells, folium_map, color_scheme)
    assert folium_map._children


def test_add_legend_to_map() -> None:
    """
    Tests the add_legend_to_map function by verifying legend addition to folium map.
    """
    folium_map = folium.Map()
    color_scheme = {"A": "red"}

    result_map = add_legend_to_map(folium_map, color_scheme)
    assert result_map._children


def test_plot_h3_cells_on_folium() -> None:
    """
    Tests the plot_h3_cells_on_folium function by verifying folium polygon creation with popup.
    """
    h3_cells = {"8928308280fffff": 10}
    folium_map = folium.Map()

    with patch("folium.Polygon") as mock_polygon:
        plot_h3_cells_on_folium(h3_cells, folium_map, show_legend=True)
        mock_polygon.assert_called_once()
        assert folium_map._children


def test_plot_h3_cells_on_folium_with_popup_callback() -> None:
    """
    Tests the plot_h3_cells_on_folium function by verifying popup callback functionality.
    """
    h3_cells = {"8928308280fffff": 10}
    folium_map = folium.Map()

    def mock_popup_callback(value) -> str:
        return f"Custom popup: {value}"

    with patch("folium.Polygon") as mock_polygon:
        plot_h3_cells_on_folium(h3_cells, folium_map, popup_callback=mock_popup_callback)
        mock_polygon.assert_called_once()
        assert folium_map._children
