from multiprocessing import Pool
from typing import Callable, Optional

import folium
import numpy as np
import pandas as pd
from branca.colormap import LinearColormap
from branca.element import MacroElement, Template
from h3 import h3

from mcr_py.package import key


def add_h3_cell_id_to_df(df: pd.DataFrame, resolution: int) -> pd.DataFrame:
    """
    Add a column to the dataframe with the H3 cell ID for the given resolution.
    Expected columns are "lat" and "lon".
    """
    df["h3_cell_id"] = df.apply(
        lambda row: h3.geo_to_h3(row["lat"], row["lon"], resolution), axis=1
    )
    return df


def process_batch(df_batch, resolution):
    df_batch["h3_cell_id"] = df_batch.apply(
        lambda row: h3.geo_to_h3(row["lat"], row["lon"], resolution), axis=1
    )
    return df_batch


def add_h3_cell_id_to_df_with_batching(
    df: pd.DataFrame, resolution: int, n_batches: int
) -> pd.DataFrame:
    dfs = np.array_split(df, n_batches)

    with Pool(processes=key.DEFAULT_N_PROCESSES) as pool:
        results = pool.starmap(
            process_batch, [(df_batch, resolution) for df_batch in dfs]
        )

    return pd.concat(results, ignore_index=True)


def plot_h3_cells_discrete_colors_on_folium(
    h3_cells: dict[str, str],
    folium_map: folium.Map,
    color_scheme: dict[str, str],
    fill_opacity: float = 1,
):
    for h3_cell in h3_cells:
        geo_boundary = list(h3.h3_to_geo_boundary(h3_cell))
        geo_boundary.append(geo_boundary[0])

        value = h3_cells[h3_cell]
        color = color_scheme[value]

        folium.Polygon(
            locations=geo_boundary,
            color=color,
            weight=0.2,
            opacity=1,
            fill_opacity=fill_opacity,
            fill_color=color,
        ).add_to(folium_map)
    add_legend_to_map(folium_map, color_scheme, fill_opacity)


def add_legend_to_map(
    folium_map: folium.Map,
    color_scheme: dict[str, str],
    opacity: float = 1,
):
    template = """
{% macro html(this, kwargs) %}

<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>jQuery UI Draggable - Default functionality</title>
  <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">

  <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
  <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>
  
  <script>
  $( function() {
    $( "#maplegend" ).draggable({
                    start: function (event, ui) {
                        $(this).css({
                            right: "auto",
                            top: "auto",
                            bottom: "auto"
                        });
                    }
                });
});

  </script>
</head>
<body>

 
<div id='maplegend' class='maplegend' 
    style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
     border-radius:6px; padding: 10px; font-size:16px; right: 20px; top: 20px;'>
     
<div class='legend-title'>Legend</div>
<div class='legend-scale'>
  <ul class='legend-labels'>"""
    template_part_2 = """
  </ul>
</div>
</div>
 
</body>
</html>

<style type='text/css'>
  .maplegend .legend-title {
    text-align: left;
    margin-bottom: 5px;
    font-weight: bold;
    font-size: 90%;
    }
  .maplegend .legend-scale ul {
    margin: 0;
    margin-bottom: 5px;
    padding: 0;
    float: left;
    list-style: none;
    }
  .maplegend .legend-scale ul li {
    font-size: 80%;
    list-style: none;
    margin-left: 0;
    line-height: 18px;
    margin-bottom: 2px;
    }
  .maplegend ul.legend-labels li span {
    display: block;
    float: left;
    height: 16px;
    width: 30px;
    margin-right: 5px;
    margin-left: 0;
    border: 1px solid #999;
    }
  .maplegend .legend-source {
    font-size: 80%;
    color: #777;
    clear: both;
    }
  .maplegend a {
    color: #777;
    }
</style>
{% endmacro %}"""

    for key_, value in color_scheme.items():
        template += (
            f'<li><span style="background:{value};opacity:{opacity}"></span>{key_}</li>'
        )

    template += template_part_2
    macro = MacroElement()
    macro._template = Template(template)

    folium_map.get_root().add_child(macro)
    return folium_map


def plot_h3_cells_on_folium(
    h3_cells: set[str] | dict[str, int],
    folium_map: folium.Map,
    reverse_color: bool = False,
    popup_callback: Optional[Callable] = None,
    color: str = "blue",
    maximum: Optional[int] = None,
    show_legend: bool = False,
    legend_color_map=None,
    legend_is_scaled: bool = False,
    legend_value_callback: Optional[Callable] = None,
    legend_caption: str = "",
) -> None:
    is_dict = isinstance(h3_cells, dict)
    maximum_value = (
        maximum if maximum is not None else (max(h3_cells.values()) if is_dict else 0)
    )

    if show_legend:
        colormap = legend_color_map or LinearColormap([(255, 255, 255, 0), color])
        if not legend_is_scaled:
            if legend_value_callback:
                maximum_value_legend = legend_value_callback(maximum_value)
            else:
                maximum_value_legend = maximum_value
            colormap = colormap.scale(0, maximum_value_legend)
        colormap.caption = legend_caption
        colormap.add_to(folium_map)

    for h3_cell in h3_cells:
        geo_boundary = list(h3.h3_to_geo_boundary(h3_cell))
        geo_boundary.append(geo_boundary[0])

        opacity = 0
        value = None
        if is_dict:
            value = h3_cells[h3_cell]
            opacity = value / maximum_value
            if reverse_color:
                opacity = 1 - opacity

        popup = None
        if popup_callback:
            if popup_callback.__code__.co_argcount == 1:
                popup = popup_callback(value)
            elif popup_callback.__code__.co_argcount == 2:
                popup = popup_callback(h3_cell, value)
        else:
            popup = (f"Value: {value}" if value else None,)

        folium.Polygon(
            locations=geo_boundary,
            color=color,
            weight=0.2,
            opacity=1,
            fill_color=color,
            fill_opacity=opacity,
            popup=popup,
        ).add_to(folium_map)
