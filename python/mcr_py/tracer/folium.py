import folium
import polars as pl

from mcr_py.mcr.data import ACCURACY_MULTIPLIER
from mcr_py.tracer.tracer import Trace, TraceFootpath, TraceStart, TraceTrip
from mcr_py.utils import strtime


def add_tracer_list_to_folium_map(
    tracers: list[Trace], folium_map: folium.Map, stops_df: pl.DataFrame
) -> None:
    """
    Adds a list of tracer objects to a Folium map, visualizing their locations and paths.

    :param tracers: list[Trace] - A list of tracer objects (TraceStart, TraceFootpath, TraceTrip) to be added to the map.
    :param folium_map: folium.Map - The Folium map object to which the tracers will be added.
    :param stops_df: pl.DataFrame - A DataFrame containing stop information, including stop IDs, latitudes, longitudes, and names.
    :raises ValueError: If an unknown tracer type is encountered.
    """
    stops_df = stops_df.select(
        pl.col("stop_id"), pl.col("stop_lat"), pl.col("stop_lon"), pl.col("stop_name")
    )
    circle_marker_kwargs = {
        "radius": 4,
        "color": "cyan",
        "fill": True,
        "fill_color": "cyan",
    }
    for tracer in tracers:
        if isinstance(tracer, TraceStart):
            _, lat, lon, _ = stops_df.row(
                by_predicate=pl.col("stop_id") == tracer.start_stop_id
            )
            folium.CircleMarker(
                location=[lat, lon],
                popup=tracer.__str__(),
                **circle_marker_kwargs,
            ).add_to(folium_map)
        elif isinstance(tracer, TraceFootpath):
            _, start_stop_lat, start_stop_lon, _ = stops_df.row(
                by_predicate=pl.col("stop_id") == tracer.start_stop_id
            )
            _, end_stop_lat, end_stop_lon, end_stop_name = stops_df.row(
                by_predicate=pl.col("stop_id") == tracer.end_stop_id
            )
            folium.PolyLine(
                locations=[
                    (start_stop_lat, start_stop_lon),
                    (end_stop_lat, end_stop_lon),
                ],
                popup=tracer.__str__(),
                color="blue",
            ).add_to(folium_map)
            folium.CircleMarker(
                location=(end_stop_lat, end_stop_lon),
                popup=f"{end_stop_name} ({tracer.end_stop_id}) duration:{strtime.seconds_to_str_time(tracer.walking_time, ACCURACY_MULTIPLIER)}",
                **circle_marker_kwargs,
            ).add_to(folium_map)
        elif isinstance(tracer, TraceTrip):
            _, start_stop_lat, start_stop_lon, _ = stops_df.row(
                by_predicate=pl.col("stop_id") == tracer.start_stop_id
            )
            _, end_stop_lat, end_stop_lon, end_stop_name = stops_df.row(
                by_predicate=pl.col("stop_id") == tracer.end_stop_id
            )
            folium.PolyLine(
                locations=[
                    (start_stop_lat, start_stop_lon),
                    (end_stop_lat, end_stop_lon),
                ],
                popup=tracer.__str__(),
                color="blue",
            ).add_to(folium_map)
            folium.CircleMarker(
                location=(end_stop_lat, end_stop_lon),
                popup=f"{end_stop_name} ({tracer.end_stop_id}) @ {strtime.seconds_to_str_time(tracer.arrival_time, ACCURACY_MULTIPLIER)}",
                **circle_marker_kwargs,
            ).add_to(folium_map)
        else:
            msg = f"Unknown tracer type {type(tracer).__name__}"
            raise ValueError(msg)
