import folium
import polars as pl
import pytest
from mcr_py.tracer.folium import add_tracer_list_to_folium_map
from mcr_py.tracer.tracer import TraceFootpath, TraceStart, TraceTrip


@pytest.fixture
def stops_df():
    # Create a sample DataFrame with stop information
    return pl.DataFrame(
        {
            "stop_id": ["stop1", "stop2", "stop3"],
            "stop_lat": [34.0522, 34.0522, 34.0522],
            "stop_lon": [-118.2437, -118.2437, -118.2437],
            "stop_name": ["Start Stop", "End Stop", "Third Stop"],
        }
    )


@pytest.fixture
def folium_map():
    # Create a new Folium Map
    return folium.Map(location=[34.0522, -118.2437], zoom_start=12)


def test_add_trace_start_to_map(folium_map, stops_df) -> None:
    trace_start = TraceStart("stop1", 0)
    add_tracer_list_to_folium_map([trace_start], folium_map, stops_df)

    # Check if the CircleMarker was added for the TraceStart
    assert len(folium_map._children) > 0  # Check that something was adde
    assert any(
        isinstance(child, folium.CircleMarker) for child in folium_map._children.values()
    )


def test_add_trace_footpath_to_map(folium_map, stops_df) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_footpath = TraceFootpath("stop1", "stop2", 30)
    add_tracer_list_to_folium_map([trace_start, trace_footpath], folium_map, stops_df)

    # Check if the PolyLine and CircleMarker were added for the TraceFootpath
    assert len(folium_map._children) > 1  # Check that more than one element was added
    assert any(isinstance(child, folium.PolyLine) for child in folium_map._children.values())
    assert any(
        isinstance(child, folium.CircleMarker) for child in folium_map._children.values()
    )


def test_add_trace_trip_to_map(folium_map, stops_df) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    add_tracer_list_to_folium_map([trace_start, trace_trip], folium_map, stops_df)

    # Check if the PolyLine and CircleMarker were added for the TraceTrip
    assert len(folium_map._children) > 1  # Check that more than one element was added
    assert any(isinstance(child, folium.PolyLine) for child in folium_map._children.values())
    assert any(
        isinstance(child, folium.CircleMarker) for child in folium_map._children.values()
    )


def test_add_multiple_tracer_types_to_map(folium_map, stops_df) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_footpath = TraceFootpath("stop1", "stop2", 30)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    add_tracer_list_to_folium_map(
        [trace_start, trace_footpath, trace_trip], folium_map, stops_df
    )

    # Check if all types of tracers were added
    assert len(folium_map._children) > 2  # Check that more than two elements were added
    assert any(isinstance(child, folium.PolyLine) for child in folium_map._children.values())
    assert any(
        isinstance(child, folium.CircleMarker) for child in folium_map._children.values()
    )


def test_add_unknown_tracer_type(folium_map, stops_df) -> None:
    with pytest.raises(ValueError, match="Unknown tracer type"):
        add_tracer_list_to_folium_map(["invalid_tracer"], folium_map, stops_df)  # type: ignore
