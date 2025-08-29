import pytest
from mcr_py.tracer.tracer import TraceFootpath, TracerMap, TraceStart, TraceTrip


@pytest.fixture
def tracer_map() -> TracerMap:
    return TracerMap({"stop1", "stop2", "stop3"})


def test_tracer_map_add_trace_start(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    tracer_map.add(trace_start)
    assert tracer_map["stop1"] == [trace_start]


def test_tracer_map_add_trace_trip(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    tracer_map.add(trace_start)
    tracer_map.add(trace_trip)
    assert tracer_map["stop1"] == [trace_start]
    assert tracer_map["stop2"] == [trace_start, trace_trip]


def test_tracer_map_add_trace_footpath(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    trace_footpath = TraceFootpath("stop2", "stop3", 30)
    tracer_map.add(trace_start)
    tracer_map.add(trace_trip)
    tracer_map.add(trace_footpath)
    assert tracer_map["stop1"] == [trace_start]
    assert tracer_map["stop2"] == [trace_start, trace_trip]
    assert tracer_map["stop3"] == [trace_start, trace_trip, trace_footpath]


def test_tracer_map_add_multiple_tracers(tracer_map: TracerMap) -> None:
    trace_start1 = TraceStart("stop1", 0)
    trace_start2 = TraceStart("stop2", 0)
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    tracer_map.add(trace_start1)
    tracer_map.add(trace_start2)
    tracer_map.add(trace_trip)
    assert tracer_map["stop1"] == [trace_start1]
    assert tracer_map["stop2"] == [trace_start1, trace_trip]


def test_tracer_map_add_trace_trip_without_start(tracer_map: TracerMap) -> None:
    trace_trip = TraceTrip("stop1", 10, "stop2", 20, "trip1")
    with pytest.raises(
        ValueError, match="The first tracer for stop stop1 must be a TraceStart"
    ):
        tracer_map.add(trace_trip)


def test_tracer_map_add_unknown_tracer_type(tracer_map: TracerMap) -> None:
    with pytest.raises(ValueError, match="Unknown tracer type: <class 'str'>"):
        # Attempt to add an invalid type
        tracer_map.add("invalid_tracer_type")  # type: ignore


def test_tracer_map_update_last_hop(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    tracer_map.add(trace_start)
    tracer_map.update_last_hop("stop1", 100)
    assert tracer_map.last_hop_on_stop_id == "stop1"
    assert tracer_map.last_hop_on_time == 100


def test_tracer_map_clear_last_hop(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    tracer_map.add(trace_start)
    tracer_map.update_last_hop("stop1", 100)
    tracer_map.clear_last_hop()
    assert tracer_map.last_hop_on_stop_id is None
    assert tracer_map.last_hop_on_time is None


def test_tracer_map_get_last_hop(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    tracer_map.add(trace_start)
    tracer_map.update_last_hop("stop1", 100)
    last_hop = tracer_map.get_last_hop()
    assert last_hop == ("stop1", 100)


def test_tracer_map_str(tracer_map: TracerMap) -> None:
    trace_start = TraceStart("stop1", 0)
    tracer_map.add(trace_start)
    expected_str = "stop1: Start at stop1 at 00:00:00\nstop2: \nstop3: "
    assert str(tracer_map) == expected_str
