import logging
import pathlib
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from mcr_py.tracer.tracer import (
    EnrichedTraceFootpath,
    EnrichedTraceStart,
    EnrichedTraceTrip,
    TraceFootpath,
    TracerMap,
    TraceStart,
    TraceTrip,
)
from mcr_py.utils import key
from mcr_py.utils.output import TraceEnricher, enrich_raptor_trace_results

# Sample data for testing
stops_data = {"stop_id": ["1", "2", "3"], "stop_name": ["Stop A", "Stop B", "Stop C"]}
trips_data = {
    "trip_id": ["10", "20"],
    "trip_headsign": ["Head A", "Head B"],
    "route_id": ["100", "200"],
}
routes_data = {"route_id": ["100", "200"], "route_short_name": ["Route A", "Route B"]}

stops_df = pl.DataFrame(stops_data)
trips_df = pl.DataFrame(trips_data)
routes_df = pl.DataFrame(routes_data)

# Sample traces
trace_start = TraceStart(start_stop_id="1", start_time=0)
trace_trip = TraceTrip(
    start_stop_id="1", end_stop_id="2", departure_time=10, arrival_time=20, trip_id="10"
)
trace_footpath = TraceFootpath(start_stop_id="2", end_stop_id="3", walking_time=50)


@pytest.fixture
def trace_enricher() -> TraceEnricher:
    return TraceEnricher(stops_df, trips_df, routes_df)


def test_enrich_trace_start(trace_enricher: TraceEnricher) -> None:
    enriched_trace = trace_enricher.enrich_trace_start(trace_start)
    assert isinstance(enriched_trace, EnrichedTraceStart)
    assert enriched_trace.start_stop_name == "Stop A"


def test_enrich_trace_trip(trace_enricher: TraceEnricher) -> None:
    enriched_trace = trace_enricher.enrich_trace_trip(trace_trip)
    assert isinstance(enriched_trace, EnrichedTraceTrip)
    assert enriched_trace.start_stop_name == "Stop A"
    assert enriched_trace.end_stop_name == "Stop B"
    assert enriched_trace.trip_name == "Route A Head A"


def test_enrich_trace_footpath(trace_enricher: TraceEnricher) -> None:
    enriched_trace = trace_enricher.enrich_trace_footpath(trace_footpath)
    assert isinstance(enriched_trace, EnrichedTraceFootpath)
    assert enriched_trace.start_stop_name == "Stop B"
    assert enriched_trace.end_stop_name == "Stop C"


@patch("mcr_py.utils.storage.read_any_dict")
@patch("mcr_py.gtfs.archive.read_dfs")
def test_enrich_raptor_trace_results(
    mock_read_dfs: MagicMock, mock_read_any_dict: MagicMock
) -> None:
    mock_read_dfs.return_value = {
        key.STOPS_KEY: stops_df,
        key.TRIPS_KEY: trips_df,
        key.ROUTES_KEY: routes_df,
    }
    tracer_map = TracerMap(stop_ids={"1", "2", "3"})
    tracer_map.add(trace_start)
    tracer_map.add(trace_trip)
    tracer_map.add(trace_footpath)
    mock_read_any_dict.return_value = {key.TRACER_MAP_KEY: tracer_map}

    results_dir_path = pathlib.Path("results")
    gtfs_dir_path = pathlib.Path("gtfs")

    enriched_map = enrich_raptor_trace_results(results_dir_path, gtfs_dir_path)
    enriched_tracers = enriched_map.tracers
    logging.info(tracer_map)
    assert isinstance(enriched_tracers["1"][0], EnrichedTraceStart)
    assert isinstance(enriched_tracers["2"][0], EnrichedTraceStart)
    assert isinstance(enriched_tracers["2"][1], EnrichedTraceTrip)
    assert isinstance(enriched_tracers["3"][-1], EnrichedTraceFootpath)
