import pathlib

import polars as pl

from mcr_py.gtfs import archive
from mcr_py.tracer.tracer import (
    EnrichedTraceFootpath,
    EnrichedTraceStart,
    EnrichedTraceTrip,
    Trace,
    TraceFootpath,
    TracerMap,
    TraceStart,
    TraceTrip,
)
from mcr_py.utils import key, storage


class TraceEnricher:
    def __init__(
        self, stops_df: pl.DataFrame, trips_df: pl.DataFrame, routes_df: pl.DataFrame
    ) -> None:
        """
        Initializes the TraceEnricher with DataFrames containing stop, trip, and route information.

        :param stops_df: pl.DataFrame - DataFrame containing stop information with stop ID and stop name.
        :param trips_df: pl.DataFrame - DataFrame containing trip information with trip ID, headsign, and route ID.
        :param routes_df: pl.DataFrame - DataFrame containing route information with route ID and short name.
        """
        self.stops_df = stops_df.select([key.STOP_ID_KEY, key.STOP_NAME_KEY])
        self.trips_df = trips_df.select(
            [key.TRIP_ID_KEY, key.TRIP_HEADSIGN_KEY, key.ROUTE_ID_KEY]
        )
        self.routes_df = routes_df.select([key.ROUTE_ID_KEY, key.ROUTE_SHORT_NAME_KEY])

    def enrich_traces(self, traces: list[Trace]) -> list[Trace]:
        """
        Enriches a list of Trace objects by adding additional information from the DataFrames.

        :param traces: list[Trace] - A list of Trace objects to be enriched.
        :returns: list[Trace] - A list of enriched Trace objects.
        """
        new_traces: list[Trace] = []
        for trace in traces:
            new_traces.append(self.enrich_trace(trace))

        return new_traces

    def enrich_trace(self, trace: Trace) -> Trace:
        """
        Determines the type of Trace and enriches it accordingly with additional information.

        :param trace: Trace - A Trace object to be enriched.
        :returns: Trace - An enriched Trace object.
        :raises ValueError: If the Trace type is unknown.
        """
        if isinstance(trace, TraceStart):
            return self.enrich_trace_start(trace)
        elif isinstance(trace, TraceTrip):
            return self.enrich_trace_trip(trace)
        elif isinstance(trace, TraceFootpath):
            return self.enrich_trace_footpath(trace)

        msg = f"Unknown trace type: {type(trace).__name__}"
        raise ValueError(msg)

    def enrich_trace_start(self, trace: TraceStart) -> EnrichedTraceStart:
        """
        Enriches a TraceStart object with stop name information.

        :param trace: TraceStart - A TraceStart object to be enriched.
        :returns: EnrichedTraceStart - An enriched TraceStart object with stop name.
        """
        stop_id = trace.start_stop_id
        _, stop_name = self.stops_df.row(by_predicate=(pl.col(key.STOP_ID_KEY) == stop_id))

        return EnrichedTraceStart(trace, stop_name)

    def enrich_trace_trip(self, trace: TraceTrip) -> EnrichedTraceTrip:
        """
        Enriches a TraceTrip object with start and end stop names, trip headsign, and route short name.

        :param trace: TraceTrip - A TraceTrip object to be enriched.
        :returns: EnrichedTraceTrip - An enriched TraceTrip object with additional trip and route information.
        """
        start_stop_id, end_stop_id = trace.start_stop_id, trace.end_stop_id
        trip_id = trace.trip_id

        _, start_stop = self.stops_df.row(
            by_predicate=(pl.col(key.STOP_ID_KEY) == start_stop_id)
        )
        _, end_stop = self.stops_df.row(by_predicate=(pl.col(key.STOP_ID_KEY) == end_stop_id))
        _, trip_headsign, route_id = self.trips_df.row(
            by_predicate=(pl.col(key.TRIP_ID_KEY) == trip_id)
        )

        _, short_name = self.routes_df.row(by_predicate=(pl.col(key.ROUTE_ID_KEY) == route_id))

        return EnrichedTraceTrip(
            trace,
            f"{short_name} {trip_headsign}",
            start_stop,
            end_stop,
        )

    def enrich_trace_footpath(self, trace: TraceFootpath) -> EnrichedTraceFootpath:
        """
        Enriches a TraceFootpath object with start and end stop names.

        :param trace: TraceFootpath - A TraceFootpath object to be enriched.
        :returns: EnrichedTraceFootpath - An enriched TraceFootpath object with stop names.
        """
        start_stop_id, end_stop_id = trace.start_stop_id, trace.end_stop_id
        _, start_stop = self.stops_df.row(
            by_predicate=(pl.col(key.STOP_ID_KEY) == start_stop_id)
        )
        _, end_stop = self.stops_df.row(by_predicate=(pl.col(key.STOP_ID_KEY) == end_stop_id))

        return EnrichedTraceFootpath(
            trace,
            start_stop,
            end_stop,
        )


def enrich_raptor_trace_results(
    results_dir_path: pathlib.Path, gtfs_dir_path: pathlib.Path
) -> TracerMap:
    """
    Enriches raptor trace results by reading tracer map and GTFS data, then enriching traces with additional information.

    :param results_dir_path: pathlib.Path - The directory path containing raptor trace result files.
    :param gtfs_dir_path: pathlib.Path - The directory path containing GTFS data files.
    :returns: TracerMap - An enriched TracerMap with enriched traces.
    """
    tracer_map_file = storage.read_any_dict(results_dir_path / key.RAPTOR_TRACE_FILE_NAME)
    tracer_map: TracerMap = tracer_map_file[key.TRACER_MAP_KEY]

    dfs = archive.read_dfs(gtfs_dir_path)

    stops_df, trips_df, routes_df = (
        dfs[key.STOPS_KEY],
        dfs[key.TRIPS_KEY],
        dfs[key.ROUTES_KEY],
    )

    trace_enricher = TraceEnricher(stops_df, trips_df, routes_df)

    for stop_id, tracers in tracer_map.tracers.items():
        tracer_map.tracers[stop_id] = trace_enricher.enrich_traces(tracers)

    return tracer_map
