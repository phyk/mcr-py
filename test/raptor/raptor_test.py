import os.path
import pathlib
import shutil

import polars as pl
from mcr_py.command.raptor import raptor
from mcr_py.tracer.tracer import (
    EnrichedTraceFootpath,
    EnrichedTraceStart,
    EnrichedTraceTrip,
)
from mcr_py.utils import storage, strtime
from mcr_py.utils.output import enrich_raptor_trace_results

NESSELRODE_STR_STOP_ID = "818"
EHRENFELD_BF_STOP_ID = "835"
AMSTERDAMER_STR_STOP_ID = "317"
VENLOER_STR_STOP_ID = "251"

MAX_TRANSFERS = 10
DEFAULT_TRANFER_TIME = 180


def test_raptor(testdata_path: pathlib.Path) -> None:
    output_dir = pathlib.Path(testdata_path) / "output"

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    raptor(
        footpaths=os.path.join(testdata_path, "footpaths.pkl"),
        structs=os.path.join(testdata_path, "structs.pkl"),
        start_stop_id=NESSELRODE_STR_STOP_ID,
        start_time="15:00:00",
        output_dir=str(output_dir),
        max_transfers=MAX_TRANSFERS,
        default_transfer_time=DEFAULT_TRANFER_TIME,
        accuracy_multiplier=1,
    )

    tracer_map = enrich_raptor_trace_results(
        output_dir,
        testdata_path / "gtfs.zip",
    )

    arrival_times = storage.read_df(output_dir / "arrival_times.parquet")

    # all stops are reachable
    assert len(arrival_times.filter(pl.col("arrival_time") == "--:--:--")) == 0
    assert (
        arrival_times.row(by_predicate=pl.col("stop_id") == EHRENFELD_BF_STOP_ID, named=True)[
            "arrival_time"
        ]
        == "15:27:27"
    )

    tracers = tracer_map.tracers[EHRENFELD_BF_STOP_ID]
    assert len(tracers) == 4
    start_trace = tracers[0]
    t16_trace = tracers[1]
    t13_trace = tracers[2]
    footpath_trace = tracers[3]

    assert isinstance(start_trace, EnrichedTraceStart), f"type: {type(start_trace).__name__}"
    assert start_trace.start_stop_id == NESSELRODE_STR_STOP_ID
    assert start_trace.start_time == strtime.str_time_to_seconds(
        "15:00:00", accuracy_multiplier=1
    )

    assert isinstance(t16_trace, EnrichedTraceTrip)
    assert t16_trace.start_stop_id == NESSELRODE_STR_STOP_ID
    assert t16_trace.end_stop_id == AMSTERDAMER_STR_STOP_ID
    assert t16_trace.departure_time == strtime.str_time_to_seconds(
        "15:08:00", accuracy_multiplier=1
    )
    assert t16_trace.arrival_time == strtime.str_time_to_seconds(
        "15:09:00", accuracy_multiplier=1
    )

    assert isinstance(t13_trace, EnrichedTraceTrip)
    assert t13_trace.start_stop_id == AMSTERDAMER_STR_STOP_ID
    assert t13_trace.end_stop_id == VENLOER_STR_STOP_ID
    assert t13_trace.departure_time == strtime.str_time_to_seconds(
        "15:13:00", accuracy_multiplier=1
    )
    assert t13_trace.arrival_time == strtime.str_time_to_seconds(
        "15:25:00", accuracy_multiplier=1
    )

    assert isinstance(footpath_trace, EnrichedTraceFootpath)
    assert footpath_trace.start_stop_id == VENLOER_STR_STOP_ID
    assert footpath_trace.end_stop_id == EHRENFELD_BF_STOP_ID
    assert footpath_trace.walking_time == strtime.str_time_to_seconds(
        "00:02:27", accuracy_multiplier=1
    )
