"""GTFS line metadata (name + vehicle type) for the indexed mcr-rust GTFS parquets.

The indexed parquets under ``gtfs_clean/<city>/`` only carry dense integers
(``route_idx``/``trip_idx``/``stop_idx``). The cropped GTFS zip next to them
(``gtfs_clean/<city>.zip``) still has the human-readable ``routes.txt`` /
``stops.txt``. ``load_gtfs_lines`` re-runs ``mcr_py.gtfs.clean.clean`` on that
zip and replays the id -> idx assignment of ``mcr_py.gtfs.index`` to attach a
line name and a vehicle type to every ``route_idx``.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

import polars as pl

from mcr_py.gtfs import clean
from mcr_py.gtfs.snap import STOP_NODES_FILE
from mcr_py.utils import key

# GTFS route_type (basic + common extended codes) -> display type
_BASIC_TYPES = {
    0: "Tram",
    1: "Subway",
    2: "Train",
    3: "Bus",
    4: "Ferry",
    5: "Cable tram",
    6: "Gondola",
    7: "Funicular",
    11: "Trolleybus",
    12: "Monorail",
}

# Extended route types: (first, last, display type)
_EXTENDED_TYPES = (
    (100, 199, "Train"),
    (200, 299, "Bus"),
    (400, 499, "Subway"),
    (700, 799, "Bus"),
    (800, 899, "Trolleybus"),
    (900, 999, "Tram"),
    (1000, 1099, "Ferry"),
)

# Display order + colours (RGBA) for the layers / legend.
TYPE_COLOR: dict[str, list[int]] = {
    "Train": [190, 18, 60, 220],
    "Subway": [29, 78, 216, 220],
    "Tram": [8, 145, 178, 220],
    "Bus": [126, 34, 206, 200],
    "Ferry": [2, 132, 199, 220],
    "Other": [100, 116, 139, 200],
}
TYPE_WIDTH: dict[str, int] = {
    "Train": 4,
    "Subway": 4,
    "Tram": 3,
    "Bus": 2,
    "Ferry": 3,
    "Other": 2,
}


def route_type_name(route_type: int) -> str:
    """Display type for a GTFS ``route_type`` code; unknown codes -> ``Other``."""
    if route_type in _BASIC_TYPES:
        name = _BASIC_TYPES[route_type]
    else:
        name = next((n for lo, hi, n in _EXTENDED_TYPES if lo <= route_type <= hi), "Other")
    return name if name in TYPE_COLOR else "Other"


@dataclass
class GtfsLines:
    """Indexed GTFS with line info.

    stops:        stop_idx, stop_lat, stop_lon, stop_name
    routes:       route_idx, line, type   (``line`` = route_short_name)
    route_stops:  route_idx -> ordered stop_idx list (from the first trip of the route)
    """

    stops: pl.DataFrame
    routes: pl.DataFrame
    route_stops: dict[int, list[int]]


def load_gtfs_lines(gtfs_dir: pathlib.Path) -> GtfsLines:
    """Load line names/types for ``gtfs_dir`` (indexed parquets) from its sibling ``.zip``."""
    zip_path = gtfs_dir.with_suffix(".zip")
    dfs = clean.clean(zip_path)
    trips_df, stops_df, routes_df = dfs[key.TRIPS_KEY], dfs[key.STOPS_KEY], dfs[key.ROUTES_KEY]

    # Same assignment as `gtfs_to_indexed_parquet`.
    stops = (
        stops_df.select("stop_id", "stop_name", "stop_lat", "stop_lon")
        .sort("stop_id")
        .with_row_index("stop_idx")
        .with_columns(pl.col("stop_idx").cast(pl.UInt32))
    )
    route_ids = (
        trips_df.select("route_id")
        .unique(maintain_order=True)
        .sort("route_id")
        .with_row_index("route_idx")
        .with_columns(pl.col("route_idx").cast(pl.UInt32))
    )
    info = (
        routes_df.drop_nulls("route_id")
        .unique("route_id")
        .select(
            "route_id",
            pl.col("route_short_name").fill_null(pl.col("route_long_name")).alias("line"),
            pl.col("route_type")
            .map_elements(route_type_name, return_dtype=pl.String)
            .alias("type"),
        )
    )
    routes = route_ids.join(info, on="route_id", how="left").select(
        "route_idx",
        pl.col("line").fill_null("?"),
        pl.col("type").fill_null("Other"),
    )

    # Stop sequence per route: first trip of each route in the indexed parquets.
    trips = pl.read_parquet(gtfs_dir / "trips.parquet")
    first_trip = trips.group_by("route_idx").agg(pl.col("trip_idx").min())
    seq = (
        pl.read_parquet(gtfs_dir / "stop_times.parquet")
        .join(first_trip, left_on="trip_idx", right_on="trip_idx", how="inner")
        .sort("trip_idx", "stop_sequence")
        .group_by("route_idx", maintain_order=True)
        .agg(pl.col("stop_idx"))
    )
    route_stops = dict(zip(seq["route_idx"].to_list(), seq["stop_idx"].to_list(), strict=True))
    return GtfsLines(stops=stops, routes=routes, route_stops=route_stops)


def connections_in_cells(
    gtfs: GtfsLines,
    cells: frozenset[str],
    resolution: int = 9,
) -> pl.DataFrame:
    """Stop-to-stop connections with both ends inside ``cells``.

    One row per unordered stop pair and vehicle type, with the sorted, comma-joined
    ``lines`` that serve it. Columns: type, lines, a_lat, a_lon, b_lat, b_lon,
    a_name, b_name.
    """
    import h3

    stops = gtfs.stops
    stop_cell = {
        idx: h3.latlng_to_cell(lat, lon, resolution)
        for idx, lat, lon in stops.select("stop_idx", "stop_lat", "stop_lon").iter_rows()
    }
    in_area = {idx for idx, c in stop_cell.items() if c in cells}

    line_of = dict(zip(gtfs.routes["route_idx"], gtfs.routes["line"], strict=True))
    type_of = dict(zip(gtfs.routes["route_idx"], gtfs.routes["type"], strict=True))

    pairs: dict[tuple[int, int, str], set[str]] = {}
    for ridx, stop_list in gtfs.route_stops.items():
        vehicle = type_of[ridx]
        for a, b in zip(stop_list, stop_list[1:], strict=False):
            if a in in_area and b in in_area and a != b:
                pairs.setdefault((min(a, b), max(a, b), vehicle), set()).add(line_of[ridx])

    pos = {
        idx: (lat, lon, name)
        for idx, lat, lon, name in stops.select(
            "stop_idx", "stop_lat", "stop_lon", "stop_name"
        ).iter_rows()
    }
    rows = []
    for (a, b, vehicle), lines in pairs.items():
        rows.append(
            {
                "type": vehicle,
                "lines": ", ".join(sorted(lines, key=_natural_key)),
                "a_lat": pos[a][0],
                "a_lon": pos[a][1],
                "a_name": pos[a][2],
                "b_lat": pos[b][0],
                "b_lon": pos[b][1],
                "b_name": pos[b][2],
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "type": pl.String,
            "lines": pl.String,
            "a_lat": pl.Float64,
            "a_lon": pl.Float64,
            "a_name": pl.String,
            "b_lat": pl.Float64,
            "b_lon": pl.Float64,
            "b_name": pl.String,
        },
    )


def _natural_key(s: str) -> tuple[int, int, str]:
    """Sort '2' < '10' < 'RE1'."""
    return (0, int(s), s) if s.isdigit() else (1, 0, s)


class LegMatcher:
    """Identify which GTFS line(s) a public-transport leg of a route path rode.

    A leg is the run of consecutive path nodes tagged public transport; each node is
    the OSM node a stop snapped to (``stop_nodes.parquet``, the same mapping the router
    used). Every route that visits the leg's stops in order is a candidate.
    """

    def __init__(self, gtfs: GtfsLines, gtfs_dir: pathlib.Path) -> None:
        self._gtfs = gtfs
        stop_nodes = pl.read_parquet(gtfs_dir / STOP_NODES_FILE)
        self._node_stops: dict[int, set[int]] = {}
        for stop_idx, osm_id in stop_nodes.select("stop_idx", "osm_id").iter_rows():
            self._node_stops.setdefault(osm_id, set()).add(stop_idx)
        self._line_type = {
            r: (line, typ)
            for r, line, typ in gtfs.routes.select("route_idx", "line", "type").iter_rows()
        }
        self._stop_routes: dict[int, set[int]] = {}
        for ridx, stop_list in gtfs.route_stops.items():
            for s in stop_list:
                self._stop_routes.setdefault(s, set()).add(ridx)

    def match(self, node_ids: list[int]) -> tuple[str, str]:
        """(lines, type) for a leg given its OSM node id sequence.

        ``lines`` is the comma-joined candidate lines (several if the timetable alone
        can't tell them apart); ``type`` is the single vehicle type or ``/``-joined.
        """
        node_stops = [self._node_stops.get(n, set()) for n in node_ids]
        if not node_stops or any(not s for s in node_stops):
            return "?", "Other"
        cands = set.intersection(
            *(set().union(*(self._stop_routes.get(s, set()) for s in stops)) for stops in node_stops)
        )
        matched = [r for r in cands if self._visits_in_order(r, node_stops)]
        if not matched:
            return "?", "Other"
        lines = sorted({self._line_type[r][0] for r in matched}, key=_natural_key)
        types = sorted({self._line_type[r][1] for r in matched})
        return ", ".join(lines), "/".join(types)

    def _visits_in_order(self, ridx: int, node_stops: list[set[int]]) -> bool:
        stop_list = self._gtfs.route_stops[ridx]
        i = 0
        for s in stop_list:
            if s in node_stops[i]:
                i += 1
                if i == len(node_stops):
                    return True
        return False
