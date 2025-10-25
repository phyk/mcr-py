import sys
from typing import Optional

from typing_extensions import Self

from mcr_py.raptor.data import DataQuerier
from mcr_py.tracer.tracer import (
    TraceFootpath,
    TracerMap,
    TraceStart,
    TraceTrip,
)
from mcr_py.utils import strtime
from mcr_py.utils.logger import rlog

MarkedRouteStopTuples = dict[str, tuple[str, int]]
TausPerIteration = dict[int, dict[str, int]]
TausBest = dict[str, int]


class Raptor:
    def __init__(
        self: Self,
        structs_dict: dict,
        footpaths: dict,
        max_transfers: int,
        default_transfer_time: int,
        accuracy_multiplier: int,
    ) -> None:
        """
        Initializes the Raptor algorithm with structured data, footpaths, transfer limits,
        and default transfer time.

        :param structs_dict: dict - A dictionary containing structured data for the algorithm.
        :param footpaths: dict - A dictionary mapping stop IDs to footpath information.
        :param max_transfers: int - The maximum number of transfers allowed.
        :param default_transfer_time: int - The default time to transfer between stops.
        """
        self.dq = DataQuerier(structs_dict, footpaths)

        self.max_transfers = max_transfers
        self.default_transfer_time = default_transfer_time
        self.accuracy_multiplier = accuracy_multiplier

    def run(self, start_stop_id: str, end_stop_id: Optional[str], start_time_str: str):
        """
        Executes the Raptor algorithm from a starting stop to an optional ending stop,
        given a start time.

        :param start_stop_id: str - The identifier of the starting stop.
        :param end_stop_id: Optional[str] - The identifier of the ending stop (if any).
        :param start_time_str: str - The start time in string format.
        :returns: tuple[dict[str, str], TracerMap] - A tuple containing a dictionary of best arrival times
                 in human-readable format and the tracer map.
        """
        start_time = strtime.str_time_to_seconds(
            start_time_str, accuracy_multiplier=self.accuracy_multiplier
        )

        tau_i, tau_best, marked_stops, tracers_map = self.init_vars(start_stop_id, start_time)

        k = 0
        for k in range(1, self.max_transfers + 1):
            rlog.debug(f"iteration {k}")
            tau_i[k] = tau_i[k - 1].copy()

            Q = self.collect_Q(marked_stops)

            marked_stops, tau_i, tau_best, tracers_map = self.process_routes(
                Q, k, tau_i, tau_best, end_stop_id, tracers_map
            )

            (
                additional_marked_stops,
                tau_i,
                tau_best,
                tracers_map,
            ) = self.process_footpaths(
                marked_stops, k, tau_i, tau_best, end_stop_id, start_time, tracers_map
            )

            marked_stops.update(additional_marked_stops)

            rlog.debug(f"marked_stops: {marked_stops}")
            rlog.debug(f"tau_i: {tau_i[k]}")

            if len(marked_stops) == 0:
                break

        rlog.info(f"RAPTOR finished after {k} iterations")
        return seconds_dict_to_times_dict(tau_best, self.accuracy_multiplier), tracers_map

    def collect_Q(self, marked_stops: set[str]) -> dict[str, tuple[str, int]]:
        """
        Collects a dictionary of routes and their corresponding stops and indices from marked stops.

        :param marked_stops: set[str] - A set of stop IDs that have been marked for processing.
        :returns: dict[str, tuple[str, int]] - A dictionary mapping route IDs to their closest stop and index.
        """
        Q: dict[str, tuple[str, int]] = {}
        for stop_id in marked_stops:
            for route_id in self.dq.get_routes_serving_stop(stop_id):
                idx = self.dq.get_idx_of_stop_in_route(stop_id, route_id)
                if route_id not in Q:
                    Q[route_id] = (stop_id, idx)
                    continue

                # if our stop is closer to the start than the existing one, we replace it
                _, existing_idx = Q[route_id]
                if idx < existing_idx:
                    Q[route_id] = (stop_id, idx)
        return Q

    def process_routes(
        self: Self,
        Q: MarkedRouteStopTuples,
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        end_stop_id: Optional[str],
        tracers_map: TracerMap,
    ) -> tuple[set[str], TausPerIteration, TausBest, TracerMap]:
        """
        Processes the routes based on the collected data and updates the bags and marked stops.

        :param Q: MarkedRouteStopTuples - A dictionary of routes with their closest stops and indices.
        :param k: int - The current iteration count.
        :param tau_i: TausPerIteration - A dictionary mapping iteration counts to arrival times.
        :param tau_best: TausBest - A dictionary mapping stop IDs to best arrival times.
        :param end_stop_id: Optional[str] - The identifier of the ending stop (if any).
        :param tracers_map: TracerMap - The tracer map to track trips.
        :returns: tuple[set[str], TausPerIteration, TausBest, TracerMap] - A tuple containing updated marked stops,
                 tau_i, tau_best, and tracers_map.
        """
        marked_stops = set()

        for route_id, (stop_id, idx) in Q.items():
            trip_id: Optional[str] = None

            tracers_map.clear_last_hop()

            for stop_id in self.dq.iterate_stops_in_route_from_idx(route_id, idx):
                trip_id, tau_i, tau_best, tracers_map = self.process_route(
                    route_id,
                    trip_id,
                    stop_id,
                    marked_stops,
                    k,
                    tau_i,
                    tau_best,
                    end_stop_id,
                    tracers_map,
                )

        return marked_stops, tau_i, tau_best, tracers_map

    def process_route(
        self: Self,
        route_id: str,
        trip_id: Optional[str],
        stop_id: str,
        marked_stops: set[str],
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        end_stop_id: Optional[str],
        tracers_map: TracerMap,
    ) -> tuple[Optional[str], TausPerIteration, TausBest, TracerMap]:
        """
        Processes a specific route by updating arrival times and merging bags.

        :param route_id: str - The identifier of the route.
        :param trip_id: Optional[str] - The identifier of the current trip (if any).
        :param stop_id: str - The identifier of the stop.
        :param marked_stops: set[str] - A set of marked stops to be updated.
        :param k: int - The current iteration count.
        :param tau_i: TausPerIteration - A dictionary mapping iteration counts to arrival times.
        :param tau_best: TausBest - A dictionary mapping stop IDs to best arrival times.
        :param end_stop_id: Optional[str] - The identifier of the ending stop (if any).
        :param tracers_map: TracerMap - The tracer map to track trips.
        :returns: tuple[Optional[str], TausPerIteration, TausBest, TracerMap] - A tuple containing the updated trip ID,
                 tau_i, tau_best, and tracers_map.
        """
        tau_best_end_stop_id = tau_best[end_stop_id] if end_stop_id else sys.maxsize

        if trip_id is not None and self.dq.get_arrival_time(trip_id, stop_id) < min(
            tau_best[stop_id], tau_best_end_stop_id
        ):
            self.update_tau_through_trip(trip_id, stop_id, k, tau_i, tau_best, tracers_map)
            marked_stops.add(stop_id)

        ready_to_depart = tau_i[k - 1][stop_id] + self.default_transfer_time
        if ready_to_depart < self.dq.get_departure_time(trip_id, stop_id):
            new_trip_id = self.find_earlier_trip(
                route_id, stop_id, ready_to_depart, tracers_map
            )
            trip_id = new_trip_id if new_trip_id is not None else trip_id

        return trip_id, tau_i, tau_best, tracers_map

    def update_tau_through_trip(
        self: Self,
        trip_id: str,
        stop_id: str,
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        tracers_map: TracerMap,
    ) -> None:
        """
        Updates the arrival times through a specific trip and logs the tracer information.

        :param trip_id: str - The identifier of the trip.
        :param stop_id: str - The identifier of the stop.
        :param k: int - The current iteration count.
        :param tau_i: TausPerIteration - A dictionary mapping iteration counts to arrival times.
        :param tau_best: TausBest - A dictionary mapping stop IDs to best arrival times.
        :param tracers_map: TracerMap - The tracer map to track trips.
        """
        arrival_time = self.dq.get_arrival_time(trip_id, stop_id)
        tau_i[k][stop_id] = arrival_time
        tau_best[stop_id] = arrival_time

        hop_on_stop_id, hop_on_time = tracers_map.get_last_hop()
        if hop_on_stop_id is None or hop_on_time is None:
            msg = "hop_on_stop_id or hop_on_time should not be None if trip_id is not None"
            raise Exception(msg)
        tracers_map.add(
            TraceTrip(
                start_stop_id=hop_on_stop_id,
                end_stop_id=stop_id,
                departure_time=hop_on_time,
                arrival_time=arrival_time,
                trip_id=trip_id,
            ),
        )

    def find_earlier_trip(
        self: Self,
        route_id: str,
        stop_id: str,
        ready_to_depart: int,
        tracers_map: TracerMap,
    ) -> Optional[str]:
        """
        Finds the earliest trip for a given route and stop that departs after a specified time.

        :param route_id: str - The identifier of the route.
        :param stop_id: str - The identifier of the stop.
        :param ready_to_depart: int - The time at which we are ready to depart.
        :param tracers_map: TracerMap - The tracer map to track trips.
        :returns: Optional[str] - The identifier of the earliest trip found, or None if no trip is available.
        """
        result = self.dq.earliest_trip(
            route_id,
            stop_id,
            ready_to_depart,
        )
        if result is None:
            # we could not find a new trip, so we return as is
            return None
        trip_id, hop_on_time = result
        hop_on_stop_id = stop_id
        tracers_map.update_last_hop(hop_on_stop_id, hop_on_time)

        return trip_id

    def process_footpaths(
        self: Self,
        marked_stops: set[str],
        k: int,
        tau_i: TausPerIteration,
        tau_best: TausBest,
        end_stop_id: Optional[str],
        start_time: int,
        tracers_map: TracerMap,
    ) -> tuple[set[str], TausPerIteration, TausBest, TracerMap]:
        """
        Processes footpaths from marked stops and updates the bags accordingly.

        :param marked_stops: set[str] - A set of stop IDs that have been marked for processing.
        :param k: int - The current iteration count.
        :param tau_i: TausPerIteration - A dictionary mapping iteration counts to arrival times.
        :param tau_best: TausBest - A dictionary mapping stop IDs to best arrival times.
        :param end_stop_id: Optional[str] - The identifier of the ending stop (if any).
        :param start_time: int - The start time in seconds.
        :param tracers_map: TracerMap - The tracer map to track trips.
        :returns: tuple[set[str], TausPerIteration, TausBest, TracerMap] - A tuple containing updated marked stops,
                 tau_i, tau_best, and tracers_map.
        """
        additional_marked_stops = set()
        for stop_id in marked_stops:
            for nearby_stop_id, walking_time in self.dq.iterate_footpaths_from_stop(stop_id):
                nearby_stop_arrival_time = tau_i[k][stop_id] + walking_time

                tau_best_end_stop_id = tau_best[end_stop_id] if end_stop_id else sys.maxsize
                # we have a couple of modifications here:
                # 1. we only mark the stop if tau is actually updated
                # 2. we use tau_best instead of tau_k (should not make a difference)
                # 3. we also consider tau_best[end_stop_id] just like in the stop before
                # 4. we also update tau_best
                if nearby_stop_arrival_time < min(
                    tau_best[nearby_stop_id], tau_best_end_stop_id
                ):
                    assert isinstance(nearby_stop_arrival_time, int)
                    assert nearby_stop_arrival_time >= start_time
                    tau_i[k][nearby_stop_id] = nearby_stop_arrival_time
                    tau_best[nearby_stop_id] = nearby_stop_arrival_time
                    additional_marked_stops.add(nearby_stop_id)

                    tracers_map.add(
                        TraceFootpath(stop_id, nearby_stop_id, walking_time),
                    )
        return additional_marked_stops, tau_i, tau_best, tracers_map

    def init_vars(
        self, start_stop_id: str, start_time: int
    ) -> tuple[dict[int, dict[str, int]], dict[str, int], set[str], TracerMap]:
        """
        Initializes variables for the algorithm, including arrival times for each stop.

        :param start_stop_id: str - The identifier of the starting stop.
        :param start_time: int - The start time in seconds.
        :returns: tuple[dict[int, dict[str, int]], dict[str, int], set[str], TracerMap] - A tuple containing the initialized arrival times, best arrival times, marked stops, and tracer map.
        """
        tau_i: dict[int, dict[str, int]] = {
            0: {},
        }
        tracer = TracerMap(self.dq.get_stop_ids())
        tau_best = {}
        marked_stops = set()

        for stop_id in self.dq.get_stop_ids():
            tau_i[0][stop_id] = sys.maxsize
            tau_best[stop_id] = sys.maxsize

        tau_i[0][start_stop_id] = start_time
        tau_best[start_stop_id] = start_time
        tracer.add(
            tracer=TraceStart(start_stop_id, start_time),
        )

        marked_stops.add(start_stop_id)

        return tau_i, tau_best, marked_stops, tracer


def seconds_dict_to_times_dict(
    seconds_dict: dict[str, int], accuracy_multiplier: int
) -> dict[str, str]:
    """
    Converts a dictionary of arrival times in seconds to a human-readable format.

    :param seconds_dict: dict[str, int] - A dictionary mapping stop IDs to arrival times in seconds.
    :returns: dict[str, str] - A dictionary mapping stop IDs to their arrival times in human-readable format.
    """
    return {
        stop_id: strtime.seconds_to_str_time(seconds, accuracy_multiplier)
        for stop_id, seconds in seconds_dict.items()
    }
