from typing import Generic, Optional

from typing_extensions import Any, Self

from mcr_py.raptor.bag import Bag, L, RouteBag
from mcr_py.raptor.data import ExpandedDataQuerier
from mcr_py.tracer.tracer import (
    TracerMap,
    TraceStart,
)
from mcr_py.utils import strtime
from mcr_py.utils.key import S, T
from mcr_py.utils.logger import rlog


class McRaptor(Generic[L, S, T]):
    def __init__(
        self,
        structs_dict: dict,
        footpaths: dict,
        max_transfers: int,
        default_transfer_time: int,
        additional_stop_information: dict[str, S],
        additional_trip_information: dict[str, T],
        label_class: type[L],
        accuracy_multiplier: int,
    ) -> None:
        """
        Initializes the McRaptor algorithm with structured data, footpaths, transfer limits,
        and additional information for stops and trips.

        :param structs_dict: dict - A dictionary containing structured data for the algorithm.
        :param footpaths: dict - A dictionary mapping stop IDs to footpath information.
        :param max_transfers: int - The maximum number of transfers allowed.
        :param default_transfer_time: int - The default time to transfer between stops.
        :param additional_stop_information: dict[str, S] - A dictionary mapping stop IDs to additional information.
        :param additional_trip_information: dict[str, T] - A dictionary mapping trip IDs to additional information.
        :param label_class: type[L] - The class type for labels used in the algorithm.
        """
        self.dq = ExpandedDataQuerier(
            structs_dict,
            footpaths,
            additional_stop_information,
            additional_trip_information,
        )

        self.max_transfers = max_transfers
        self.default_transfer_time = default_transfer_time

        self.label_class = label_class
        self.accuracy_multiplier = accuracy_multiplier

    def run(
        self, start_stop_id: str, end_stop_id: Optional[str], start_time_str: str
    ) -> dict[str, Any]:
        """
        Executes the McRaptor algorithm from a starting stop to an optional ending stop,
        given a start time.

        :param start_stop_id: str - The identifier of the starting stop.
        :param end_stop_id: Optional[str] - The identifier of the ending stop (if any).
        :param start_time_str: str - The start time in string format.
        :returns: dict[str, Any] - A dictionary of human-readable bags after processing.
        """
        start_time = strtime.str_time_to_seconds(
            start_time_str, accuracy_multiplier=self.accuracy_multiplier
        )

        b_i, b_best, marked_stops, tracers_map = self.init_vars(start_stop_id, start_time)

        k = 0
        for k in range(1, self.max_transfers + 1):
            rlog.debug(f"iteration {k}")
            b_i[k] = b_i[k - 1].copy()

            Q = self.collect_Q(marked_stops)

            b_i, marked_stops = self.process_routes(Q, k, b_i)
            b_i, additional_marked_stops = self.process_footpaths(marked_stops, k, b_i)

            marked_stops.update(additional_marked_stops)

            # copy current bags into b_best
            b_best = {stop_id: bag.copy() for stop_id, bag in b_i[k].items()}

            rlog.debug(f"marked_stops: {len(marked_stops)}")

            if len(marked_stops) == 0:
                break

        rlog.info(f"RAPTOR finished after {k} iterations")
        return bags_to_human_readable(b_best)

    def init_vars(
        self, start_stop_id: str, start_time: int
    ) -> tuple[dict[int, dict[str, Bag]], dict[str, Bag], set[str], TracerMap]:
        """
        Initializes variables for the algorithm, including bags for each stop and a tracer.

        :param start_stop_id: str - The identifier of the starting stop.
        :param start_time: int - The start time in seconds.
        :returns: tuple[dict[int, dict[str, Bag]], dict[str, Bag], set[str], TracerMap] - A tuple containing the initialized bags, best bags, marked stops, and tracer map.
        """
        tau_i: dict[int, dict[str, Bag]] = {
            0: {},
        }
        tracer = TracerMap(self.dq.stop_id_set)
        tau_best: dict[str, Bag] = {}
        marked_stops = set()

        for stop_id in self.dq.stop_id_set:
            tau_i[0][stop_id] = Bag()
            tau_best[stop_id] = Bag()

        start_bag = Bag()
        start_bag.add_if_necessary(self.label_class(start_time, start_stop_id))
        tau_i[0][start_stop_id] = start_bag
        tau_best[start_stop_id] = start_bag.copy()
        tracer.add(
            tracer=TraceStart(start_stop_id, start_time),
        )

        marked_stops.add(start_stop_id)

        return tau_i, tau_best, marked_stops, tracer

    def collect_Q(
        self: Self,
        marked_stops: set[str],
    ) -> dict[str, tuple[str, int]]:
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
        Q: dict[str, tuple[str, int]],
        k: int,
        b_i: dict[int, dict[str, Bag]],
    ) -> tuple[dict[int, dict[str, Bag]], set[str]]:
        """
        Processes the routes based on the collected data and updates the bags and marked stops.

        :param Q: dict[str, tuple[str, int]] - A dictionary of routes with their closest stops and indices.
        :param k: int - The current iteration count.
        :param b_i: dict[int, dict[str, Bag]] - A dictionary mapping iteration counts to bags.
        :returns: tuple[dict[int, dict[str, Bag]], set[str]] - A tuple containing updated bags and marked stops.
        """
        marked_stops = set()
        for route_id, (stop_id, idx) in Q.items():
            route_bag = RouteBag[L, S, T](
                self.dq,
            )

            for stop_id in self.dq.iterate_stops_in_route_from_idx(route_id, idx):
                b_i, marked_stops, route_bag = self.process_route(
                    route_id,
                    stop_id,
                    k,
                    b_i,
                    route_bag,
                    marked_stops,
                )
        return b_i, marked_stops

    def process_route(
        self,
        route_id: str,
        stop_id: str,
        k: int,
        b_i: dict[int, dict[str, Bag]],
        route_bag: RouteBag,
        marked_stops: set[str],
    ) -> tuple[dict[int, dict[str, Bag]], set[str], RouteBag]:
        """
        Processes a specific route by updating arrival times and merging bags.

        :param route_id: str - The identifier of the route.
        :param stop_id: str - The identifier of the stop.
        :param k: int - The current iteration count.
        :param b_i: dict[int, dict[str, Bag]] - A dictionary mapping iteration counts to bags.
        :param route_bag: RouteBag - The route bag to be updated.
        :param marked_stops: set[str] - A set of marked stops to be updated.
        :returns: tuple[dict[int, dict[str, Bag]], set[str], RouteBag] - A tuple containing updated bags, marked stops, and the route bag.
        """
        # first step - update arrival times in route bag
        route_bag.update_along_trip(stop_id)

        # second step - merge route_bag into stop_bag
        stop_bag = b_i[k][stop_id]
        is_any_added = stop_bag.merge(route_bag.to_bag())
        if is_any_added:
            marked_stops.add(stop_id)

        # third step - merge stop_bag into route_bag
        self.merge_bag_into_route_bag(
            route_bag,
            stop_bag,
            route_id,
            stop_id,
        )
        return b_i, marked_stops, route_bag

    def merge_bag_into_route_bag(
        self,
        route_bag: RouteBag,
        bag: Bag,
        route_id: str,
        stop_id: str,
    ) -> None:
        """
        Merges a bag into the route bag by finding the earliest trip for each label.

        :param route_bag: RouteBag - The route bag to which the labels will be merged.
        :param bag: Bag - The bag containing labels to be merged.
        :param route_id: str - The identifier of the route.
        :param stop_id: str - The identifier of the stop.
        """
        for label in bag:
            res = self.dq.earliest_trip(
                route_id,
                stop_id,
                label.arrival_time + self.default_transfer_time,
            )
            if res is None:
                continue
            trip, departure_time = res
            label = label.copy()
            label.update_before_route_bag_merge(departure_time, stop_id)
            route_bag.add_if_necessary(label, trip)

    def process_footpaths(
        self: Self,
        marked_stops: set[str],
        k: int,
        b_i: dict[int, dict[str, Bag]],
    ) -> tuple[dict[int, dict[str, Bag]], set[str]]:
        """
        Processes footpaths from marked stops and updates the bags accordingly.

        :param marked_stops: set[str] - A set of stop IDs that have been marked for processing.
        :param k: int - The current iteration count.
        :param b_i: dict[int, dict[str, Bag]] - A dictionary mapping iteration counts to bags.
        :returns: tuple[dict[int, dict[str, Bag]], set[str]] - A tuple containing updated bags and additional marked stops.
        """
        additional_marked_stops = set()
        for stop_id in marked_stops:
            for nearby_stop_id, walking_time in self.dq.get_footpaths()[stop_id].items():
                start_bag = b_i[k][stop_id]
                footpath_bag = start_bag.create_footpath_bag(
                    walking_time,
                    nearby_stop_id,
                )

                end_bag = b_i[k][nearby_stop_id]

                is_any_added = end_bag.merge(footpath_bag)

                if is_any_added:
                    additional_marked_stops.add(nearby_stop_id)
        return b_i, additional_marked_stops


def bags_to_human_readable(bags: dict[str, Bag]) -> dict[str, Any]:
    """
    Converts a dictionary of bags to a human-readable format.

    :param bags: dict[str, Bag] - A dictionary mapping stop IDs to their corresponding bags.
    :returns: dict[str, Any] - A dictionary mapping stop IDs to their human-readable bag representations.
    """
    return {stop_id: bag.to_human_readable() for stop_id, bag in bags.items()}
