import logging
from copy import deepcopy
from typing import Generic, Tuple

from typing_extensions import Any, Self

from mcr_py.raptor.bag import Bag, L, RouteBag
from mcr_py.raptor.data import DataQuerier
from mcr_py.utils.key import S, T
from mcr_py.utils.logger import rlog


class McRaptorSingle(Generic[L, S, T]):
    def __init__(
        self,
        structs_dict: dict,
        default_transfer_time: int,
        label_class: type[L],
        limits: dict[int, int],
    ) -> None:
        """
        Initializes the McRaptorSingle algorithm with structured data, default transfer time, and label class.

        :param structs_dict: dict - A dictionary containing structured data for the algorithm.
        :param default_transfer_time: int - The default time to transfer between stops.
        :param label_class: type[L] - The class type for labels used in the algorithm.
        """
        self.dq = DataQuerier(
            structs_dict,
            footpaths=None,
        )

        self.default_transfer_time = default_transfer_time

        self.label_class = label_class
        self.limit_cache = limits
        logging.debug("Initialized with limits %s", self.limit_cache)

    def run(
        self,
        bags: dict[str, Bag],
    ) -> dict[str, Bag]:
        """
        Executes the McRaptorSingle algorithm on the provided bags.

        :param bags: dict[str, Bag] - A dictionary mapping stop IDs to their corresponding bags.
        :returns: dict[str, Bag] - A dictionary of output bags after processing.
        """
        output_bags, marked_stops = self.init_vars(bags)

        Q = self.collect_Q(marked_stops)

        output_bags, marked_stops = self.process_routes(Q, bags, output_bags)

        if len(marked_stops) == 0:
            rlog.info("No updates")
        return output_bags

    def init_vars(
        self,
        bags: dict[str, Bag],
    ) -> Tuple[dict[str, Bag], set[str]]:
        """
        Initializes variables for the algorithm, including creating missing bags for stops.

        :param bags: dict[str, Bag] - A dictionary mapping stop IDs to their corresponding bags.
        :returns: Tuple[dict[str, Bag], set[str]] - A tuple containing the output bags and a set of marked stops.
        """
        marked_stops = set(bags.keys())

        n_missing_stops = 0
        for stop_id in self.dq.stop_id_set:
            if stop_id not in bags:
                bags[stop_id] = Bag()
                n_missing_stops += 1
        if n_missing_stops > 0:
            rlog.debug(
                f"Added {n_missing_stops} missing stops to bags ({n_missing_stops / len(self.dq.stop_id_set) * 100:.2f}%)"
            )

        output_bags = deepcopy(bags)

        return output_bags, marked_stops

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
        bags: dict[str, Bag],
        output_bags: dict[str, Bag],
    ) -> tuple[dict[str, Bag], set[str]]:
        """
        Processes the routes based on the collected data and updates the output bags and marked stops.

        :param Q: dict[str, tuple[str, int]] - A dictionary of routes with their closest stops and indices.
        :param bags: dict[str, Bag] - A dictionary mapping stop IDs to their corresponding bags.
        :param output_bags: dict[str, Bag] - A dictionary of output bags to be updated.
        :returns: tuple[dict[str, Bag], set[str]] - A tuple containing updated output bags and marked stops.
        """
        marked_stops = set()
        for route_id, (stop_id, idx) in Q.items():
            route_bag = RouteBag[L, S, T](self.dq)

            for stop_id in self.dq.iterate_stops_in_route_from_idx(route_id, idx):
                output_bags, marked_stops, route_bag = self.process_route(
                    route_id,
                    stop_id,
                    bags,
                    output_bags,
                    route_bag,
                    marked_stops,
                )

        return output_bags, marked_stops

    def process_route(
        self,
        route_id: str,
        stop_id: str,
        bags: dict[str, Bag],
        output_bags: dict[str, Bag],
        route_bag: RouteBag,
        marked_stops: set[str],
    ) -> tuple[dict[str, Bag], set[str], RouteBag]:
        """
        Processes a specific route by updating arrival times and merging bags.

        :param route_id: str - The identifier of the route.
        :param stop_id: str - The identifier of the stop.
        :param bags: dict[str, Bag] - A dictionary mapping stop IDs to their corresponding bags.
        :param output_bags: dict[str, Bag] - A dictionary of output bags to be updated.
        :param route_bag: RouteBag - The route bag to be updated.
        :param marked_stops: set[str] - A set of marked stops to be updated.
        :returns: tuple[dict[str, Bag], set[str], RouteBag] - A tuple containing updated output bags, marked stops, and the route bag.
        """
        # first step - update arrival times in route bag
        # Uses limit_cache in route_bag
        route_bag.update_along_trip(stop_id)

        # second step - merge route_bag into stop_bag
        output_stop_bag = output_bags[stop_id]
        is_any_added = output_stop_bag.merge(
            route_bag.to_bag().update_before_stop_bag_merge(stop_id),
        )
        if is_any_added:
            marked_stops.add(stop_id)

        stop_bag = bags[stop_id]
        # third step - merge stop_bag into route_bag
        self.merge_bag_into_route_bag(
            route_bag,
            stop_bag,
            route_id,
            stop_id,
        )
        return output_bags, marked_stops, route_bag

    # TODO: should this be moved into RouteBag?
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


def bags_to_human_readable(bags: dict[str, Bag]) -> dict[str, Any]:
    """
    Converts a dictionary of bags to a human-readable format.

    :param bags: dict[str, Bag] - A dictionary mapping stop IDs to their corresponding bags.
    :returns: dict[str, Any] - A dictionary mapping stop IDs to their human-readable bag representations.
    """
    return {stop_id: bag.to_human_readable() for stop_id, bag in bags.items()}
