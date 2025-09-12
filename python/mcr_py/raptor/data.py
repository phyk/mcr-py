import sys
from typing import Generic, Optional

from typing_extensions import Self

from mcr_py.structs import build
from mcr_py.utils.key import S, T


class DataQuerier:
    def __init__(self: Self, structs_dict: dict, footpaths: dict | None) -> None:
        """
        Initializes a DataQuerier with structured data and optional footpaths.

        :param structs_dict: dict - A dictionary containing structured data for trips, routes, and stops.
        :param footpaths: dict | None - An optional dictionary mapping stop IDs to footpath information.
        """
        self.footpaths = footpaths

        (
            # stop_times_by_trip,
            self.trip_ids_by_route,
            self.stops_by_route,
            self.idx_by_stop_by_route,
            self.routes_by_stop,
            self.times_by_stop_by_trip,
            self.stop_id_set,
            # route_id_set,
            # trip_id_set,
        ) = build.unpack_structs(structs_dict)

    def get_footpaths(self) -> dict:
        if self.footpaths is None:
            msg = "Footpaths are not set"
            raise Exception(msg)
        return self.footpaths

    def get_stop_ids(self) -> set[str]:
        """
        Retrieves a set of all stop IDs.

        :returns: set[str] - A set of stop IDs.
        """
        return self.stop_id_set

    def get_routes_by_stop(self, stop_id: str) -> set[str]:
        """
        Retrieves a set of routes that serve a specific stop.

        :param stop_id: str - The identifier of the stop.
        :returns: set[str] - A set of route IDs serving the specified stop.
        """
        return self.routes_by_stop[stop_id]

    def get_routes_serving_stop(self, stop_id: str) -> set[str]:
        """
        Retrieves a set of routes that serve a specific stop (alias for get_routes_by_stop).

        :param stop_id: str - The identifier of the stop.
        :returns: set[str] - A set of route IDs serving the specified stop.
        """
        return self.routes_by_stop[stop_id]

    def get_idx_of_stop_in_route(self, stop_id: str, route_id: str) -> int:
        """
        Retrieves the index of a stop within a specific route.

        :param stop_id: str - The identifier of the stop.
        :param route_id: str - The identifier of the route.
        :returns: int - The index of the stop in the specified route.
        """
        return self.idx_by_stop_by_route[route_id][stop_id]

    def get_arrival_time(self, trip_id: str, stop_id: str) -> int:
        """
        Retrieves the arrival time for a specific trip at a given stop.

        :param trip_id: str - The identifier of the trip.
        :param stop_id: str - The identifier of the stop.
        :returns: int - The arrival time at the specified stop for the given trip.
        """
        assert trip_id is not None
        assert stop_id is not None

        arrival_time = self.times_by_stop_by_trip[trip_id][stop_id][0]
        assert isinstance(arrival_time, int)
        return arrival_time

    def get_departure_time(self, trip_id: Optional[str], stop_id: str) -> int:
        """
        Retrieves the departure time for a specific trip at a given stop.

        :param trip_id: Optional[str] - The identifier of the trip (can be None).
        :param stop_id: str - The identifier of the stop.
        :returns: int - The departure time at the specified stop for the given trip, or sys.maxsize if trip_id is None.
        """
        assert stop_id is not None

        if trip_id is None:
            return sys.maxsize

        departure_time = self.times_by_stop_by_trip[trip_id][stop_id][1]
        assert isinstance(departure_time, int)
        return departure_time

    def earliest_trip(
        self, route_id: str, stop_id: str, arrival_time: int
    ) -> Optional[tuple[str, int]]:
        """
        Finds the earliest trip for a given route and stop that departs after a specified arrival time.

        :param route_id: str - The identifier of the route.
        :param stop_id: str - The identifier of the stop.
        :param arrival_time: int - The arrival time to compare against.
        :returns: Optional[tuple[str, int]] - A tuple containing the trip ID and departure time, or None if no trip is found.
        """
        trip_ids = self.trip_ids_by_route[route_id]  # sorted by departure time
        for trip_id in trip_ids:
            departure_time = self.get_departure_time(trip_id, stop_id)
            if departure_time >= arrival_time:
                return trip_id, departure_time

    def iterate_footpaths_from_stop(self, stop_id: str):
        """
        Iterates over footpaths originating from a specific stop.

        :param stop_id: str - The identifier of the stop.
        :raises Exception: If footpaths are not defined.
        :returns: dict_items - An iterable of footpath items from the specified stop.
        """
        if self.footpaths is None:
            msg = "footpaths has to be defined when calling iterate_footpaths_from_stop"
            raise Exception(msg)
        return self.footpaths[stop_id].items()

    def iterate_stops_in_route_from_idx(self: Self, route_id: str, idx: int) -> list[str]:
        """
        Iterates over stops in a specific route starting from a given index.

        :param route_id: str - The identifier of the route.
        :param idx: int - The starting index for iteration.
        :returns: list[str] - A list of stop IDs from the specified route starting at the given index.
        """
        return self.stops_by_route[route_id][idx:]


class ExpandedDataQuerier(Generic[S, T], DataQuerier):
    def __init__(
        self: Self,
        structs_dict: dict,
        footpaths: dict | None,
        additional_stop_information: dict[str, S],
        additional_trip_information: dict[str, T],
    ) -> None:
        """
        Initializes an ExpandedDataQuerier with structured data, optional footpaths,
        and additional stop and trip information.

        :param structs_dict: dict - A dictionary containing structured data for trips, routes, and stops.
        :param footpaths: dict | None - An optional dictionary mapping stop IDs to footpath information.
        :param additional_stop_information: dict[str, S] - A dictionary mapping stop IDs to additional information.
        :param additional_trip_information: dict[str, T] - A dictionary mapping trip IDs to additional information.
        """
        self.additional_stop_information = additional_stop_information
        self.additional_trip_information = additional_trip_information
        super().__init__(structs_dict, footpaths)

    def get_stop(self, stop_id: str) -> S:
        """
        Retrieves additional information for a specific stop.

        :param stop_id: str - The identifier of the stop.
        :returns: S - The additional information associated with the specified stop.
        """
        return self.additional_stop_information[stop_id]

    def get_trip(self, trip_id: str) -> T:
        """
        Retrieves additional information for a specific trip.

        :param trip_id: str - The identifier of the trip.
        :returns: T - The additional information associated with the specified trip.
        """
        return self.additional_trip_information[trip_id]
