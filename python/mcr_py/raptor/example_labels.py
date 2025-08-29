from typing import Optional

from mcr_py.raptor import bag
from mcr_py.utils import strtime


class ArrivalTimeLabel(bag.TraceLabel):
    """
    Label class for McRAPTOR algorithm, that only considers arrival time and therefore
    behaves like the original RAPTOR algorithm.
    """

    def __init__(self, time: int, stop: Optional[str] = None):
        """
        Initializes an ArrivalTimeLabel with an arrival time and an optional stop ID.

        :param time: int - The arrival time associated with the label.
        :param stop: Optional[str] - An optional identifier for the stop.
        """
        super().__init__(time, stop)

    def strictly_dominates(self, other: bag.BaseLabel) -> bool:
        """
        Determines if this ArrivalTimeLabel strictly dominates another label based on arrival time.

        :param other: Self - Another ArrivalTimeLabel to compare against.
        :returns: bool - True if this label strictly dominates the other, False otherwise.
        """
        return self.arrival_time <= other.arrival_time

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        """
        Updates the ArrivalTimeLabel's arrival time along a trip.

        :param arrival_time: int - The new arrival time.
        :param stop_id: str - The identifier of the stop.
        :param trip_id: str - The identifier of the trip.
        """
        super().update_along_trip(arrival_time, stop_id, trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        """
        Updates the ArrivalTimeLabel's arrival time based on walking time along a footpath.

        :param walking_time: int - The time spent walking.
        :param stop_id: str - The identifier of the stop.
        """
        super().update_along_footpath(walking_time, stop_id)

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        """
        Updates the ArrivalTimeLabel before merging with a route bag.

        :param departure_time: int - The departure time to set.
        :param stop_id: str - The identifier of the stop.
        """
        super().update_before_route_bag_merge(departure_time, stop_id)

    def to_human_readable(self):
        """
        Converts the ArrivalTimeLabel to a human-readable format.

        :returns: dict - A dictionary representation of the label including arrival time, stops, trips, and traces.
        """
        return {
            "arrival_time": strtime.seconds_to_str_time(self.arrival_time),
            "stops": self.stops,
            "trips": self.trips,
            "traces": self.traces,
        }


class ActivityDurationLabel(bag.TraceLabel):
    """
    Label class for McRAPTOR algorithm that tracks arrival time, travel time, walking time, and waiting time.
    """

    def __init__(self, time: int, stop: Optional[str] = None):
        """
        Initializes an ActivityDurationLabel with an arrival time and an optional stop ID,
        and initializes time tracking attributes.

        :param time: int - The arrival time associated with the label.
        :param stop: Optional[str] - An optional identifier for the stop.
        """
        super().__init__(time, stop)
        self.travel_time = 0
        self.walking_time = 0
        self.waiting_time = 0

    def __repr__(self):
        """
        Returns a string representation of the ActivityDurationLabel.

        :returns: str - A string representation of the label with arrival time, travel time, walking time, and waiting time.
        """
        return f"Label({self.arrival_time}, t={self.travel_time}, w={self.walking_time}, wait={self.waiting_time})"

    def strictly_dominates(self, other: bag.BaseLabel) -> bool:
        """
        Determines if this ActivityDurationLabel strictly dominates another label based on arrival time and durations.

        :param other: Self - Another ActivityDurationLabel to compare against.
        :returns: bool - True if this label strictly dominates the other, False otherwise.
        """
        return (
            self.arrival_time <= other.arrival_time
            # and self.travel_time <= other.travel_time
            # and self.walking_time <= other.walking_time
            # and self.waiting_time <= other.waiting_time
        )

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        """
        Updates the ActivityDurationLabel's arrival time along a trip and tracks travel time.

        :param arrival_time: int - The new arrival time.
        :param stop_id: str - The identifier of the stop.
        :param trip_id: str - The identifier of the trip.
        """
        old_arrival_time = self.arrival_time
        super().update_along_trip(arrival_time, stop_id, trip_id)
        interval = arrival_time - old_arrival_time
        assert interval >= 0
        self.travel_time += interval

    def update_along_footpath(self, walking_time: int, stop_id: str):
        """
        Updates the ActivityDurationLabel's arrival time based on walking time along a footpath and tracks walking time.

        :param walking_time: int - The time spent walking.
        :param stop_id: str - The identifier of the stop.
        """
        super().update_along_footpath(walking_time, stop_id)
        self.walking_time += walking_time

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        """
        Updates the ActivityDurationLabel before merging with a route bag and tracks waiting time.

        :param departure_time: int - The departure time to set.
        :param stop_id: str - The identifier of the stop.
        """
        old_arrival_time = self.arrival_time
        super().update_before_route_bag_merge(departure_time, stop_id)
        interval = departure_time - old_arrival_time
        assert interval >= 0
        self.waiting_time += interval

    def to_human_readable(self):
        """
        Converts the ActivityDurationLabel to a human-readable format.

        :returns: dict - A dictionary representation of the label including arrival time, travel time, walking time, waiting time, stops, trips, and traces.
        """
        return {
            "arrival_time": strtime.seconds_to_str_time(self.arrival_time),
            "travel_time": strtime.seconds_to_str_time(self.travel_time),
            "walking_time": strtime.seconds_to_str_time(self.walking_time),
            "waiting_time": strtime.seconds_to_str_time(self.waiting_time),
            "stops": self.stops,
            "trips": self.trips,
            "traces": self.traces,
        }
