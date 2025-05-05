from copy import deepcopy
from typing import Generic, Optional, TypeVar

from typing_extensions import Self

from mcr_py.utils.key import S, T
from mcr_py.raptor.data import DataQuerier, ExpandedDataQuerier
from mcr_py.tracer.tracer import TraceFootpath, TraceStart, TraceTrip


class BaseLabel:
    def __init__(self, time: int, stop_id: Optional[str] = None):
        """
        Initializes a BaseLabel with an arrival time and an optional stop ID.

        :param time: int - The arrival time associated with the label.
        :param stop_id: Optional[str] - An optional identifier for the stop.
        """
        self.arrival_time = time

    def __repr__(self):
        """
        Returns a string representation of the BaseLabel.

        :returns: str - A string representation of the label.
        """
        return f"Label({self.arrival_time})"

    def strictly_dominates(self, other: Self) -> bool:
        """
        Determines if this label strictly dominates another label.

        :param other: Self - Another BaseLabel to compare against.
        :returns: bool - True if this label strictly dominates the other, False otherwise.
        """
        return True

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        """
        Updates the label's arrival time along a trip.

        :param arrival_time: int - The new arrival time.
        :param stop_id: str - The identifier of the stop.
        :param trip_id: str - The identifier of the trip.
        """
        self.arrival_time = arrival_time

    def update_along_footpath(self, walking_time: int, stop_id: str):
        """
        Updates the label's arrival time based on walking time along a footpath.

        :param walking_time: int - The time spent walking.
        :param stop_id: str - The identifier of the stop.
        """
        self.arrival_time = self.arrival_time + walking_time

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        """
        Updates the label before merging with a route bag.

        :param departure_time: int - The departure time to set.
        :param stop_id: str - The identifier of the stop.
        """
        self.arrival_time = departure_time

    def update_before_stop_bag_merge(self, stop_id: str):
        """
        Placeholder method for updates before merging with a stop bag.

        :param stop_id: str - The identifier of the stop.
        """
        pass

    def to_human_readable(self) -> dict:
        """
        Converts the label to a human-readable format.

        :returns: dict - A dictionary representation of the label.
        """
        return {}

    def copy(self: Self) -> Self:
        """
        Creates a deep copy of the label.

        :returns: Self - A new instance of the label with the same attributes.
        """
        return deepcopy(self)

    def __eq__(self, value):
        """
        Checks for equality between this label and another value.

        :param value: Any - The value to compare with.
        :returns: bool - True if the values are equal, False otherwise.
        """
        if isinstance(value, BaseLabel) and self.arrival_time == value.arrival_time:
            return True
        return False

    def __hash__(self):
        """
        Returns a hash of the label based on its arrival time.

        :returns: int - The hash value of the label.
        """
        return self.arrival_time.__hash__()


L = TypeVar("L", bound=BaseLabel)  # custom label


class Bag:
    def __init__(self):
        """
        Initializes an empty Bag to store BaseLabel objects.

        """
        self._bag: set[BaseLabel] = set()

    def __iter__(self):
        return iter(self._bag)

    def __str__(self):
        return str(self._bag)

    def __repr__(self):
        return repr(self._bag)

    @staticmethod
    def from_labels(labels: list[BaseLabel]):
        """
        Creates a Bag from a list of BaseLabel objects.

        :param labels: list[BaseLabel] - A list of labels to be added to the Bag.
        :returns: Bag - A new Bag instance containing the provided labels.
        """
        bag = Bag()
        bag._bag = set(labels)
        return bag

    def add_if_necessary(self, label: BaseLabel) -> bool:
        """
        Adds a label to the Bag if it is necessary based on dominance.

        :param label: BaseLabel - The label to add.
        :returns: bool - True if the label was added, False otherwise.
        """
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label)
            return True
        return False

    def add(self, label: BaseLabel):
        """
        Adds a copy of a label to the Bag.

        :param label: BaseLabel - The label to add.
        """
        self._bag.add(label.copy())

    def content_dominates(self, label: BaseLabel):
        """
        Checks if any label in the Bag strictly dominates the given label.

        :param label: BaseLabel - The label to check against.
        :returns: bool - True if any label dominates the given label, False otherwise.
        """
        return any(other.strictly_dominates(label) for other in self._bag)

    def remove_dominated_by(self, label: BaseLabel):
        """
        Removes any labels from the Bag that are strictly dominated by the given label.

        :param label: BaseLabel - The label to compare against.
        """
        self._bag = {
            other for other in self._bag if not label.strictly_dominates(other)
        }

    # merge other into self
    def merge(self: Self, other: Self) -> bool:
        """
        Merges another Bag into this Bag, adding necessary labels.

        :param other: Self - The Bag to merge into this one.
        :returns: bool - True if any labels were added during the merge, False otherwise.
        """
        is_any_added = False
        for label in other._bag:
            print(label)
            is_added = self.add_if_necessary(label)
            print(is_added)
            is_any_added = is_any_added or is_added
        return is_any_added

    def create_bag_with_timeoffset(self: Self, time: int):
        """
        Creates a new Bag with all labels' arrival times offset by a given time.

        :param time: int - The time offset to apply to each label's arrival time.
        :returns: Bag - A new Bag instance with updated arrival times.
        """
        bag = self.copy()
        bag.add_arrival_time_to_all(time)
        return bag

    def create_footpath_bag(
        self: Self,
        walk_time: int,
        stop_id: str,
    ):
        """
        Creates a new Bag with updated labels based on a footpath walk time.

        :param walk_time: int - The time spent walking.
        :param stop_id: str - The identifier of the stop.
        :returns: Bag - A new Bag instance with updated arrival times.
        """
        bag = self.copy()
        for label in bag._bag:
            label.update_along_footpath(walk_time, stop_id)
        return bag

    def add_arrival_time_to_all(self, time: int):
        """
        Adds a specified time to the arrival time of all labels in the Bag.

        :param time: int - The time to add to each label's arrival time.
        """
        for label in self._bag:
            label.arrival_time += time

    def update_before_stop_bag_merge(self, stop_id: str) -> Self:
        """
        Updates all labels in the Bag before merging with a stop bag.

        :param stop_id: str - The identifier of the stop.
        :returns: Self - The current Bag instance after updates.
        """
        for label in self._bag:
            label.update_before_stop_bag_merge(stop_id)
        return self

    def to_human_readable(self):
        """
        Converts all labels in the Bag to a human-readable format.

        :returns: list - A list of dictionaries representing each label.
        """
        return list((label.to_human_readable()) for label in self._bag)

    def copy(self):
        """
        Creates a deep copy of the Bag.

        :returns: Bag - A new instance of the Bag with copied labels.
        """
        new_bag = Bag()
        new_bag._bag = set(label.copy() for label in self._bag)
        return new_bag


ArrivalTimePerTrip = dict[str, int]


class RouteBag(Generic[L, S, T]):
    def __init__(
        self,
        dq: ExpandedDataQuerier[S, T] | DataQuerier,
    ):
        """
        Initializes a RouteBag with a data querier.

        :param dq: ExpandedDataQuerier[S, T] | DataQuerier - The data querier used to retrieve arrival times.
        """
        self._bag: set[tuple[L, str]] = set()
        self._dq = dq

    def __str__(self):
        """
        Returns a string representation of the RouteBag.

        :returns: str - A string representation of the RouteBag.
        """
        return str(self._bag)

    def __repr__(self):
        """
        Returns a detailed string representation of the RouteBag.

        :returns: str - A detailed string representation of the RouteBag.
        """
        return repr(self._bag)

    def add_if_necessary(self, label: L, trip: str) -> Self:
        """
        Adds a label and trip to the RouteBag if necessary based on dominance.

        :param label: L - The label to add.
        :param trip: str - The trip identifier associated with the label.
        :returns: Self - The current RouteBag instance for method chaining.
        """
        if not self.content_dominates(label):
            self.remove_dominated_by(label)
            self.add(label, trip)
        return self

    def add(self, label: L, trip: str) -> Self:
        """
        Adds a label and its associated trip to the RouteBag.

        :param label: L - The label to add.
        :param trip: str - The trip identifier associated with the label.
        :returns: Self - The current RouteBag instance for method chaining.
        """
        self._bag.add((label.copy(), trip))
        return self

    def content_dominates(self, label: L) -> bool:
        """
        Checks if any label in the RouteBag strictly dominates the given label.

        :param label: L - The label to check against.
        :returns: bool - True if any label dominates the given label, False otherwise.
        """
        return any(other.strictly_dominates(label) for other, _ in self._bag)

    def remove_dominated_by(self, label: L) -> Self:
        """
        Removes any labels from the RouteBag that are strictly dominated by the given label.

        :param label: L - The label to compare against.
        :returns: Self - The current RouteBag instance after removal.
        """
        self._bag = {
            (other_label, other_trip)
            for other_label, other_trip in self._bag
            if not label.strictly_dominates(other_label)
        }
        return self

    def update_along_trip(self, stop_id: str) -> Self:
        """
        Updates the arrival times of all labels in the RouteBag along a trip.

        :param stop_id: str - The identifier of the stop for which to update arrival times.
        :returns: Self - The current RouteBag instance after updates.
        """
        for label, trip in self._bag:
            arrival_time = self._dq.get_arrival_time(trip, stop_id)
            label.update_along_trip(arrival_time, stop_id, trip)
        return self

    def get_trips(self) -> set[str]:
        """
        Retrieves a set of all trip identifiers in the RouteBag.

        :returns: set[str] - A set of trip identifiers.
        """
        return set(trip for _, trip in self._bag)

    def to_bag(self) -> Bag:
        """
        Converts the RouteBag into a Bag containing all labels.

        :returns: Bag - A new Bag instance containing the labels from the RouteBag.
        """
        bag = Bag()
        for label, _ in self._bag:
            bag.add(label)
        return bag


class TraceLabel(BaseLabel):
    def __init__(self, time: int, stop: Optional[str] = None):
        """
        Initializes a TraceLabel with an arrival time and optional stop ID,
        and initializes lists to track stops, trips, and traces.

        :param time: int - The arrival time associated with the label.
        :param stop: Optional[str] - An optional identifier for the stop.
        """
        super().__init__(time, stop)
        self.stops = []
        self.trips = []
        self.traces = []
        if stop is not None and time is not None:
            self.stops.append(stop)
            self.traces.append(TraceStart(stop, time))

        self.last_update = "start"

    def update_along_trip(self, arrival_time: int, stop_id: str, trip_id: str):
        """
        Updates the TraceLabel's arrival time along a trip and records the trace information.

        :param arrival_time: int - The new arrival time.
        :param stop_id: str - The identifier of the stop.
        :param trip_id: str - The identifier of the trip.
        """
        old_arrival_time = self.arrival_time
        super().update_along_trip(arrival_time, stop_id, trip_id)
        if self.last_update == "trip":
            prev_trace = self.traces.pop()
            assert trip_id == prev_trace.trip_id
            self.traces.append(
                TraceTrip(
                    prev_trace.start_stop_id,
                    prev_trace.departure_time,
                    stop_id,
                    arrival_time,
                    trip_id,
                )
            )
        else:
            self.traces.append(
                TraceTrip(
                    self.stops[-1], old_arrival_time, stop_id, arrival_time, trip_id
                )
            )

        self.last_update = "trip"
        self.stops.append(stop_id)
        self.trips.append(trip_id)

    def update_along_footpath(self, walking_time: int, stop_id: str):
        """
        Updates the TraceLabel's arrival time based on walking time along a footpath
        and records the trace information.

        :param walking_time: int - The time spent walking.
        :param stop_id: str - The identifier of the stop.
        """
        super().update_along_footpath(walking_time, stop_id)
        self.last_update = "footpath"
        self.traces.append(TraceFootpath(self.stops[-1], stop_id, walking_time))
        self.stops.append(stop_id)

    def update_before_route_bag_merge(self, departure_time: int, stop_id: str):
        """
        Updates the TraceLabel before merging with a route bag.

        :param departure_time: int - The departure time to set.
        :param stop_id: str - The identifier of the stop.
        """
        super().update_before_route_bag_merge(departure_time, stop_id)
        self.last_update = "waiting"
        self.stops.append(stop_id)

    def __eq__(self, value):
        """
        Checks for equality between this TraceLabel and another value.

        :param value: Any - The value to compare with.
        :returns: bool - True if the values are equal, False otherwise.
        """
        if (
            isinstance(value, TraceLabel)
            and self.arrival_time == value.arrival_time
            and self.last_update == value.last_update
        ):
            return True
        return False

    def __hash__(self):
        """
        Returns a hash of the TraceLabel based on its arrival time and last update status.

        :returns: int - The hash value of the TraceLabel.
        """
        return super().__hash__() + self.last_update.__hash__()
