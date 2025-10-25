from typing import Optional

from mcr_py.mcr.data import ACCURACY_MULTIPLIER
from mcr_py.utils import strtime


class Trace:
    """
    Base class for all trace types.
    """

    pass


class MovingTrace(Trace):
    """
    Represents a moving trace between two stops.

    :param start_stop_id: str - The ID of the starting stop.
    :param end_stop_id: str - The ID of the ending stop.
    """

    def __init__(self, start_stop_id: str, end_stop_id: str) -> None:
        self.start_stop_id = start_stop_id
        self.end_stop_id = end_stop_id


class TracerMap:
    """
    Manages a collection of traces associated with specific stops.

    :param stop_ids: set[str] - A set of stop IDs to initialize the TracerMap.
    """

    def __init__(self, stop_ids: set[str]) -> None:
        self.tracers: dict[str, list[Trace]] = {stop_id: [] for stop_id in stop_ids}
        self.last_hop_on_stop_id: Optional[str] = None
        self.last_hop_on_time: Optional[int] = None

    def __str__(self) -> str:
        """
        Returns a string representation of the TracerMap.

        :returns: str - A formatted string showing the traces associated with each stop.
        """
        sorted_keys = list(self.tracers.keys())
        sorted_keys.sort()
        return "\n".join(
            [
                f"{stop_id}: {', '.join([str(trace) for trace in self.tracers[stop_id]])}"
                for stop_id in sorted_keys
            ]
        )

    def __getitem__(self, stop_id: str):
        """
        Retrieves the list of traces associated with a specific stop ID.

        :param stop_id: str - The ID of the stop.
        :returns: list[Trace] - The list of traces associated with the stop ID.
        """
        return self.tracers[stop_id]

    def add(self, tracer: Trace) -> None:
        """
        Adds a trace to the TracerMap.

        :param tracer: Trace - The trace to add. Can be a TraceStart or MovingTrace.
        :raises ValueError: If the first tracer for a stop is not a TraceStart or if the tracer type is unknown.
        """
        if isinstance(tracer, MovingTrace):
            end_stop_id = tracer.end_stop_id
            start_stop_id = tracer.start_stop_id

            previous_tracers = self.tracers[start_stop_id]

            if len(previous_tracers) == 0 or not isinstance(previous_tracers[0], TraceStart):
                msg = f"The first tracer for stop {start_stop_id} must be a TraceStart"
                raise ValueError(msg)

            new_tracers = previous_tracers + [tracer]

            self.tracers[end_stop_id] = new_tracers
        elif isinstance(tracer, TraceStart):
            self.tracers[tracer.start_stop_id] = [tracer]
        else:
            msg = f"Unknown tracer type: {type(tracer)}"
            raise ValueError(msg)

    def update_last_hop(self, stop_id: str, time: int) -> None:
        """
        Updates the last hop information for the TracerMap.

        :param stop_id: str - The ID of the stop where the last hop occurred.
        :param time: int - The time of the last hop.
        """
        self.last_hop_on_stop_id = stop_id
        self.last_hop_on_time = time

    def clear_last_hop(self) -> None:
        """
        Clears the last hop information from the TracerMap.
        """
        self.last_hop_on_stop_id = None
        self.last_hop_on_time = None

    def get_last_hop(self):
        """
        Retrieves the last hop information.

        :returns: tuple[Optional[str], Optional[int]] - The stop ID and time of the last hop.
        """
        return self.last_hop_on_stop_id, self.last_hop_on_time


class TraceStart(Trace):
    """
    Represents the starting point of a trace.

    :param start_stop_id: str - The ID of the starting stop.
    :param start_time: int - The time at which the trace starts.
    """

    def __init__(self, start_stop_id: str, start_time: int) -> None:
        self.start_stop_id = start_stop_id
        self.start_time = start_time

    def __str__(self) -> str:
        """
        Returns a string representation of the TraceStart.

        :returns: str - A formatted string indicating the start stop and time.
        """
        return f"Start at {self.start_stop_id} at {strtime.seconds_to_str_time(self.start_time, ACCURACY_MULTIPLIER)}"


class EnrichedTraceStart(TraceStart):
    """
    Represents an enriched starting point of a trace with additional stop name information.

    :param trace_start: TraceStart - The original trace start object.
    :param start_stop_name: str - The name of the starting stop.
    """

    def __init__(self, trace_start: TraceStart, start_stop_name: str) -> None:
        super().__init__(trace_start.start_stop_id, trace_start.start_time)
        self.start_stop_name = start_stop_name

    def __str__(self) -> str:
        """
        Returns a string representation of the EnrichedTraceStart.

        :returns: str - A formatted string indicating the enriched start stop and time.
        """
        return f"Start at {self.start_stop_name} ({self.start_stop_id}) at {strtime.seconds_to_str_time(self.start_time, ACCURACY_MULTIPLIER)}"


class TraceTrip(MovingTrace):
    """
    Represents a trip between two stops.

    :param start_stop_id: str - The ID of the starting stop.
    :param departure_time: int - The time of departure from the starting stop.
    :param end_stop_id: str - The ID of the ending stop.
    :param arrival_time: int - The time of arrival at the ending stop.
    :param trip_id: str - The unique identifier for the trip.
    """

    def __init__(
        self,
        start_stop_id: str,
        departure_time: int,
        end_stop_id: str,
        arrival_time: int,
        trip_id: str,
    ) -> None:
        super().__init__(start_stop_id, end_stop_id)
        self.departure_time = departure_time
        self.arrival_time = arrival_time
        self.trip_id = trip_id

    def __str__(self) -> str:
        """
        Returns a string representation of the TraceTrip.

        :returns: str - A formatted string indicating the trip details.
        """
        return (
            f"Trip {self.trip_id} from {self.start_stop_id, ACCURACY_MULTIPLIER}@"
            + f"{strtime.seconds_to_str_time(self.departure_time, ACCURACY_MULTIPLIER)} to "
            + f"{self.end_stop_id}@{strtime.seconds_to_str_time(self.arrival_time, ACCURACY_MULTIPLIER)}"
        )


class EnrichedTraceTrip(TraceTrip):
    """
    Represents an enriched trip with additional stop name information.

    :param trace_trip: TraceTrip - The original trace trip object.
    :param trip_name: str - The name of the trip.
    :param start_stop_name: str - The name of the starting stop.
    :param end_stop_name: str - The name of the ending stop.
    """

    def __init__(
        self,
        trace_trip: TraceTrip,
        trip_name: str,
        start_stop_name: str,
        end_stop_name: str,
    ) -> None:
        super().__init__(
            trace_trip.start_stop_id,
            trace_trip.departure_time,
            trace_trip.end_stop_id,
            trace_trip.arrival_time,
            trace_trip.trip_id,
        )
        self.trip_name = trip_name
        self.start_stop_name = start_stop_name
        self.end_stop_name = end_stop_name

    def __str__(self) -> str:
        """
        Returns a string representation of the EnrichedTraceTrip.

        :returns: str - A formatted string indicating the enriched trip details.
        """
        return (
            f"Trip {self.trip_name} from {self.start_stop_name}@{strtime.seconds_to_str_time(self.departure_time, ACCURACY_MULTIPLIER)} to "
            + f"{self.end_stop_name}@{strtime.seconds_to_str_time(self.arrival_time, ACCURACY_MULTIPLIER)}"
        )


class TraceFootpath(MovingTrace):
    """
    Represents a footpath between two stops.

    :param start_stop_id: str - The ID of the starting stop.
    :param end_stop_id: str - The ID of the ending stop.
    :param walking_time: int - The time taken to walk from the start to the end stop.
    """

    def __init__(self, start_stop_id: str, end_stop_id: str, walking_time: int) -> None:
        super().__init__(start_stop_id, end_stop_id)
        self.walking_time = walking_time

    def __str__(self) -> str:
        """
        Returns a string representation of the TraceFootpath.

        :returns: str - A formatted string indicating the walking details.
        """
        return f"Walk from {self.start_stop_id} to {self.end_stop_id} in {strtime.seconds_to_str_time(self.walking_time, ACCURACY_MULTIPLIER)}"


class EnrichedTraceFootpath(TraceFootpath):
    """
    Represents an enriched footpath with additional stop name information.

    :param trace_footpath: TraceFootpath - The original trace footpath object.
    :param start_stop_name: str - The name of the starting stop.
    :param end_stop_name: str - The name of the ending stop.
    """

    def __init__(
        self,
        trace_footpath: TraceFootpath,
        start_stop_name: str,
        end_stop_name: str,
    ) -> None:
        super().__init__(
            trace_footpath.start_stop_id,
            trace_footpath.end_stop_id,
            trace_footpath.walking_time,
        )
        self.start_stop_name = start_stop_name
        self.end_stop_name = end_stop_name

    def __str__(self) -> str:
        return f"Walk from {self.start_stop_name} ({self.start_stop_id}) to {self.end_stop_name} ({self.end_stop_id}) in {strtime.seconds_to_str_time(self.walking_time, ACCURACY_MULTIPLIER)}"
