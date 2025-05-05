from mcr_py.raptor.bag import (
    BaseLabel,
    Bag,
    RouteBag,
    TraceLabel,
)  # Adjust the import based on your module structure
from mcr_py.raptor.example_labels import ArrivalTimeLabel


# Test for BaseLabel
def test_base_label_initialization():
    label = BaseLabel(10, "stop_1")
    assert label.arrival_time == 10
    assert label.__repr__() == "Label(10)"


def test_base_label_update_along_trip():
    label = BaseLabel(10)
    label.update_along_trip(15, "stop_1", "trip_1")
    assert label.arrival_time == 15


def test_base_label_update_along_footpath():
    label = BaseLabel(10)
    label.update_along_footpath(5, "stop_2")
    assert label.arrival_time == 15


# Test for Bag
def test_bag_initialization():
    bag = Bag()
    assert len(bag._bag) == 0


def test_bag_add_and_iter():
    bag = Bag()
    label = BaseLabel(10)
    bag.add(label)
    assert len(bag._bag) == 1
    assert list(bag)[0] == label


def test_bag_merge():
    bag1 = Bag()
    bag2 = Bag()
    label2 = BaseLabel(15)
    label1 = BaseLabel(10)
    bag1.add(label2)
    bag2.add(label1)
    assert not bag1.merge(bag2)
    assert len(bag1._bag) == 1


# Test for RouteBag
def test_route_bag_initialization():
    dq = None
    route_bag = RouteBag(dq)  # type: ignore
    assert len(route_bag._bag) == 0


def test_route_bag_add_if_necessary():
    dq = None
    route_bag = RouteBag(dq)  # type: ignore
    label = TraceLabel(10, "stop_1")
    route_bag.add_if_necessary(label, "trip_1")
    assert len(route_bag._bag) == 1


# Test for TraceLabel
def test_trace_label_initialization():
    trace_label = TraceLabel(10, "stop_1")
    assert trace_label.arrival_time == 10
    assert trace_label.stops == ["stop_1"]
    assert len(trace_label.traces) == 1


def test_trace_label_update_along_trip():
    trace_label = TraceLabel(10, "stop_1")
    trace_label.update_along_trip(15, "stop_2", "trip_1")
    assert trace_label.arrival_time == 15
    assert trace_label.stops == ["stop_1", "stop_2"]
    assert len(trace_label.traces) == 2


def test_trace_label_update_along_footpath():
    trace_label = TraceLabel(10, "stop_1")
    trace_label.update_along_footpath(5, "stop_2")
    assert trace_label.arrival_time == 15
    assert trace_label.stops == ["stop_1", "stop_2"]
    assert len(trace_label.traces) == 2
