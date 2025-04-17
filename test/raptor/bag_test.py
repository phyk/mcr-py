from mcr_py.raptor.bag import (
    BaseLabel,
    Bag,
    RouteBag,
    TraceLabel,
)  # Adjust the import based on your module structure


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
    label1 = BaseLabel(10)
    label2 = BaseLabel(15)
    bag1.add(label1)
    bag2.add(label2)
    bag1.merge(bag2)
    assert len(bag1._bag) == 2


# Test for RouteBag
def test_route_bag_initialization():
    dq = None  # Mock or create a proper instance of DataQuerier or ExpandedDataQuerier
    route_bag = RouteBag(dq)
    assert len(route_bag._bag) == 0


def test_route_bag_add_if_necessary():
    dq = None  # Mock or create a proper instance of DataQuerier or ExpandedDataQuerier
    route_bag = RouteBag(dq)
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
    assert len(trace_label.traces) == 1  # Only the first trace should exist


# import pytest
# from mcr_py.raptor.bag import Bag, Label, RouteBag
#
#
# @pytest.fixture
# def sample_bag():
#     bag = Bag()
#     bag.add(Label(10, 20))
#     bag.add(Label(15, 15))
#     return bag
#
#
def test_bag_add_if_necessary(sample_bag: Bag):
    label = Label(100, 100)
    assert sample_bag.add_if_necessary(label) is False
    assert len(sample_bag._bag) == 2

    dominated_label = Label(14, 16)
    assert sample_bag.add_if_necessary(dominated_label) is True
    assert len(sample_bag._bag) == 3

    dominated_label = Label(1, 1)
    assert sample_bag.add_if_necessary(dominated_label) is True
    assert len(sample_bag._bag) == 1


#
#
# def test_bag_content_dominates(sample_bag: Bag):
#     dominating_label = Label(100, 100)
#     assert sample_bag.content_dominates(dominating_label) is True
#
#     non_dominating_label = Label(11, 21)
#     assert sample_bag.content_dominates(non_dominating_label) is True
#
#     non_dominating_label = Label(1, 1)
#     assert sample_bag.content_dominates(non_dominating_label) is False
#
#
# def test_bag_remove_dominated_by(sample_bag: Bag):
#     label = Label(1, 1)
#     sample_bag.remove_dominated_by(label)
#     assert len(sample_bag._bag) == 0
#
#
# def test_bag_merge(sample_bag: Bag):
#     other_bag = Bag()
#     other_bag.add(Label(14, 16))
#
#     assert sample_bag.merge(other_bag) is True
#     assert len(sample_bag._bag) == 3
#
#
# @pytest.fixture
# def sample_route_bag():
#     route_bag = RouteBag()
#     route_bag.add(Label(10, 20), "Trip 1")
#     route_bag.add(Label(15, 15), "Trip 2")
#     return route_bag
#
#
# def test_route_bag_add_if_necessary(sample_route_bag: RouteBag):
#     label = Label(100, 100)
#     trip = "Trip 3"
#     sample_route_bag.add_if_necessary(label, trip)
#     assert len(sample_route_bag._bag) == 2
#
#     dominated_label = Label(14, 16)
#     sample_route_bag.add_if_necessary(dominated_label, trip)
#     assert len(sample_route_bag._bag) == 3
#
#     dominated_label = Label(1, 1)
#     sample_route_bag.add_if_necessary(dominated_label, trip)
#     assert len(sample_route_bag._bag) == 1
#
#
# def test_route_bag_content_dominates(sample_route_bag: RouteBag):
#     dominating_label = Label(100, 100)
#     assert sample_route_bag.content_dominates(dominating_label) is True
#
#     non_dominating_label = Label(11, 21)
#     assert sample_route_bag.content_dominates(non_dominating_label) is True
#
#     non_dominating_label = Label(1, 1)
#     assert sample_route_bag.content_dominates(non_dominating_label) is False
#
#
# def test_route_bag_remove_dominated_by(sample_route_bag: RouteBag):
#     label = Label(1, 1)
#     sample_route_bag.remove_dominated_by(label)
#     assert len(sample_route_bag._bag) == 0
#
#
# def test_route_bag_update_arrival_times(sample_route_bag: RouteBag):
#     arrival_times = {"Trip 1": 5, "Trip 2": 10, "Trip 3": 15}
#     sample_route_bag.update_along_trip(arrival_times)
#
#     for label, trip in sample_route_bag._bag:
#         assert label.arrival_time == arrival_times[trip]
#
#
# def test_route_bag_get_trips(sample_route_bag: RouteBag):
#     trips = sample_route_bag.get_trips()
#     assert len(trips) == 2
#     assert "Trip 1" in trips
#     assert "Trip 2" in trips
#
#
# def test_route_bag_to_bag(sample_route_bag: RouteBag):
#     bag = sample_route_bag.to_bag()
#     assert isinstance(bag, Bag)
#     assert len(bag._bag) == 2
#
#
# def test_route_bag_copy(sample_route_bag: RouteBag):
#     copied_bag = sample_route_bag.copy()
#     assert isinstance(copied_bag, RouteBag)
#     assert len(copied_bag._bag) == 2
#     assert copied_bag._bag != sample_route_bag._bag
#     for label, trip in copied_bag._bag:
#         assert isinstance(label, Label)
#         assert isinstance(trip, str)
