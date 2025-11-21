import logging
from typing import Callable

from mcr_py import PyLabel
from mcr_py.mcr.label import (
    IntermediateLabel,
    McRAPTORLabel,
    McRAPTORLabelWithPath,
)
from mcr_py.raptor.bag import Bag

# key is osm_node_id, value is list of labels
IntermediateBags = dict[int, set[IntermediateLabel]]


def convert_mlc_bags_to_intermediate_bags(
    bags: dict[int, list[PyLabel]],
    translate_node_id: Callable[[int], int],
) -> IntermediateBags:
    intermediate_bags = {
        translate_node_id(node_id): {
            IntermediateLabel(
                label.values,
                label.hidden_values,
                label.path,
                translate_node_id(node_id),
            )
            for label in bag
        }
        for node_id, bag in bags.items()
    }
    return intermediate_bags


def convert_mc_raptor_bags_to_intermediate_bags(
    bags: dict[int, Bag],
    min_path_length: int,
) -> IntermediateBags:
    intermediate_bags: dict[int, set[IntermediateLabel]] = {}
    for node_id, bag in bags.items():
        intermediate_bags[int(node_id)] = set()
        for label in bag:  # type: ignore
            if not isinstance(label, McRAPTORLabel):
                msg = f"Expected McRAPTORLabel, got {str(type(label))} instead"
                raise ValueError(msg)

            label: McRAPTORLabel = label
            if isinstance(label, McRAPTORLabelWithPath) and len(label.path) < min_path_length:
                continue

            intermediate_bags[int(node_id)].add(label.to_intermediate_label(int(node_id)))

    # remove empty bags
    intermediate_bags = {
        node_id: bag for node_id, bag in intermediate_bags.items() if len(bag) > 0
    }

    return intermediate_bags


def get_closest_cost(cost: int, limit_cache: dict[int, int]) -> int | None:
    """
    Returns the closest cost in the limit cache that is less than or equal to the given cost.
    If no such cost exists, returns None.
    """
    valid_costs = [c for c in limit_cache if c <= cost]
    if not valid_costs:
        return None
    return max(valid_costs)


def filter_bags_by_limits(
    bags: IntermediateBags,
    limit_cache: dict[int, int],
) -> IntermediateBags:
    filtered_bags: IntermediateBags = {}
    removed_labels = []
    for node_id, bag in bags.items():
        filtered_bag: set[IntermediateLabel] = set()
        for label in bag:
            time, cost = label.values
            closest_cost = get_closest_cost(cost, limit_cache)
            if time <= limit_cache[closest_cost]:  # type: ignore
                filtered_bag.add(label)
            else:
                if time < 292000:
                    removed_labels.append(label)

        if len(filtered_bag) > 0:
            filtered_bags[node_id] = filtered_bag
    logging.info(removed_labels)
    return filtered_bags
