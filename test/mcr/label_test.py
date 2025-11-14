from mcr_py.mcr.label import IntermediateLabel, merge_intermediate_bags


def test_merge_intermediate_bags() -> None:
    il1 = IntermediateLabel([1, 1], [1, 1], [1, "a"], 1)
    il2 = IntermediateLabel([2, 2], [2, 2], [2, "b"], 2)
    il3 = IntermediateLabel([2, 1], [2, 1], [2, "c"], 3)

    bag1 = {il1}
    bag2 = {il2, il3}

    merged_bag = merge_intermediate_bags(bag1, bag2)

    assert len(merged_bag) == 1
    assert il1 in merged_bag
    assert il3 not in merged_bag
    assert il2 not in merged_bag


def test_merge_intermediate_bags_overlapping() -> None:
    il1 = IntermediateLabel([5, 0], [1, 1], [1, "a"], 1)
    il2 = IntermediateLabel([0, 5], [2, 2], [2, "b"], 2)
    il5 = IntermediateLabel([1, 5], [2, 2], [2, "b"], 2)
    il3 = IntermediateLabel([2, 3], [2, 1], [2, "c"], 3)
    il6 = IntermediateLabel([3, 3], [2, 1], [2, "c"], 3)
    il0 = IntermediateLabel([0, 0], [2, 1], [2, "c"], 3)
    bag1 = {il1, il3, il5}
    bag2 = {il1, il2, il6}
    bag3 = {il1}
    bag4 = {il0}

    merged_bag = merge_intermediate_bags(bag1, bag2)
    assert len(merged_bag) == 3
    assert merged_bag == {il1, il2, il3}

    merged_bag = merge_intermediate_bags(bag1, bag3)
    assert len(merged_bag) == 3
    assert merged_bag == {il1, il3, il5}

    merged_bag = merge_intermediate_bags(bag3, bag1)
    assert len(merged_bag) == 3
    assert merged_bag == {il1, il3, il5}

    merged_bag = merge_intermediate_bags(bag1, bag4)
    assert len(merged_bag) == 1
    assert merged_bag == {il0}

    merged_bag = merge_intermediate_bags(bag4, bag1)
    assert len(merged_bag) == 1
    assert merged_bag == {il0}

    bag1 = set()
    merged_bag = merge_intermediate_bags(bag1, bag2)
    assert len(merged_bag) == 3
    assert merged_bag == bag2

    merged_bag = merge_intermediate_bags(bag2, bag1)
    assert len(merged_bag) == 3
    assert merged_bag == bag2
