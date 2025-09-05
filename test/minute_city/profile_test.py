import polars as pl
import pytest
from mcr_py.minute_city.profile import (
    add_any_column_is_different_column,
    add_optimum_column,
    build_profiles_df,
    calculate_profile_for_group,
)


@pytest.fixture
def simple_labels_frame() -> pl.DataFrame:
    # Row 4 dominates row 1
    # Row 2 dominates row 3
    return pl.DataFrame(
        {
            "cost": [0, 0, 0, 0],
            "time": [5, 1, 10, 0],
            "park": [1, 0, 0, 1],
            "grocery": [0, 1, 1, 0],
        }
    )


@pytest.fixture
def difficult_labels_frame() -> pl.DataFrame:
    # Row 1 is cheaper than row 4 but slower
    # Row 2 dominates row 3
    return pl.DataFrame(
        {
            "cost": [0, 0, 5, 5],
            "time": [5, 1, 10, 0],
            "park": [1, 0, 0, 1],
            "grocery": [0, 1, 1, 0],
        }
    )


@pytest.fixture
def simple_types() -> list[str]:
    return ["park", "grocery"]


@pytest.mark.parametrize(
    ("labels_frame_name", "results_expected"),
    [("simple_labels_frame", [(0, 1)]), ("difficult_labels_frame", [(0, 5), (5, 1)])],
)
def test_calculate_profile_for_group(
    labels_frame_name: str,
    simple_types: list[str],
    results_expected: list[tuple[int, int]],
    request: pytest.FixtureRequest,
) -> None:
    labels_frame = request.getfixturevalue(labels_frame_name)
    result = calculate_profile_for_group(labels_frame, simple_types)
    assert result == results_expected


def test_build_profiles_df() -> None:
    results = {
        "hex00000": [(0, 15), (5, 11)],
        "hex00001": [(1, 14), (6, 12)],
        "hex00002": [(0, 11), (1, 14), (8, 10)],
    }
    profiles_df = build_profiles_df(results, 10)
    comp_df = pl.DataFrame(
        {
            "hex_id": ["hex00000", "hex00001", "hex00002"],
            "cost_0": [5.0, float("inf"), 1.0],
            "cost_1": [5.0, 4.0, 4.0],
            "cost_5": [1.0, 4.0, 4.0],
            "cost_6": [1.0, 2.0, 4.0],
            "cost_8": [1.0, 2.0, 0.0],
        }
    )
    assert profiles_df.equals(comp_df)
    results = {
        "hex00000": [(2, 15), (5, 11)],
        "hex00001": [(1, 14), (6, 12)],
    }
    profiles_df = build_profiles_df(results, 10)
    comp_df = pl.DataFrame(
        {
            "hex_id": ["hex00000", "hex00001"],
            "cost_0": [float("inf"), float("inf")],
            "cost_1": [float("inf"), 4.0],
            "cost_2": [5.0, 4.0],
            "cost_5": [1.0, 4.0],
            "cost_6": [1.0, 2.0],
        }
    )
    assert profiles_df.equals(comp_df)


def test_any_column_is_different_column() -> None:
    equal = pl.DataFrame({"cost_a": [1, 2, 3], "cost_b": [1, 2, 3], "cost_c": [1, 2, 3]})
    different = add_any_column_is_different_column(equal)
    assert different.get_column("any_column_different").to_list() == [False, False, False]

    equal = pl.DataFrame({"cost_a": [1, 2, 6], "cost_b": [1, 3, 3], "cost_c": [1, 2, 3]})
    different = add_any_column_is_different_column(equal)
    assert different.get_column("any_column_different").to_list() == [False, True, True]


def test_add_optimum_column() -> None:
    frame = pl.DataFrame({"cost_0": [1, 3, 6], "cost_5": [1, 3, 3], "cost_10": [1, 2, 3]})
    frame = add_optimum_column(frame)
    assert frame.get_column("optimal").to_list() == [1, 2, 3]
    assert frame.get_column("required_cost_for_optimal").to_list() == [0, 10, 5]
