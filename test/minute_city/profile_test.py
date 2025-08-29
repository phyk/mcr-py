import polars as pl
import pytest
from mcr_py.minute_city.profile import calculate_profile_for_group


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
def test_calculate_profile_for_group_simple(
    labels_frame_name: str,
    simple_types: list[str],
    results_expected: list[tuple[int, int]],
    request: pytest.FixtureRequest,
) -> None:
    labels_frame = request.getfixturevalue(labels_frame_name)
    result = calculate_profile_for_group(labels_frame, simple_types)
    assert result == results_expected
