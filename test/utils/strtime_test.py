import sys

import pytest
from mcr_py.utils.strtime import InvalidTimeFormat, seconds_to_str_time, str_time_to_seconds


def test_str_time_to_seconds() -> None:
    assert str_time_to_seconds("00:00:00", accuracy_multiplier=1) == 0
    assert str_time_to_seconds("01:30:45", accuracy_multiplier=1) == 5445
    assert str_time_to_seconds("12:00:00", accuracy_multiplier=1) == 43200
    assert str_time_to_seconds("23:59:59", accuracy_multiplier=1) == 86399


def test_str_time_to_seconds_handles_past_midnight() -> None:
    assert str_time_to_seconds("24:00:00", accuracy_multiplier=1) == 86400
    assert str_time_to_seconds("25:30:45", accuracy_multiplier=1) == 91845
    assert str_time_to_seconds("36:00:00", accuracy_multiplier=1) == 129600


def test_seconds_to_str_time() -> None:
    assert seconds_to_str_time(0, accuracy_multiplier=1) == "00:00:00"
    assert seconds_to_str_time(5445, accuracy_multiplier=1) == "01:30:45"
    assert seconds_to_str_time(43200, accuracy_multiplier=1) == "12:00:00"
    assert seconds_to_str_time(86399, accuracy_multiplier=1) == "23:59:59"


def test_seconds_to_str_time_handles_maxsize() -> None:
    assert seconds_to_str_time(sys.maxsize, accuracy_multiplier=1) == "--:--:--"


def test_conversions_are_inverse() -> None:
    test_cases = [
        "00:00:00",
        "01:30:45",
        "12:00:00",
        "23:59:59",
        "24:00:00",
        "25:30:45",
        "36:00:00",
    ]
    for time_str in test_cases:
        seconds = str_time_to_seconds(time_str, accuracy_multiplier=1)
        assert seconds_to_str_time(seconds, accuracy_multiplier=1) == time_str
        seconds = str_time_to_seconds(time_str, accuracy_multiplier=10)
        assert seconds_to_str_time(seconds, accuracy_multiplier=10) == time_str


def test_invalid_time_format() -> None:
    with pytest.raises(InvalidTimeFormat):
        str_time_to_seconds("12:00", accuracy_multiplier=1)  # Missing seconds
    with pytest.raises(InvalidTimeFormat):
        str_time_to_seconds("12:00:60", accuracy_multiplier=1)  # Invalid seconds
    with pytest.raises(InvalidTimeFormat):
        str_time_to_seconds("12:00:00:00", accuracy_multiplier=1)  # Extra field
