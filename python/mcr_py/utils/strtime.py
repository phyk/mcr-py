import sys


class InvalidTimeFormat(ValueError):
    pass


def str_time_to_seconds(str_time: str, accuracy_multiplier: int) -> int:
    """
    Converts a string representing time in the format HH:MM:SS to seconds since midnight.
    Can handle times that go past midnight.

    :param str_time: str - A time string formatted as HH:MM:SS.
    :returns: int - The total number of seconds since midnight.
    :raises InvalidTimeFormat: If the time format is invalid (e.g., minutes or seconds are 60 or more).
    """
    try:
        hours, minutes, seconds = map(int, str_time.split(":"))
    except ValueError as v:
        raise InvalidTimeFormat from v

    if minutes >= 60 or seconds >= 60:
        msg = "Invalid time format"
        raise InvalidTimeFormat(msg)

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds * accuracy_multiplier


def seconds_to_str_time(seconds: int, accuracy_multiplier: int) -> str:
    """
    Converts seconds since midnight to a string formatted as HH:MM:SS.
    Can handle times that go past midnight.

    :param seconds: int - The number of seconds since midnight.
    :returns: str - A time string formatted as HH:MM:SS. Returns '--:--:--' if seconds is sys.maxsize.
    """
    if seconds == sys.maxsize:
        return "--:--:--"
    seconds = seconds // accuracy_multiplier
    hours = seconds // 3600
    minutes = (seconds - hours * 3600) // 60
    seconds = seconds - hours * 3600 - minutes * 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"
