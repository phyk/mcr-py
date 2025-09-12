import sys


def str_time_to_seconds(str_time: str) -> int:
    """
    Converts a string representing time in the format HH:MM:SS to seconds since midnight.
    Can handle times that go past midnight.

    :param str_time: str - A time string formatted as HH:MM:SS.
    :returns: int - The total number of seconds since midnight.
    :raises ValueError: If the time format is invalid (e.g., minutes or seconds are 60 or more).
    """
    hours, minutes, seconds = map(int, str_time.split(":"))

    if minutes >= 60 or seconds >= 60:
        msg = "Invalid time format"
        raise ValueError(msg)

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds


def seconds_to_str_time(seconds: int) -> str:
    """
    Converts seconds since midnight to a string formatted as HH:MM:SS.
    Can handle times that go past midnight.

    :param seconds: int - The number of seconds since midnight.
    :returns: str - A time string formatted as HH:MM:SS. Returns '--:--:--' if seconds is sys.maxsize.
    """
    if seconds == sys.maxsize:
        return "--:--:--"
    hours = seconds // 3600
    minutes = (seconds - hours * 3600) // 60
    seconds = seconds - hours * 3600 - minutes * 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"
