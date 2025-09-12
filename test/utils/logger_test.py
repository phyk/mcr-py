import logging
from io import StringIO
from unittest.mock import MagicMock, patch

from mcr_py.utils.logger import (
    Timed,
    Timer,
    copy_settings_to_root_logger,
    format_duration,
    make_string_stream_logger,
    setup,
)


def test_setup() -> None:
    """
    Tests the setup function by verifying logging configuration.
    """
    setup("DEBUG")
    assert logging.getLogger().level == logging.DEBUG


def test_timer_debug() -> None:
    """
    Tests the Timer debug method by verifying it calls Timed.debug.
    """
    with patch.object(Timed, "debug", return_value=None) as mock_debug:
        timer = Timer()
        timer.debug("Test message")
        mock_debug.assert_called_once_with("Test message", timer.logger)


def test_format_duration_seconds() -> None:
    """
    Tests format_duration for durations less than a minute.
    """
    assert format_duration(45.678) == "45.68 seconds"


def test_format_duration_minutes() -> None:
    """
    Tests format_duration for durations less than an hour.
    """
    assert format_duration(1234) == "20:34 minutes"


def test_format_duration_hours() -> None:
    """
    Tests format_duration for durations more than an hour.
    """
    assert format_duration(3661) == "1:01:01 hours"


def test_make_string_stream_logger() -> None:
    """
    Tests make_string_stream_logger by verifying logger and stream setup.
    """
    logger, stream = make_string_stream_logger("test_logger", logging.DEBUG)
    assert logger.name == "test_logger"
    assert logger.level == logging.DEBUG
    assert isinstance(stream, StringIO)


def test_copy_settings_to_root_logger() -> None:
    """
    Tests copy_settings_to_root_logger by verifying settings copy.
    """
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.WARNING)
    handler = logging.StreamHandler()
    logger.addHandler(handler)

    copy_settings_to_root_logger(logger)
    root_logger = logging.getLogger()

    assert root_logger.level == logging.WARNING
    assert root_logger.handlers == logger.handlers


def test_timed_context_manager() -> None:
    """
    Tests the Timed context manager by verifying log output with duration.
    """
    mock_logger = MagicMock()
    with Timed(logging.INFO, "Test message", mock_logger):
        pass
    mock_logger.log.assert_any_call(logging.INFO, "Test message")
    assert "done" in mock_logger.log.call_args_list[1][0][1]


def test_timed_context_manager_exception() -> None:
    """
    Tests the Timed context manager by verifying log output with exception handling.
    """
    mock_logger = MagicMock()
    try:
        with Timed(logging.INFO, "Test message", mock_logger):
            msg = "Test error"
            raise ValueError(msg)
    except ValueError:
        pass
    mock_logger.log.assert_any_call(logging.INFO, "Test message")
    assert "failed" in mock_logger.log.call_args_list[1][0][1]
