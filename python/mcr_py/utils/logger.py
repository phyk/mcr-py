import inspect
import io
import logging
import pathlib
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING
from time import time

import click
from rich.console import Console
from rich.logging import RichHandler


def setup(log_level: str, setup_case: str = "console") -> None:
    """
    Sets up logging configuration using RichHandler with a specified log level.

    :param log_level: str - The log level to set for the logger (e.g., 'DEBUG', 'INFO').
    """
    FORMAT = "%(message)s"
    if setup_case == "vscode-jupyter":
        logging.basicConfig(
            level=log_level,
            format=FORMAT,
            datefmt="[%X]",
            handlers=[
                RichHandler(
                    rich_tracebacks=True,
                    enable_link_path=False,
                    console=Console(force_jupyter=False, width=130),
                    tracebacks_suppress=[click],
                )
            ],
            force=True,
        )
    else:
        logging.basicConfig(
            level=log_level,
            format=FORMAT,
            datefmt="[%X]",
            handlers=[
                RichHandler(
                    rich_tracebacks=True,
                    console=Console(force_jupyter=False),
                    tracebacks_suppress=[click],
                )
            ],
            force=True,
        )


rlog = logging.getLogger("rich")

nlog = logging.getLogger("null")
null_handler = logging.NullHandler()
nlog.addHandler(null_handler)


class Timer:
    def __init__(self, logger=rlog) -> None:
        """
        Initializes a Timer instance with a specified logger.

        :param logger: logging.Logger - The logger to use for timed logging operations. Defaults to 'rlog'.
        """
        self.logger = logger

    def debug(self, msg, *args, **kwargs):
        """
        Logs a debug message with timing using the Timed class.

        :param msg: str - The message to log.
        """
        return Timed.debug(msg, self.logger, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """
        Logs an info message with timing using the Timed class.

        :param msg: str - The message to log.
        """
        return Timed.info(msg, self.logger, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """
        Logs a warning message with timing using the Timed class.

        :param msg: str - The message to log.
        """
        return Timed.warning(msg, self.logger, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """
        Logs an error message with timing using the Timed class.

        :param msg: str - The message to log.
        """
        return Timed.error(msg, self.logger, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """
        Logs a critical message with timing using the Timed class.

        :param msg: str - The message to log.
        """
        return Timed.critical(msg, self.logger, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        """
        Logs a message at a specified level with timing using the Timed class.

        :param level: int - The log level for the message.
        :param msg: str - The message to log.
        """
        return Timed.log(level, msg, self.logger, *args, **kwargs)


class Timed:
    def __init__(self, level, msg, logger=rlog, *args, **kwargs) -> None:
        """
        Initializes a Timed instance for logging with timing information.

        :param level: int - The log level for the message.
        :param msg: str - The message to log.
        :param logger: logging.Logger - The logger to use for logging. Defaults to 'rlog'.
        """
        self.level = level
        self.msg = msg
        self.args = args
        self.kwargs = kwargs
        self.time = time()
        self.logger = logger

        # Create a filter that changes the log record to point to the calling frame
        # from this file to the file that actually called Timed
        calling_frame = inspect.stack()[2].frame
        trace = inspect.getframeinfo(calling_frame)

        class UpStackFilter(logging.Filter):
            def filter(self, record) -> bool:
                record.lineno = trace.lineno
                record.pathname = trace.filename
                record.filename = pathlib.Path(trace.filename).name
                return True

        self.f = UpStackFilter()

    def __enter__(self):
        """
        Adds the UpStackFilter and logs the message when entering the context.
        """
        self.logger.addFilter(self.f)
        self.logger.log(self.level, self.msg, *self.args, **self.kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Logs the message with the outcome and duration when exiting the context.

        :param exc_type: Exception type if an exception occurred.
        :param exc_value: Exception value if an exception occurred.
        :param traceback: Traceback if an exception occurred.
        """
        duration = time() - self.time
        outcome = "failed" if exc_type else "done"
        self.logger.log(
            self.level,
            self.msg + f" {outcome} ({format_duration(duration)})",
            *self.args,
            **self.kwargs,
        )
        self.logger.removeFilter(self.f)

    @staticmethod
    def debug(msg, logger=rlog, *args, **kwargs):
        """
        Logs a debug message with timing.

        :param msg: str - The message to log.
        """
        return Timed(DEBUG, msg, logger, *args, **kwargs)

    @staticmethod
    def info(msg, logger=rlog, *args, **kwargs):
        """
        Logs an info message with timing.

        :param msg: str - The message to log.
        """
        return Timed(INFO, msg, logger, *args, **kwargs)

    @staticmethod
    def warning(msg, logger=rlog, *args, **kwargs):
        """
        Logs a warning message with timing.

        :param msg: str - The message to log.
        """
        return Timed(WARNING, msg, logger, *args, **kwargs)

    @staticmethod
    def error(msg, logger=rlog, *args, **kwargs):
        """
        Logs an error message with timing.

        :param msg: str - The message to log.
        """
        return Timed(ERROR, msg, logger, *args, **kwargs)

    @staticmethod
    def critical(msg, logger=rlog, *args, **kwargs):
        """
        Logs a critical message with timing.

        :param msg: str - The message to log.
        """
        return Timed(CRITICAL, msg, logger, *args, **kwargs)

    @staticmethod
    def log(level, msg, logger=rlog, *args, **kwargs):
        """
        Logs a message at a specified level with timing.

        :param level: int - The log level for the message.
        :param msg: str - The message to log.
        """
        return Timed(level, msg, logger, *args, **kwargs)


def format_duration(duration: float) -> str:
    """
    Formats a duration in seconds into a human-readable string.

    :param duration: float - The duration in seconds.
    :returns: str - The formatted duration string.
    """
    if duration < 60:
        return f"{duration:.2f} seconds"

    duration = int(duration)

    if duration < 3600:
        return f"{duration // 60}:{duration % 60:02d} minutes"
    return f"{duration // 3600}:{(duration % 3600) // 60:02d}:{duration % 60:02d} hours"


def make_string_stream_logger(name: str | None, level: int = logging.INFO):
    """
    Creates a logger that logs to a string stream.

    :param name: str | None - The name of the logger. Can be None for root logger.
    :param level: int - The logging level. Defaults to logging.INFO.
    :returns: tuple - A tuple containing the logger and the string stream.
    """
    log_stream = io.StringIO()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger, log_stream


def copy_settings_to_root_logger(logger: logging.Logger) -> None:
    """
    Copies settings from a specified logger to the root logger.

    :param logger: logging.Logger - The logger whose settings are to be copied.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logger.level)
    root_logger.handlers = logger.handlers
    root_logger.filters = logger.filters
    root_logger.propagate = logger.propagate
