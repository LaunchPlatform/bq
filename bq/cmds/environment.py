import dataclasses
import enum
import logging

import click

from ..app import BeanQueue


@enum.unique
class LogLevel(enum.Enum):
    VERBOSE = "verbose"
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


LOG_LEVEL_MAP = {
    LogLevel.DEBUG: logging.DEBUG,
    LogLevel.INFO: logging.INFO,
    LogLevel.WARNING: logging.WARNING,
    LogLevel.ERROR: logging.ERROR,
    LogLevel.FATAL: logging.FATAL,
}


@dataclasses.dataclass
class Environment:
    log_level: LogLevel = LogLevel.INFO
    logger: logging.Logger = logging.getLogger("bq")
    app: BeanQueue = BeanQueue()


pass_env = click.make_pass_decorator(Environment, ensure=True)
