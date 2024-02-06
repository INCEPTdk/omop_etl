"""Transformation logging"""
import types
from functools import partial
from logging import ERROR, Handler, Logger as Log, getLogger
from typing import Optional

import pandas as pd

from .exceptions import TransformationErrorException
from .memory import get_memory_use


def log_memory_use(logger: Log) -> None:
    """
    Utility function to log the current memory usage of
    the etl process.
    """
    used_memory = get_memory_use()
    logger.debug("Memory used: %s MB", used_memory / 1024000)


# pylint: disable=useless-option-value, unused-argument, no-self-use
class ErrorHandler(Handler):
    """A handler for checking if a critical error has been raised"""

    has_error = False

    def __init__(
        self,
        level: Optional[int] = ERROR,
        logger: Optional[str] = "",
        install: Optional[bool] = True,
    ) -> None:
        Handler.__init__(self)
        self.level = level
        self.logger = logger
        if install:
            self.install()

    def install(self) -> None:
        self.setLevel(self.level)
        getLogger(self.logger).addHandler(self)

    def emit(self, *args, **kwargs) -> None:
        ErrorHandler.has_error = True

    def reset(self) -> None:
        ErrorHandler.has_error = False

    def remove(self) -> None:
        getLogger().removeHandler(self)


class Logger:
    """Class for logging specific transformation information"""

    def __init__(self, func: types.FunctionType) -> None:
        self.func = func
        self.logger = getLogger("ETL.Core")

    def __get__(self, obj, *args, **kwargs):
        return partial(self.__call__, obj)

    def __call__(self, obj, *args, **kwargs) -> pd.DataFrame:
        try:
            result = self.func(obj, *args, **kwargs)
            log_memory_use(self.logger)
            return result
        except TransformationErrorException as exception:
            self.logger.critical(exception)
            return pd.DataFrame()
        except Exception as exception:  # pylint: disable=broad-except
            self.logger.critical(exception)
            return pd.DataFrame()
