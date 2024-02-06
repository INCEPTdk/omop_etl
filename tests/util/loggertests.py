import logging
import unittest
from typing import Any

from etl.util.logger import Logger


class LoggerUnitTests(unittest.TestCase):
    def test_logger_decorator_no_args_no_exception(self):
        class MyClass:
            @Logger
            def __call__(self, *args, **kwargs) -> Any:
                return 9

        c = MyClass()
        with self.assertLogs("ETL.Core", level=logging.DEBUG) as captured:
            self.assertEqual(len(captured.records), 0)
            c()
            self.assertEqual(len(captured.records), 1)

    def test_logger_decorator_no_args_with_exception(self):
        class MyClass:
            @Logger
            def __call__(self, *args, **kwargs) -> Any:
                raise RuntimeError("Test")

        c = MyClass()
        with self.assertLogs("ETL.Core", level=logging.CRITICAL) as captured:
            self.assertEqual(len(captured.records), 0)
            c()
            self.assertEqual(len(captured.records), 1)


__all__ = ["LoggerUnitTests"]
