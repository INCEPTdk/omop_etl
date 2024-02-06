import unittest

from etl.models.source import SOURCE_MODEL_NAMES, SOURCE_MODELS
from etl.util.exceptions import FrozenClassException


class SourceModelsUnitTest(unittest.TestCase):
    @unittest.skip('Currently there are no source models in the codebase')
    def test_models(self):
        self.assertTrue(len(SOURCE_MODELS) > 0)
        self.assertTrue(len(SOURCE_MODEL_NAMES) > 0)

        good_bad_names = [
            ("ExampleSourceModel", "NotATable"),
        ]

        for good, bad in good_bad_names:
            self.assertTrue(
                good in SOURCE_MODEL_NAMES,
                f"{good} Model not registered, but it should be!",
            )
            self.assertFalse(
                bad in SOURCE_MODEL_NAMES,
                f"{bad} (bad name) Model is registered, but should not be!",
            )

    def test_immutable_class_attr_notexist(self):
        for cls in SOURCE_MODELS:
            with self.assertRaises(FrozenClassException):
                cls.doesnotexist = 2

    def test_immutable_instance_attr_notexist(self):
        for cls in SOURCE_MODELS:
            c = cls()
            with self.assertRaises(FrozenClassException):
                c.doesnotexist = 2


__all__ = ["SourceModelsUnitTest"]
