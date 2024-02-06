import unittest

from etl.models.omopcdm54 import (
    OMOPCDM_MODEL_NAMES as TARGET_MODEL_NAMES,
    OMOPCDM_MODELS as TARGET_MODELS,
)
from etl.util.exceptions import FrozenClassException


class TargetModelsUnitTest(unittest.TestCase):
    def test_models(self):
        self.assertTrue(len(TARGET_MODELS) > 0)
        self.assertTrue(len(TARGET_MODEL_NAMES) > 0)

        good_bad_names = [
            ("Person", "Persons"),
            ("ObservationPeriod", "ObservatiomPeriod"),
            ("VisitOccurrence", "VisitOccurrences"),
            ("VisitDetail", "VisitDetails"),
            ("ConditionOccurrence", "ConditionOcurrence"),
            ("DrugExposure", "DrugExposures"),
            ("ProcedureOccurrence", "ProcedureOccurrences"),
            ("DeviceExposure", "DeviceExposures"),
            ("Measurement", "Measurements"),
            ("Observation", "Observations"),
            ("Death", "Deaths"),
            ("Note", "Notes"),
            ("NoteNlp", "NoteNlps"),
            ("Specimen", "Specimens"),
            ("FactRelationship", "FectRelationship"),
            ("CDMSource", "CDMsSource"),
            ("DrugEra", "DrugEta"),
        ]

        for good, bad in good_bad_names:
            self.assertTrue(
                good in TARGET_MODEL_NAMES,
                f"{good} Model not registered, but it should be!",
            )
            self.assertFalse(
                bad in TARGET_MODEL_NAMES,
                f"{bad} (bad name) Model is registered, but should not be!",
            )

    def test_immutable_class_attr_notexist(self):
        for cls in TARGET_MODELS:
            with self.assertRaises(FrozenClassException):
                cls.doesnotexist = 2

    def test_immutable_instance_attr_notexist(self):
        for cls in TARGET_MODELS:
            c = cls()
            with self.assertRaises(FrozenClassException):
                c.doesnotexist = 2


__all__ = ["TargetModelsUnitTest"]
