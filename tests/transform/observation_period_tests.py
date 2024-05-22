""" Observation period transformation tests. """

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    ConditionOccurrence as OmopConditionOccurrence,
    Death as OmopDeath,
    DrugExposure as OmopDrugExposure,
    Measurement as OmopMeasurement,
    Observation as OmopObservation,
    ObservationPeriod as OmopObservationPeriod,
    Person as OmopPerson,
    ProcedureOccurrence as OmopProcedureOccurrence,
    VisitOccurrence as OmopVisitOccurrence,
)
from etl.transform.observation_period import (
    transform as observation_period_transformation,
)
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    PostgresBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class ObservationPeriodTransformationTest(PostgresBaseTest):

    OMOP_MODELS = [OmopMeasurement, OmopConditionOccurrence, OmopVisitOccurrence, OmopProcedureOccurrence, OmopObservation, OmopDrugExposure, OmopDeath, OmopPerson, OmopObservationPeriod]


    INPUT_OMOP_MEASUREMENT = f"{base_path()}/test_data/observation_period/in_omop_measurement.csv"
    INPUT_OMOP_CONDITION_OCCURRENCE = f"{base_path()}/test_data/observation_period/in_omop_condition_occurrence.csv"
    INPUT_OMOP_VISIT_OCCURRENCE = f"{base_path()}/test_data/observation_period/in_omop_visit_occurrence.csv"
    INPUT_OMOP_PROCEDURE_OCCURRENCE = f"{base_path()}/test_data/observation_period/in_omop_procedure_occurrence.csv"
    INPUT_OMOP_OBSERVATION = f"{base_path()}/test_data/observation_period/in_omop_observation.csv"
    INPUT_OMOP_DRUG_EXPOSURE = f"{base_path()}/test_data/observation_period/in_omop_drug_exposure.csv"
    INPUT_OMOP_DEATH = f"{base_path()}/test_data/observation_period/in_omop_death.csv"
    INPUT_OMOP_PERSON = f"{base_path()}/test_data/observation_period/in_omop_person.csv"

    OUTPUT_FILE = f"{base_path()}/test_data/observation_period/out_omop_observation_period.csv"

    DATETIME_COLS_TO_PARSE = ["observation_period_start_date", "observation_period_end_date"]

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.OMOP_MODELS, schema='omopcdm')


        self.omop_person = pd.read_csv(self.INPUT_OMOP_PERSON, index_col=False, sep=';')
        self.omop_visit_occurrence = pd.read_csv(self.INPUT_OMOP_VISIT_OCCURRENCE, index_col=False, sep=';')
        self.omop_measurement = pd.read_csv(self.INPUT_OMOP_MEASUREMENT, index_col=False, sep=';')
        self.omop_condition_occurrence = pd.read_csv(self.INPUT_OMOP_CONDITION_OCCURRENCE, index_col=False, sep=';')
        self.omop_procedure_occurrence = pd.read_csv(self.INPUT_OMOP_PROCEDURE_OCCURRENCE, index_col=False, sep=';')
        self.omop_observation = pd.read_csv(self.INPUT_OMOP_OBSERVATION, index_col=False, sep=';')
        self.omop_drug_exposure = pd.read_csv(self.INPUT_OMOP_DRUG_EXPOSURE, index_col=False, sep=';')
        self.omop_death = pd.read_csv(self.INPUT_OMOP_DEATH, index_col=False, sep=';')


        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=self.DATETIME_COLS_TO_PARSE)
        self.expected_cols = [getattr(self.OMOP_MODELS[-1], col) for col in self.expected_df.columns.to_list()]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.OMOP_MODELS, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.omop_person, OmopPerson.__tablename__, schema=OmopPerson.metadata.schema)
        write_to_db(engine, self.omop_visit_occurrence, OmopVisitOccurrence.__tablename__, schema=OmopVisitOccurrence.metadata.schema)
        write_to_db(engine, self.omop_measurement, OmopMeasurement.__tablename__, schema=OmopMeasurement.metadata.schema)
        write_to_db(engine, self.omop_condition_occurrence, OmopConditionOccurrence.__tablename__, schema=OmopConditionOccurrence.metadata.schema)
        write_to_db(engine, self.omop_procedure_occurrence, OmopProcedureOccurrence.__tablename__, schema=OmopProcedureOccurrence.metadata.schema)
        write_to_db(engine, self.omop_observation, OmopObservation.__tablename__, schema=OmopObservation.metadata.schema)
        write_to_db(engine, self.omop_drug_exposure, OmopDrugExposure.__tablename__, schema=OmopDrugExposure.metadata.schema)
        write_to_db(engine, self.omop_death, OmopDeath.__tablename__, schema=OmopDeath.metadata.schema)

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            observation_period_transformation(session)

        result = select(self.expected_cols)
        result_df = pd.read_sql(result, self.engine, parse_dates=self.DATETIME_COLS_TO_PARSE)
        result_df = enforce_dtypes(self.expected_df, result_df)
        pd.testing.assert_frame_equal(result_df, self.expected_df, check_like=True, check_datetimelike_compat=True)

__all__ = ['ObservationPeriodTransformationTest']
