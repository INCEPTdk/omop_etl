"""Merge tests for concatenation of intervals."""
import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.standardized_derived_elements import DrugEra, ConditionEra
from etl.models.omopcdm54.clinical import ObservationPeriod
from etl.sql.merge.drug_era import concatenate_overlapping_intervals as concatenate_drugs
from etl.sql.merge.condition_era import concatenate_overlapping_intervals as concatenate_conditions
from etl.sql.merge.observation_period import concatenate_overlapping_intervals as concatenate_observations
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    assert_dataframe_equality,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class MergeConcatenateIntervals(DuckDBBaseTest):
    MODELS = [DrugEra, ConditionEra, ObservationPeriod]

    IN_DRUG_ERA = f"{base_path()}/test_data/merge_concatenate_intervals/in_drug_era.csv"
    OUT_DRUG_ERA = f"{base_path()}/test_data/merge_concatenate_intervals/out_drug_era.csv"
    IN_CONDITION_ERA = f"{base_path()}/test_data/merge_concatenate_intervals/in_condition_era.csv"
    OUT_CONDITION_ERA = f"{base_path()}/test_data/merge_concatenate_intervals/out_condition_era.csv"
    IN_OBSERVATION_PERIOD = f"{base_path()}/test_data/merge_concatenate_intervals/in_observation_period.csv"
    OUT_OBSERVATION_PERIOD = f"{base_path()}/test_data/merge_concatenate_intervals/out_observation_period.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.MODELS, schema='omopcdm')

        self.in_drug_era = pd.read_csv(self.IN_DRUG_ERA, index_col=False, sep=';')
        self.in_condition_era = pd.read_csv(self.IN_CONDITION_ERA, index_col=False, sep=';')
        self.in_observation_period = pd.read_csv(self.IN_OBSERVATION_PERIOD, index_col=False, sep=';')
        self.expected_drugs = pd.read_csv(self.OUT_DRUG_ERA, index_col=False, sep=';', parse_dates=['drug_era_start_date', 'drug_era_end_date'])
        self.expected_conditions = pd.read_csv(self.OUT_CONDITION_ERA, index_col=False, sep=';', parse_dates=['condition_era_start_date', 'condition_era_end_date'])
        self.expected_observations = pd.read_csv(self.OUT_OBSERVATION_PERIOD, index_col=False, sep=';', parse_dates=['observation_period_start_date', 'observation_period_end_date'])

        self.expected_drug_cols = [getattr(self.MODELS[0], col) for col in self.expected_drugs.columns.to_list() if col not in {"_id"}]
        self.expected_condition_cols = [getattr(self.MODELS[1], col) for col in self.expected_conditions.columns.to_list() if col not in {"_id"}]
        self.expected_observation_cols = [getattr(self.MODELS[2], col) for col in self.expected_observations.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.MODELS, schema='omopcdm')

    def test_drugs(self):
        write_to_db(self.engine, self.in_drug_era, DrugEra.__tablename__, schema=DrugEra.metadata.schema)

        with session_context(make_db_session(self.engine)) as session:
            session.execute(concatenate_drugs())

            result = select(self.expected_drug_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_drugs,
                pd.DataFrame(session.query(result).all())
            )
        assert_dataframe_equality(result_df, self.expected_drugs) 

    def test_conditions(self):

        write_to_db(self.engine, self.in_condition_era, ConditionEra.__tablename__, schema=ConditionEra.metadata.schema)
        with session_context(make_db_session(self.engine)) as session:
            session.execute(concatenate_conditions())

            result = select(self.expected_condition_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_conditions,
                pd.DataFrame(session.query(result).all())
            )
        assert_dataframe_equality(result_df, self.expected_conditions)

    def test_observations(self):
        write_to_db(self.engine, self.in_observation_period, ObservationPeriod.__tablename__, schema=ObservationPeriod.metadata.schema)

        with session_context(make_db_session(self.engine)) as session:
            session.execute(concatenate_observations())

            result = select(self.expected_observation_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_observations,
                pd.DataFrame(session.query(result).all())
            )
        assert_dataframe_equality(result_df, self.expected_observations)

__all__ = ["MergeConcatenateIntervals"]
