"""Observation occurence transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    Observation as OmopObservation,
    Stem as OmopStem,
)
from etl.transform.observation import transform as observation_transformation
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
    assert_dataframe_equality
)


class ObservationTest(DuckDBBaseTest):

    TARGET_MODEL = [OmopStem, OmopObservation]

    INPUT_OMOP_STEM = f"{base_path()}/test_data/observation/in_omop_stem.csv"
    OUTPUT_FILE = f"{base_path()}/test_data/observation/out_omop_observation.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.TARGET_MODEL)


        self.omop_stem = pd.read_csv(self.INPUT_OMOP_STEM, index_col=False, sep=';')

        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates = ['observation_date','observation_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[1], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.TARGET_MODEL)

    def _insert_test_data(self, engine):
        write_to_db(engine, self.omop_stem, OmopStem.__tablename__, schema=OmopStem.metadata.schema)

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            observation_transformation(session)

            result = select(self.expected_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_df,
                pd.DataFrame(session.query(result).all())
            )

        assert_dataframe_equality(result_df, self.expected_df)

__all__ = ['ObservationTest']
