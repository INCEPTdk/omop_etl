"""Death transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    Death as OmopDeath,
    Person as OmopPerson,
)
from etl.models.source import Person as SourcePerson
from etl.transform.death import transform as death_transform
from etl.util.db import make_db_session, session_context
from tests.testutils import DuckDBBaseTest, base_path, write_to_db, enforce_dtypes, assert_dataframe_equality


class DeathTransformationTest(DuckDBBaseTest):
    SOURCE_MODELS = [SourcePerson]
    TARGET_MODEL = [OmopPerson, OmopDeath]
    INPUT_OMOP_PERSON = f"{base_path()}/test_data/death/in_omop_person.csv"
    INPUT_SOURCE_PERSON = f"{base_path()}/test_data/death/in_source_person.csv"
    OUTPUT_FILE = f"{base_path()}/test_data/death/out_death.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._create_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')
        self.source_person_in = pd.read_csv(self.INPUT_SOURCE_PERSON, index_col=False, sep=';')
        self.omop_person_in = pd.read_csv(self.INPUT_OMOP_PERSON, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=['death_date', 'death_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[1], col) for col in self.expected_df.columns.to_list()]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._drop_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

    def _insert_test_data(self, session):
        write_to_db(session, self.source_person_in, SourcePerson.__tablename__, schema=SourcePerson.metadata.schema)
        write_to_db(session, self.omop_person_in, OmopPerson.__tablename__, schema=OmopPerson.metadata.schema)

    def test_transform(self):
        with session_context(make_db_session(self.engine)) as session:
            self._insert_test_data(session)
            death_transform(session)

            # Only queries the columns that exist in the self.test_data_in
            result = str(
                select(self.expected_cols).compile(
                    compile_kwargs={"literal_binds": True}
                )
            )
            result_df = pd.read_sql(result, con=session.connection().connection)

        result_df = enforce_dtypes(self.expected_df, result_df)
        assert_dataframe_equality(result_df, self.expected_df, index_col="person_id")


__all__ = ["DeathTransformationTest"]
