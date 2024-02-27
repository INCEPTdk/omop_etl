"""Death transformation tests"""

import pathlib

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import Person as OmopPerson
from etl.models.omopcdm54.clinical import Death as OmopDeath
from etl.models.source import Person as SourcePerson
from etl.transform.person import transform as person_transform
from etl.transform.death import transform as death_transform
from etl.util.db import make_db_session, session_context
from tests.testutils import base_path, PostgresBaseTest, write_to_db

class DeathTransformationTest(PostgresBaseTest):
    SOURCE_MODELS = [SourcePerson]
    TARGET_MODEL = [OmopPerson, OmopDeath]
    INPUT_FILE = f"{base_path()}/test_data/person/in_person.parquet.csv"
    OUTPUT_FILE = f"{base_path()}/test_data/person/out_death.csv"
    
    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._create_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')
        self.test_data_in = pd.read_csv(self.INPUT_FILE, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=['death_date', 'death_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[1], col) for col in self.expected_df.columns.to_list()]
    
    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._drop_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.test_data_in, SourcePerson.__tablename__, schema=SourcePerson.metadata.schema)

    def test_transform(self):
        self._insert_test_data(self.engine)
        with session_context(make_db_session(self.engine)) as session:
            person_transform(session)
            death_transform(session)

        # Only queries the columns that exist in the self.test_data_in
        result = select(self.expected_cols)
        result_df = pd.read_sql(result, self.engine)
        self.assertTrue(result_df.compare(self.expected_df).empty)


__all__ = ["DeathTransformationTest"]
