"""Person transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import Person as OmopPerson
from etl.models.source import Person as SourcePerson
from etl.transform.person import transform
from etl.util.db import make_db_session, session_context
from tests.testutils import PostgresBaseTest, base_path, write_to_db


class PersonTransformationTest(PostgresBaseTest):
    SOURCE_MODELS = [SourcePerson]
    TARGET_MODEL = OmopPerson
    INPUT_SOURCE_PERSON = f"{base_path()}/test_data/person/in_person.csv"
    OUTPUT_OMOP_PERSON = f"{base_path()}/test_data/person/out_person.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.SOURCE_MODELS, schema='registries')
        self._create_tables_and_schema([self.TARGET_MODEL], schema='omopcdm')
        self.test_data_in = pd.read_csv(self.INPUT_SOURCE_PERSON, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUTPUT_OMOP_PERSON, index_col=False, sep=';')
        self.expected_cols = [getattr(self.TARGET_MODEL, col) for col in self.expected_df.columns.to_list()]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.SOURCE_MODELS, schema='registries')
        self._drop_tables_and_schema([self.TARGET_MODEL], schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.test_data_in, SourcePerson.__tablename__, schema=SourcePerson.metadata.schema)


    def test_transform(self):
        self._insert_test_data(self.engine)
        with session_context(make_db_session(self.engine)) as session:
            transform(session)

        # Only queries the columns that exist in the self.test_data_in
        result = select(self.expected_cols)
        result_df = pd.read_sql(result, self.engine)
        self.assertTrue(result_df.compare(self.expected_df).empty)


__all__ = ["PersonTransformationTest"]
