"""Deduplication tests tests"""
import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import Person
from etl.sql.merge.mergeutils import drop_duplicate_rows
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class MergeDeduplicationsTest(DuckDBBaseTest):
    MODELS = [Person]

    INPUT_MERGED_PERSON = f"{base_path()}/test_data/merge_deduplication/in_person.csv"
    OUTPUT_DEDUPLICATED_PERSON = f"{base_path()}/test_data/merge_deduplication/out_person.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.MODELS, schema='omopcdm')

        self.in_merged_person = pd.read_csv(self.INPUT_MERGED_PERSON, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.INPUT_MERGED_PERSON, index_col=False, sep=';', parse_dates=['birth_datetime'])

        self.expected_cols = [getattr(self.MODELS[0], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.MODELS, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.in_merged_person, Person.__tablename__, schema=Person.metadata.schema)

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            session.execute(drop_duplicate_rows(Person, Person.person_id.key, Person.person_id.key))

            result = select(self.expected_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_df,
                pd.DataFrame(session.query(result).all())
            )

        pd.testing.assert_frame_equal(result_df, self.expected_df)

__all__ = ["MergeDeduplicationsTest"]
