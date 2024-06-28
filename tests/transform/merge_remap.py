"""Merge remap tests"""
import pandas as pd
from sqlalchemy import select, text

from etl.models.omopcdm54.clinical import Person, Measurement
from etl.sql.merge.mergeutils import remap_person_id
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    base_path,
    write_to_db,
    enforce_dtypes,
    assert_dataframe_equality
)


class MergeRemapTest(DuckDBBaseTest):
    MODELS = [Person, Measurement]

    IN_MERGED_PERSON = f"{base_path()}/test_data/merge_remap/in_merged_person.csv"
    IN_SITE_PERSON = f"{base_path()}/test_data/merge_remap/in_site_person.csv"
    IN_SITE_MEASUREMENT = f"{base_path()}/test_data/merge_remap/in_site_measurement.csv"
    OUT_REMAPPED_PERSON = f"{base_path()}/test_data/merge_remap/out_remapped_person.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.MODELS, schema='omopcdm')
        self._create_tables_and_schemas(self.MODELS, schema='site1')

        self.in_merged_person = pd.read_csv(self.IN_MERGED_PERSON, index_col=False, sep=';')
        self.in_site_person = pd.read_csv(self.IN_SITE_PERSON, index_col=False, sep=';')
        self.in_site_measurement = pd.read_csv(self.IN_SITE_MEASUREMENT, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUT_REMAPPED_PERSON, index_col=False, sep=';')

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.MODELS, schema='omopcdm')
        self._drop_tables_and_schemas(self.MODELS, schema='site1')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.in_merged_person, Person.__tablename__, schema=Person.metadata.schema)
        write_to_db(engine, self.in_site_person, Person.__tablename__, schema='site1')
        write_to_db(engine, self.in_site_measurement, Measurement.__tablename__, schema='site1')

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            SQL = remap_person_id('site1', Measurement, Person)
            result_df = session.connection_execute(SQL).df()
            result_df = enforce_dtypes(self.expected_df, result_df)

        assert_dataframe_equality(result_df, self.expected_df)

__all__ = ["MergeRemapTest"]
