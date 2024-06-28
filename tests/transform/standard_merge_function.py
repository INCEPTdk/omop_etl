"""Deduplication tests tests"""
import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import Person
from etl.models.omopcdm54.clinical import Measurement
from etl.sql.merge.mergeutils import _sql_merge_cdm_table
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class MergeStandardFunction(DuckDBBaseTest):
    MODELS = [Person, Measurement]

    INPUT_MERGED_PERSON = f"{base_path()}/test_data/merge/in_merge_person.csv"
    INPUT_SITE1_PERSON = f"{base_path()}/test_data/merge/in_site1_person.csv"
    INPUT_SITE2_PERSON = f"{base_path()}/test_data/merge/in_site2_person.csv"
    INPUT_SITE1_MEASUREMENT = f"{base_path()}/test_data/merge/in_site1_measurement.csv"
    INPUT_SITE2_MEASUREMENT = f"{base_path()}/test_data/merge/in_site2_measurement.csv"
    OUT_MERGED_MEASUREMENT = f"{base_path()}/test_data/merge/out_merge_measurement.csv"
    
    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.MODELS, schema='site1')
        self._create_tables_and_schemas(self.MODELS, schema='site2')
        self._create_tables_and_schemas(self.MODELS, schema='omopcdm')

        self.in_merged_person = pd.read_csv(self.INPUT_MERGED_PERSON, index_col=False, sep=';')
        self.in_site1_person = pd.read_csv(self.INPUT_SITE1_PERSON, index_col=False, sep=';')
        self.in_site2_person = pd.read_csv(self.INPUT_SITE2_PERSON, index_col=False, sep=';')
        self.in_site1_measurement = pd.read_csv(self.INPUT_SITE1_MEASUREMENT, index_col=False, sep=';')
        self.in_site2_measurement = pd.read_csv(self.INPUT_SITE2_MEASUREMENT, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUT_MERGED_MEASUREMENT, index_col=False, sep=';', parse_dates=['measurement_date', 'measurement_datetime'])

        self.expected_cols = [getattr(self.MODELS[-1], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.MODELS, schema='site1')
        self._drop_tables_and_schemas(self.MODELS, schema='site2')
        self._drop_tables_and_schemas(self.MODELS, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.in_merged_person, Person.__tablename__, schema=Person.metadata.schema)
        write_to_db(engine, self.in_site1_person, Person.__tablename__, schema="site1")
        write_to_db(engine, self.in_site2_person, Person.__tablename__, schema="site2")
        write_to_db(engine, self.in_site1_measurement, Measurement.__tablename__, schema="site1")
        write_to_db(engine, self.in_site2_measurement, Measurement.__tablename__, schema="site2") 

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            session.execute(_sql_merge_cdm_table(schemas=['site1', 'site2'], cdm_table=Measurement))

            result = select(self.expected_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_df,
                pd.DataFrame(session.query(result).all())
            )
        pd.testing.assert_frame_equal(result_df, self.expected_df) 

__all__ = ["MergeStandardFunction"]
