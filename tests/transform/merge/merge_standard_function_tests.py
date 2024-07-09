"""Merge standard function tests. This function simply takes CDM and merge them together (remapping the person_id)"""
from logging import getLogger

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    CareSite,
    Measurement,
    Person,
    VisitOccurrence,
)
from etl.sql.merge.mergeutils import _sql_get_care_site, _sql_merge_cdm_table
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class MergeStandardFunction(DuckDBBaseTest):
    MODELS = [Person, CareSite, VisitOccurrence, Measurement]

    INPUT_MERGED_PERSON = f"{base_path()}/test_data/merge/in_merge_person.csv"
    INPUT_SITE1_PERSON = f"{base_path()}/test_data/merge/in_site1_person.csv"
    INPUT_SITE2_PERSON = f"{base_path()}/test_data/merge/in_site2_person.csv"
    INPUT_SITE1_CARE_SITE = f"{base_path()}/test_data/merge/in_site1_care_site.csv"
    INPUT_SITE2_CARE_SITE = f"{base_path()}/test_data/merge/in_site2_care_site.csv"
    INPUT_MERGED_CARE_SITE = f"{base_path()}/test_data/merge/in_merge_care_site.csv"
    INPUT_SITE1_MEASUREMENT = f"{base_path()}/test_data/merge/in_site1_measurement.csv"
    INPUT_SITE2_MEASUREMENT = f"{base_path()}/test_data/merge/in_site2_measurement.csv"
    INPUT_SITE1_VISIT_OCCURRENCE = f"{base_path()}/test_data/merge/in_site1_visit_occurrence.csv"
    INPUT_SITE2_VISIT_OCCURRENCE = f"{base_path()}/test_data/merge/in_site2_visit_occurrence.csv"
    INPUT_MERGED_VISIT_OCCURRENCE = f"{base_path()}/test_data/merge/in_merge_visit_occurrence.csv"
    OUT_MERGED_MEASUREMENT = f"{base_path()}/test_data/merge/out_merge_measurement.csv"
    OUT_MERGED_MEASUREMENT_NO_REMAP = f"{base_path()}/test_data/merge/out_merge_measurement_no_remap.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.MODELS, schema='site1')
        self._create_tables_and_schemas(self.MODELS, schema='site2')
        self._create_tables_and_schemas(self.MODELS, schema='omopcdm')

        self.in_merged_person = pd.read_csv(self.INPUT_MERGED_PERSON, index_col=False, sep=';')
        self.in_site1_person = pd.read_csv(self.INPUT_SITE1_PERSON, index_col=False, sep=';')
        self.in_site2_person = pd.read_csv(self.INPUT_SITE2_PERSON, index_col=False, sep=';')
        self.in_site1_care_site = pd.read_csv(self.INPUT_SITE1_CARE_SITE, index_col=False, sep=';')
        self.in_site2_care_site = pd.read_csv(self.INPUT_SITE2_CARE_SITE, index_col=False, sep=';')
        self.in_merged_care_site = pd.read_csv(self.INPUT_MERGED_CARE_SITE, index_col=False, sep=';')
        self.in_site1_visit_occurrence = pd.read_csv(self.INPUT_SITE1_VISIT_OCCURRENCE, index_col=False, sep=';')
        self.in_site2_visit_occurrence = pd.read_csv(self.INPUT_SITE2_VISIT_OCCURRENCE, index_col=False, sep=';')
        self.in_merged_visit_occurrence = pd.read_csv(self.INPUT_MERGED_VISIT_OCCURRENCE, index_col=False, sep=';')
        self.in_site1_measurement = pd.read_csv(self.INPUT_SITE1_MEASUREMENT, index_col=False, sep=';')
        self.in_site2_measurement = pd.read_csv(self.INPUT_SITE2_MEASUREMENT, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUT_MERGED_MEASUREMENT, index_col=False, sep=';', parse_dates=['measurement_date', 'measurement_datetime'])
        self.expected_df_no_remap = pd.read_csv(self.OUT_MERGED_MEASUREMENT_NO_REMAP, index_col=False, sep=';', parse_dates=['measurement_date', 'measurement_datetime'])

        self.expected_cols = [getattr(self.MODELS[-1], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]
        self.expected_cols_no_remap = [getattr(self.MODELS[-1], col) for col in self.expected_df_no_remap.columns.to_list() if col not in {"_id"}]
        self._insert_test_data(self.engine)


    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.MODELS, schema='site1')
        self._drop_tables_and_schemas(self.MODELS, schema='site2')
        self._drop_tables_and_schemas(self.MODELS, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.in_merged_person, Person.__tablename__, schema=Person.metadata.schema)
        write_to_db(engine, self.in_site1_person, Person.__tablename__, schema="site1")
        write_to_db(engine, self.in_site2_person, Person.__tablename__, schema="site2")
        write_to_db(engine, self.in_site1_care_site, CareSite.__tablename__, schema="site1")
        write_to_db(engine, self.in_site2_care_site, CareSite.__tablename__, schema="site2")
        write_to_db(engine, self.in_merged_care_site, CareSite.__tablename__, schema=CareSite.metadata.schema)
        write_to_db(engine, self.in_site1_measurement, Measurement.__tablename__, schema="site1")
        write_to_db(engine, self.in_site2_measurement, Measurement.__tablename__, schema="site2")
        write_to_db(engine, self.in_site1_visit_occurrence, "visit_occurrence", schema="site1")
        write_to_db(engine, self.in_site2_visit_occurrence, "visit_occurrence", schema="site2")
        write_to_db(engine, self.in_merged_visit_occurrence, "visit_occurrence", schema=VisitOccurrence.metadata.schema)

    def test_get_correct_care_site(self):
        with session_context(make_db_session(self.engine)) as session:
            care_site_site1 = session.execute(_sql_get_care_site("site1")).fetchone()
            self.assertEqual(care_site_site1[0], 1)
            care_site_site2 = session.execute(_sql_get_care_site("site2")).fetchone()
            self.assertEqual(care_site_site2[0], 2)

    def test_transform_with_remapping(self):

        with session_context(make_db_session(self.engine)) as session:
            session.execute(
                _sql_merge_cdm_table(
                    "site1", Measurement,
                    cdm_columns=[c for c in Measurement.__table__.columns if c.key not in Measurement.__table__.primary_key.columns],
                    skip_person_remap=False,
                    care_site_id=1
                )
            )

            session.execute(
                _sql_merge_cdm_table(
                    "site2", Measurement,
                    cdm_columns=[c for c in Measurement.__table__.columns if c.key not in Measurement.__table__.primary_key.columns],
                    skip_person_remap=False,
                    care_site_id=2
                )
            )

            result = select(self.expected_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_df,
                pd.DataFrame(session.query(result).all())
            )
        pd.testing.assert_frame_equal(result_df, self.expected_df)

    def test_transform_without_remapping(self):

        with session_context(make_db_session(self.engine)) as session:
            session.execute(
                _sql_merge_cdm_table(
                    "site1", Measurement,
                    cdm_columns=[c for c in Measurement.__table__.columns if c.key not in Measurement.__table__.primary_key.columns],
                    skip_person_remap=True,
                    care_site_id=1
                )
            )

            session.execute(
                _sql_merge_cdm_table(
                    "site2", Measurement,
                    cdm_columns=[c for c in Measurement.__table__.columns if c.key not in Measurement.__table__.primary_key.columns],
                    skip_person_remap=True,
                    care_site_id=2
                )
            )

            result = select(self.expected_cols_no_remap).subquery()
            result_df = enforce_dtypes(
                self.expected_df_no_remap,
                pd.DataFrame(session.query(result).all())
            )
        pd.testing.assert_frame_equal(result_df, self.expected_df_no_remap)


__all__ = ["MergeStandardFunction"]
