"""Visit occurrence transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    CareSite as OmopCareSite,
    Person as OmopPerson,
    VisitOccurrence as OmopVisitOccurrence,
)
from etl.models.source import (
    CourseIdCprMapping as SourceCourseIdCprMapping,
    CourseMetadata as SourceCourseMetadata,
)
from etl.models.tempmodels import ConceptLookup
from etl.sql.visit_occurrence import get_visit_occurrence_insert
from etl.util.db import make_db_session, session_context
from tests.testutils import DuckDBBaseTest, base_path, write_to_db, enforce_dtypes, assert_dataframe_equality


class VisitOccurrenceTransformationTest(DuckDBBaseTest):
    SOURCE_MODELS = [SourceCourseIdCprMapping, SourceCourseMetadata]
    TARGET_MODEL = [OmopCareSite, OmopVisitOccurrence, OmopPerson]
    LOOKUPS = [ConceptLookup]

    CONCEPT_LOOKUP_DF = "etl/csv/concept_lookup.csv"
    INPUT_SOURCE_COURSEIDCPRMAPPING = f"{base_path()}/test_data/visit_occurrence/in_courseid_mapping.csv"
    INPUT_SOURCE_COURSEMETADATA = f"{base_path()}/test_data/visit_occurrence/in_course_metadata.csv"
    INPUT_OMOP_PERSON = f"{base_path()}/test_data/visit_occurrence/in_omop_person.csv"
    INPUT_OMOP_CARESITE = f"{base_path()}/test_data/visit_occurrence/in_omop_caresite_rigs.csv"

    OUTPUT_FILE = f"{base_path()}/test_data/visit_occurrence/out_visit_occurrence.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.LOOKUPS, schema='lookups')
        self._create_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._create_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

        self.concept_lookup = pd.read_csv(self.CONCEPT_LOOKUP_DF, index_col=False, sep=';')
        self.source_courseid_cpr_mapping = pd.read_csv(self.INPUT_SOURCE_COURSEIDCPRMAPPING, index_col=False, sep=';')
        self.source_course_metadata = pd.read_csv(self.INPUT_SOURCE_COURSEMETADATA, index_col=False, sep=';')

        self.omop_person = pd.read_csv(self.INPUT_OMOP_PERSON, index_col=False, sep=';')
        self.omop_caresite = pd.read_csv(self.INPUT_OMOP_CARESITE, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=['visit_start_date', 'visit_start_datetime', 'visit_end_date', 'visit_end_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[1], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.LOOKUPS, schema='lookups')
        self._drop_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._drop_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

    def _insert_test_data(self, session):

        write_to_db(session, self.concept_lookup, ConceptLookup.__tablename__, schema=ConceptLookup.metadata.schema)

        write_to_db(session, self.source_courseid_cpr_mapping, SourceCourseIdCprMapping.__tablename__, schema=SourceCourseIdCprMapping.metadata.schema)
        write_to_db(session, self.source_course_metadata, SourceCourseMetadata.__tablename__, schema=SourceCourseMetadata.metadata.schema)
        write_to_db(session, self.omop_person, OmopPerson.__tablename__, schema=OmopPerson.metadata.schema)
        write_to_db(session, self.omop_caresite, OmopCareSite.__tablename__, schema=OmopCareSite.metadata.schema)

    def test_transform(self):
        shak_code = "1301011"
        with session_context(make_db_session(self.engine)) as session:
            self._insert_test_data(session)
            session.execute(get_visit_occurrence_insert(shak_code))
            result = str(select(self.expected_cols).compile(self.engine, compile_kwargs={"literal_binds": True}))
            result_df = pd.read_sql(result, con=session.connection().connection)

        result_df = enforce_dtypes(self.expected_df, result_df)
        assert_dataframe_equality(result_df, self.expected_df, index_col='visit_occurrence_id')

__all__ = ['VisitOccurrenceTransformationTest']
