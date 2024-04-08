"""Stem transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    Person as OmopPerson,
    Stem as OmopStem,
    VisitOccurrence as OmopVisitOccurrence,
)
from etl.models.source import (
    Administrations as SourceAdministrations,
    CourseIdCprMapping as SourceCourseIdCprMapping,
    CourseMetadata as SourceCourseMetadata,
    DiagnosesProcedures as SourceDiagnosesProcedures,
    Observations as SourceObservations,
)
from etl.models.tempmodels import ConceptLookup, ConceptLookupStem
from etl.transform.stem import transform as stem_transformation
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    PostgresBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class StemTransformationTest(PostgresBaseTest):
    SOURCE_MODELS = [SourceCourseIdCprMapping, SourceCourseMetadata, SourceObservations, SourceAdministrations, SourceDiagnosesProcedures]
    TARGET_MODEL = [OmopVisitOccurrence, OmopPerson, OmopStem]
    LOOKUPS = [ConceptLookup, ConceptLookupStem]

    CONCEPT_LOOKUP_DF = "etl/csv/concept_lookup.csv"
    CONCEPT_LOOKUP_STEM_DF = "etl/csv/concept_lookup_stem.csv"

    INPUT_SOURCE_COURSEIDCPRMAPPING = f"{base_path()}/test_data/stem/in_source_courseid_cpr_mapping.csv"
    INPUT_SOURCE_COURSEMETADATA = f"{base_path()}/test_data/stem/in_source_course_metadata.csv"
    INPUT_SOURCE_OBSERVATIONS = f"{base_path()}/test_data/stem/in_source_observations.csv"
    INPUT_SOURCE_ADMINISTRATIONS = f"{base_path()}/test_data/stem/in_source_administrations.csv"
    INPUT_SOURCE_DIAGNOSESPROCEDURES = f"{base_path()}/test_data/stem/in_source_diagnoses_procedures.csv"

    INPUT_OMOP_PERSON = f"{base_path()}/test_data/stem/in_omop_person.csv"
    INPUT_OMOP_VISIT_OCCURRENCE = f"{base_path()}/test_data/stem/in_omop_visit_occurrence.csv"

    OUTPUT_FILE = f"{base_path()}/test_data/stem/out_omop_stem.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.LOOKUPS, schema='lookups')
        self._create_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._create_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

        self.concept_lookup = pd.read_csv(self.CONCEPT_LOOKUP_DF, index_col=False, sep=';')
        self.concept_lookup_stem = pd.read_csv(self.CONCEPT_LOOKUP_STEM_DF, index_col=False, sep=';', dtype=str)

        self.source_courseid_cpr_mapping = pd.read_csv(self.INPUT_SOURCE_COURSEIDCPRMAPPING, index_col=False, sep=';')
        self.source_course_metadata = pd.read_csv(self.INPUT_SOURCE_COURSEMETADATA, index_col=False, sep=';')
        self.source_observations = pd.read_csv(self.INPUT_SOURCE_OBSERVATIONS, index_col=False, sep=';', dtype={'value': str}, parse_dates=['timestamp'])
        self.source_administrations = pd.read_csv(self.INPUT_SOURCE_ADMINISTRATIONS, index_col=False, sep=';')
        self.source_diagnoses_procedures = pd.read_csv(self.INPUT_SOURCE_DIAGNOSESPROCEDURES, index_col=False, sep=';')

        self.omop_person = pd.read_csv(self.INPUT_OMOP_PERSON, index_col=False, sep=';')
        self.omop_visit_occurrence = pd.read_csv(self.INPUT_OMOP_VISIT_OCCURRENCE, index_col=False, sep=';')

        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=['start_date','start_datetime'])# 'visit_start_datetime', 'visit_end_date', 'visit_end_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[2], col) for col in self.expected_df.columns.to_list()]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.LOOKUPS, schema='lookups')
        self._drop_tables_and_schema(self.SOURCE_MODELS, schema='source')
        self._drop_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.concept_lookup, ConceptLookup.__tablename__, schema=ConceptLookup.metadata.schema)
        write_to_db(engine, self.concept_lookup_stem, ConceptLookupStem.__tablename__, schema=ConceptLookupStem.metadata.schema)
        write_to_db(engine, self.source_courseid_cpr_mapping, SourceCourseIdCprMapping.__tablename__, schema=SourceCourseIdCprMapping.metadata.schema)
        write_to_db(engine, self.source_course_metadata, SourceCourseMetadata.__tablename__, schema=SourceCourseMetadata.metadata.schema)
        write_to_db(engine, self.source_observations, SourceObservations.__tablename__, schema=SourceObservations.metadata.schema)
        write_to_db(engine, self.source_administrations, SourceAdministrations.__tablename__, schema=SourceAdministrations.metadata.schema)
        write_to_db(engine, self.source_diagnoses_procedures, SourceDiagnosesProcedures.__tablename__, schema=SourceDiagnosesProcedures.metadata.schema)
        write_to_db(engine, self.omop_person, OmopPerson.__tablename__, schema=OmopPerson.metadata.schema)
        write_to_db(engine, self.omop_visit_occurrence, OmopVisitOccurrence.__tablename__, schema=OmopVisitOccurrence.metadata.schema)

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            stem_transformation(session)

        result = select(self.expected_cols)
        result_df = pd.read_sql(result, self.engine)
        result_df = enforce_dtypes(self.expected_df, result_df)
        import pdb; pdb.set_trace()
        pd.testing.assert_frame_equal(result_df,
                                      self.expected_df)

__all__ = ['StemTransformationTest']
