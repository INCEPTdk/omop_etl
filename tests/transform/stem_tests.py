"""Stem transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    Concept as OmopConcept,
    Person as OmopPerson,
    Stem as OmopStem,
    VisitOccurrence as OmopVisitOccurrence,
)
from etl.models.omopcdm54.vocabulary import (
    ConceptRelationship as OmopConceptRelationship,
)
from etl.models.source import (
    Administrations as SourceAdministrations,
    CourseIdCprMapping as SourceCourseIdCprMapping,
    CourseMetadata as SourceCourseMetadata,
    DiagnosesProcedures as SourceDiagnosesProcedures,
    LabkaBccLaboratory as SourceLabkaBccLaboratory,
    LprDiagnoses as SourceLprDiagnoses,
    LprOperations as SourceLprOperations,
    LprProcedures as SourceLprProcedures,
    Observations as SourceObservations,
    Prescriptions as SourcePrescriptions,
)
from etl.models.tempmodels import ConceptLookup, ConceptLookupStem
from etl.transform.stem import transform as stem_transformation
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    assert_dataframe_equality,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class StemTransformationTest(DuckDBBaseTest):
    SOURCE_MODELS = [SourceCourseIdCprMapping, SourceCourseMetadata, SourceObservations, SourceAdministrations, SourcePrescriptions, SourceDiagnosesProcedures]
    REGISTRY_MODELS = [SourceLprDiagnoses, SourceLprProcedures, SourceLprOperations, SourceLabkaBccLaboratory]
    TARGET_MODEL = [OmopVisitOccurrence, OmopPerson, OmopStem]
    VOCAB_MODELS = [OmopConcept, OmopConceptRelationship]
    LOOKUPS = [ConceptLookup, ConceptLookupStem]

    CONCEPT_LOOKUP_DF = "etl/csv/concept_lookup.csv"
    CONCEPT_LOOKUP_STEM_DF = "etl/csv/concept_lookup_stem.csv"

    INPUT_VOCAB_CONCEPT = f"{base_path()}/test_data/stem/in_vocab_concept.csv"
    INPUT_VOCAB_CONCEPT_RELATIONSHIP = f"{base_path()}/test_data/stem/in_vocab_concept_relationship.csv"

    INPUT_SOURCE_COURSEIDCPRMAPPING = f"{base_path()}/test_data/stem/in_source_courseid_cpr_mapping.csv"
    INPUT_SOURCE_COURSEMETADATA = f"{base_path()}/test_data/stem/in_source_course_metadata.csv"
    INPUT_SOURCE_OBSERVATIONS = f"{base_path()}/test_data/stem/in_source_observations.csv"
    INPUT_SOURCE_ADMINISTRATIONS = f"{base_path()}/test_data/stem/in_source_administrations.csv"
    INPUT_SOURCE_PRESCRIPTIONS = f"{base_path()}/test_data/stem/in_source_prescriptions.csv"
    INPUT_SOURCE_DIAGNOSESPROCEDURES = f"{base_path()}/test_data/stem/in_source_diagnoses_procedures.csv"

    INPUT_REGISTRIES_DIAGNOSES = f"{base_path()}/test_data/stem/in_registries_diagnoses.csv"
    INPUT_REGISTRIES_PROCEDURES = f"{base_path()}/test_data/stem/in_registries_procedures.csv"
    INPUT_REGISTRIES_OPERATIONS = f"{base_path()}/test_data/stem/in_registries_operations.csv"
    INPUT_LABKA_BCC_LABORATORY = f"{base_path()}/test_data/stem/in_labka_bcc_laboratory.csv"

    INPUT_OMOP_PERSON = f"{base_path()}/test_data/stem/in_omop_person.csv"
    INPUT_OMOP_VISIT_OCCURRENCE = f"{base_path()}/test_data/stem/in_omop_visit_occurrence.csv"

    OUTPUT_FILE = f"{base_path()}/test_data/stem/out_omop_stem.csv"

    DATETIME_COLS_TO_PARSE = ['start_date', 'start_datetime', 'end_date', 'end_datetime'] # 'visit_start_datetime', 'visit_end_date', 'visit_end_datetime'])

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.VOCAB_MODELS)
        self._create_tables_and_schemas(self.LOOKUPS)
        self._create_tables_and_schemas(self.SOURCE_MODELS)
        self._create_tables_and_schemas(self.TARGET_MODEL)
        self._create_tables_and_schemas(self.REGISTRY_MODELS)

        self.concept_lookup = pd.read_csv(self.CONCEPT_LOOKUP_DF, index_col=False, sep=';')
        self.concept_lookup_stem = pd.read_csv(self.CONCEPT_LOOKUP_STEM_DF, index_col=False, sep=';', dtype=str)
        self.vocab_concept = pd.read_csv(self.INPUT_VOCAB_CONCEPT, index_col=False, sep=';')
        self.vocab_concept_relationship = pd.read_csv(self.INPUT_VOCAB_CONCEPT_RELATIONSHIP, index_col=False, sep=';')

        self.source_courseid_cpr_mapping = pd.read_csv(self.INPUT_SOURCE_COURSEIDCPRMAPPING, index_col=False, sep=';')
        self.source_course_metadata = pd.read_csv(self.INPUT_SOURCE_COURSEMETADATA, index_col=False, sep=';')
        self.source_observations = pd.read_csv(self.INPUT_SOURCE_OBSERVATIONS, index_col=False, sep=';', dtype={'value': str}, parse_dates=['timestamp'])
        self.source_administrations = pd.read_csv(self.INPUT_SOURCE_ADMINISTRATIONS, index_col=False, sep=';')
        self.source_prescriptions = pd.read_csv(self.INPUT_SOURCE_PRESCRIPTIONS, index_col=False, sep=';')
        self.source_diagnoses_procedures = pd.read_csv(self.INPUT_SOURCE_DIAGNOSESPROCEDURES, index_col=False, sep=';')

        self.source_lpr_diagnoses = pd.read_csv(self.INPUT_REGISTRIES_DIAGNOSES, index_col=False, sep=';')
        self.source_lpr_procedures = pd.read_csv(self.INPUT_REGISTRIES_PROCEDURES, index_col=False, sep=';')
        self.source_lpr_operations = pd.read_csv(self.INPUT_REGISTRIES_OPERATIONS, index_col=False, sep=';')
        self.source_labka_bcc_laboratory = pd.read_csv(self.INPUT_LABKA_BCC_LABORATORY, index_col=False, sep=';', parse_dates=['timestamp'])

        self.omop_person = pd.read_csv(self.INPUT_OMOP_PERSON, index_col=False, sep=';')
        self.omop_visit_occurrence = pd.read_csv(self.INPUT_OMOP_VISIT_OCCURRENCE, index_col=False, sep=';')

        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=self.DATETIME_COLS_TO_PARSE)
        self.expected_cols = [getattr(self.TARGET_MODEL[2], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.LOOKUPS)
        self._drop_tables_and_schemas(self.SOURCE_MODELS)
        self._drop_tables_and_schemas(self.TARGET_MODEL)
        self._drop_tables_and_schemas(self.REGISTRY_MODELS)
        self._drop_tables_and_schemas(self.VOCAB_MODELS)

    def _insert_test_data(self, engine):
        write_to_db(engine, self.concept_lookup, ConceptLookup.__tablename__, schema=ConceptLookup.metadata.schema)
        write_to_db(engine, self.concept_lookup_stem, ConceptLookupStem.__tablename__, schema=ConceptLookupStem.metadata.schema)
        write_to_db(engine, self.vocab_concept, OmopConcept.__tablename__, schema=OmopConcept.metadata.schema)
        write_to_db(engine, self.vocab_concept_relationship, OmopConceptRelationship.__tablename__, schema=OmopConceptRelationship.metadata.schema)
        write_to_db(engine, self.source_courseid_cpr_mapping, SourceCourseIdCprMapping.__tablename__, schema=SourceCourseIdCprMapping.metadata.schema)
        write_to_db(engine, self.source_course_metadata, SourceCourseMetadata.__tablename__, schema=SourceCourseMetadata.metadata.schema)
        write_to_db(engine, self.source_observations, SourceObservations.__tablename__, schema=SourceObservations.metadata.schema)
        write_to_db(engine, self.source_administrations, SourceAdministrations.__tablename__, schema=SourceAdministrations.metadata.schema)
        write_to_db(engine, self.source_prescriptions, SourcePrescriptions.__tablename__, schema=SourcePrescriptions.metadata.schema)
        write_to_db(engine, self.source_diagnoses_procedures, SourceDiagnosesProcedures.__tablename__, schema=SourceDiagnosesProcedures.metadata.schema)
        write_to_db(engine, self.omop_person, OmopPerson.__tablename__, schema=OmopPerson.metadata.schema)
        write_to_db(engine, self.omop_visit_occurrence, OmopVisitOccurrence.__tablename__, schema=OmopVisitOccurrence.metadata.schema)
        write_to_db(engine, self.source_lpr_diagnoses, SourceLprDiagnoses.__tablename__, schema=SourceLprDiagnoses.metadata.schema)
        write_to_db(engine, self.source_lpr_procedures, SourceLprProcedures.__tablename__, schema=SourceLprProcedures.metadata.schema)
        write_to_db(engine, self.source_lpr_operations, SourceLprOperations.__tablename__, schema=SourceLprOperations.metadata.schema)
        write_to_db(engine, self.source_labka_bcc_laboratory, SourceLabkaBccLaboratory.__tablename__, schema=SourceLabkaBccLaboratory.metadata.schema)

    def test_transform(self):
        with session_context(make_db_session(self.engine)) as session:
            self._insert_test_data(session)

            stem_transformation(session)
            result = select(self.expected_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_df,
                pd.DataFrame(session.query(result).all())
            )

        assert_dataframe_equality(result_df, self.expected_df, index_cols=['stem_id', 'source_concept_id'])

__all__ = ['StemTransformationTest']
