"""Drug era transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import Stem as OmopStem
from etl.models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from etl.models.omopcdm54.vocabulary import Concept, ConceptAncestor
from etl.transform.drug_era import transform as drug_era_transformation
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    assert_dataframe_equality,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class DrugEraTest(DuckDBBaseTest):

    TARGET_MODELS = [OmopStem, OmopDrugEra, Concept, ConceptAncestor]

    INPUT_VOCAB_CONCEPT = f"{base_path()}/test_data/drug_era/in_vocab_concept.csv"
    INPUT_VOCAB_CONCEPT_ANCESTOR = f"{base_path()}/test_data/drug_era/in_vocab_concept_ancestor.csv"
    INPUT_OMOP_STEM = f"{base_path()}/test_data/drug_era/in_omop_stem.csv"
    OUTPUT_FILE = f"{base_path()}/test_data/drug_era/out_omop_drug_era.csv"
    DATETIME_COLS = ["drug_era_start_date", "drug_era_end_date"]

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.TARGET_MODELS)

        self.vocab_concept = pd.read_csv(self.INPUT_VOCAB_CONCEPT, index_col=False, sep=';')
        self.vocab_concept_ancestor = pd.read_csv(self.INPUT_VOCAB_CONCEPT_ANCESTOR, index_col=False, sep=';')
        self.omop_stem = pd.read_csv(self.INPUT_OMOP_STEM, index_col=False, sep=';')

        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates = self.DATETIME_COLS)
        self.expected_cols = [getattr(self.TARGET_MODELS[1], col) for col in self.expected_df.columns.to_list() if col not in {'_id'}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.TARGET_MODELS)

    def _insert_test_data(self, engine):
        write_to_db(engine, self.vocab_concept, Concept.__tablename__, schema=Concept.metadata.schema)
        write_to_db(engine, self.vocab_concept_ancestor, ConceptAncestor.__tablename__, schema=ConceptAncestor.metadata.schema)
        write_to_db(engine, self.omop_stem, OmopStem.__tablename__, schema=OmopStem.metadata.schema)

    def test_transform_drug_era(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            drug_era_transformation(session)

        result = select(self.expected_cols)
        result_df = pd.read_sql(result, self.engine)
        result_df = enforce_dtypes(self.expected_df, result_df)
        assert_dataframe_equality(result_df, self.expected_df)

__all__ = ['DrugEraTest']
