"""Condition era transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    ConditionOccurrence as OmopConditionOccurrence,
)
from etl.models.omopcdm54.standardized_derived_elements import (
    ConditionEra as OmopConditionEra,
)
from etl.models.omopcdm54.vocabulary import Concept, ConceptAncestor
from etl.transform.condition_era import (
    transform as condition_era_transformation,
)
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    assert_dataframe_equality,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class ConditionEraTest(DuckDBBaseTest):

    TARGET_MODELS = [OmopConditionOccurrence, OmopConditionEra]

    INPUT_OMOP_CONDITION_OCCURRENCE = f"{base_path()}/test_data/condition_era/in_omop_condition_occurrence.csv"
    OUTPUT_FILE = (
        f"{base_path()}/test_data/condition_era/out_omop_condition_era.csv"
    )
    DATETIME_COLS = ["condition_era_start_date", "condition_era_end_date"]

    def setUp(self):
        super().setUp()
        self._create_tables_and_schemas(self.TARGET_MODELS)

        self.input_omop_condition_occurrence = pd.read_csv(
            self.INPUT_OMOP_CONDITION_OCCURRENCE, index_col=False, sep=";"
        )

        self.expected_df = pd.read_csv(
            self.OUTPUT_FILE,
            index_col=False,
            sep=";",
            parse_dates=self.DATETIME_COLS,
            date_format="%Y-%m-%d",
        )
        self.expected_cols = [
            getattr(self.TARGET_MODELS[1], col)
            for col in self.expected_df.columns.to_list()
            if col not in {"_id"}
        ]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schemas(self.TARGET_MODELS)

    def _insert_test_data(self, engine):
        write_to_db(
            engine,
            self.input_omop_condition_occurrence,
            OmopConditionOccurrence.__tablename__,
            schema=OmopConditionOccurrence.metadata.schema,
        )

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            condition_era_transformation(session)
            result_sql = str(select(self.expected_cols).compile())
            result_df = session.connection_execute(result_sql).df()

        result_df = enforce_dtypes(self.expected_df, result_df)

        assert_dataframe_equality(
            result_df, self.expected_df, index_cols=["condition_era_id"]
        )


__all__ = ["ConditionEraTest"]
