"""Death transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    Death as OmopDeath,
    Person as OmopPerson,
)
from etl.models.source import Person as RegistryPerson
from etl.transform.death import transform as death_transform
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    DuckDBBaseTest,
    assert_dataframe_equality,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class DeathTransformationTest(DuckDBBaseTest):
    REGISTRY_MODELS = [RegistryPerson]
    TARGET_MODEL = [OmopPerson, OmopDeath]
    INPUT_OMOP_PERSON = f"{base_path()}/test_data/death/in_omop_person.csv"
    INPUT_SOURCE_PERSON = f"{base_path()}/test_data/death/in_source_person.csv"
    OUTPUT_FILE = f"{base_path()}/test_data/death/out_death.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.REGISTRY_MODELS, schema='registries')
        self._create_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')
        self.source_person_in = pd.read_csv(self.INPUT_SOURCE_PERSON, index_col=False, sep=';')
        self.omop_person_in = pd.read_csv(self.INPUT_OMOP_PERSON, index_col=False, sep=';')
        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates=['death_date', 'death_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[1], col) for col in self.expected_df.columns.to_list() if col not in {"_id"}]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.REGISTRY_MODELS, schema='registries')
        self._drop_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

    def _insert_test_data(self, session):
        write_to_db(session, self.source_person_in, RegistryPerson.__tablename__, schema=RegistryPerson.metadata.schema)
        write_to_db(session, self.omop_person_in, OmopPerson.__tablename__, schema=OmopPerson.metadata.schema)

    def test_transform(self):
        del self.expected_df["_id"]  # no need to test stochastic columns

        with session_context(make_db_session(self.engine)) as session:
            self._insert_test_data(session)

            death_transform(session)
            result = select(self.expected_cols).subquery()
            result_df = enforce_dtypes(
                self.expected_df,
                pd.DataFrame(session.query(result).all())
            )

        assert_dataframe_equality(result_df, self.expected_df)

__all__ = ["DeathTransformationTest"]
