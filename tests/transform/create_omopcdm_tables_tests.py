from etl.sql.create_omopcdm_tables import MODELS
from etl.transform.create_omopcdm_tables import transform
from etl.util.db import (
    check_table_exists,
    make_db_session,
    session_context,
)
from tests.testutils import PostgresBaseTest


class CreateOMOPTablesPostgresTests(PostgresBaseTest):
    def test_transform(self):
        with session_context(make_db_session(self.engine)) as session:
            transform(session)

        for m in MODELS:
            self.assertTrue(
                check_table_exists(
                    self.engine, m.__tablename__, schema="omopcdm"
                )
            )


__all__ = ["CreateOMOPTablesPostgresTests"]
