""" A module for integration testing with postgres"""
import os
import unittest
from typing import Any, List, Optional

from etl.models.modelutils import create_tables_sql
from etl.util.connection import POSTGRES_DB, ConnectionDetails
from etl.util.db import (
    make_db_session,
    make_engine_postgres,
    session_context,
)


class PostgresBaseTest(unittest.TestCase):
    """Base class for testing with postgres"""

    def setUp(self):
        super().setUp()
        cxn_details = ConnectionDetails(
            dbms=POSTGRES_DB,
            host=os.getenv("ETL_TEST_TARGET_HOST", "localhost"),
            dbname=os.getenv("ETL_TEST_TARGET_DBNAME", "postgres"),
            user=os.getenv("ETL_TEST_TARGET_USER", "postgres"),
            password=os.getenv("ETL_TEST_TARGET_PASSWORD", "postgres"),
            port=os.getenv("ETL_TEST_TARGET_PORT", 5432),
        )
        self.engine = make_engine_postgres(cxn_details)

    def _drop_tables_and_schema(
        self, models: List[str], schema: Optional[str] = None
    ):
        with session_context(make_db_session(self.engine)) as session:
            schema_str = ""
            if schema is not None:
                schema_str = f"{schema}."

            with session.cursor() as cursor:
                for model in models:
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {schema_str}{model.__tablename__} CASCADE;"
                    )

                if schema is not None:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")

    def _create_tables_and_schema(
        self, models: List[Any], schema: Optional[str] = None
    ):
        with session_context(make_db_session(self.engine)) as session:
            with session.cursor() as cursor:
                if schema is not None:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                if models:
                    sql = create_tables_sql(models)
                    cursor.execute(sql)
