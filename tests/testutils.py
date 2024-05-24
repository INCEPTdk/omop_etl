""" A module for integration testing with postgres"""
import inspect
import os
import unittest
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from etl.models.modelutils import create_tables_sql, drop_tables_sql
from etl.util.connection import POSTGRES_DB, ConnectionDetails
from etl.util.db import (
    make_db_session,
    make_engine_duckdb,
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

            for model in models:
                session.execute(
                    f"DROP TABLE IF EXISTS {schema_str}{model.__tablename__} CASCADE;"
                )

            if schema is not None:
                session.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")

    def _create_tables_and_schema(
        self, models: List[Any], schema: Optional[str] = None
    ):
        with session_context(make_db_session(self.engine)) as session:
            if schema is not None:
                session.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            if models:
                sql = create_tables_sql(models)
                session.execute(sql)

class DuckDBBaseTest(unittest.TestCase):
    """Base class for testing with duckdb"""

    def setUp(self):
        super().setUp()
        cxn_details = ConnectionDetails(
            host="duckdb",
            dbname=os.getenv("ETL_TEST_TARGET_DBNAME", ":memory:"),
        )
        self.engine = make_engine_duckdb(cxn_details)

    def _drop_tables_and_schema(
        self, models: List[str], schema: Optional[str] = None
    ):
        with session_context(make_db_session(self.engine)) as session:
            if schema is not None:
                session.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")

            if models:
                sql = drop_tables_sql(models)
                session.execute(sql)

    def _create_tables_and_schema(
        self, models: List[Any], schema: Optional[str] = None
    ):
        with session_context(make_db_session(self.engine)) as session:
            if schema is not None:
                session.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            if models:
                sql = create_tables_sql(models)
                session.execute(sql)

def write_to_db(session, table_frame: pd.DataFrame, table_name: str, schema: Optional[str]=None):
    table_name = "{}.{}".format(schema, table_name) if schema else table_name
    cols = "(" + ", ".join(table_frame.columns) + ")"
    session.execute(f"INSERT INTO {table_name} {cols} select * from table_frame")


def base_path() -> Path:
    caller_module = inspect.getmodule(inspect.stack()[1][0])
    return Path(caller_module.__file__).parent.resolve()

def enforce_dtypes(df_source, df_target):
    source_dtypes = df_source.dtypes
    df_target_converted = df_target.copy()  # To avoid modifying the original df_target

    for column, dtype in source_dtypes.items():
        if column in df_target_converted.columns:
            try:
                df_target_converted[column] = df_target_converted[column].astype(dtype)
            except (TypeError, ValueError) as e:
                print(f"Cannot convert column {column} to {dtype}: {e}")

    return df_target_converted

def assert_dataframe_equality(df1, df2, index_cols: str = None, **kwargs):
    if index_cols:
        if isinstance(index_cols, str):
            index_cols = [index_cols]

        for index_col in index_cols:
            df1 = df1.drop(columns=[index_col])
            df2 = df2.drop(columns=[index_col])

    column_names = sorted(df1.columns.tolist())

    sorted_df1 = df1.sort_values(by=column_names).reset_index(drop=True)
    sorted_df2 = df2.sort_values(by=column_names).reset_index(drop=True)

    pd.testing.assert_frame_equal(sorted_df1, sorted_df2, check_like=True, check_dtype=False, check_datetimelike_compat=True, **kwargs)
