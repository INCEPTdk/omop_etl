import json
import math
from typing import Any, Final

import pandas as pd
from sqlalchemy.orm import Session

from etl.models.modelutils import (
    CharField,
    FloatField,
    IntField,
    JSONField,
    make_model_base,
    PKIdMixin,
)
from etl.util.db import (
    DataBaseWriterBuilder,
    WriteMode,
    make_db_session,
    session_context,
)
from tests.testutils import DuckDBBaseTest


class DBDuckDBTests(DuckDBBaseTest):
    TestModelBase: Final[Any] = make_model_base(schema="dummy")

    class DummyTable(TestModelBase, PKIdMixin):
        __tablename__: Final = "dummy_table"
        __table_args__: Final = {"schema": "dummy"}

        a: Final = IntField()
        b: Final = CharField(10)
        camelCase: Final = FloatField(key='"camelCase"')
        json_field: Final = JSONField()

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(models=[self.DummyTable], schema="dummy")
        self._session = make_db_session(self.engine)
        self.dummy_df = pd.DataFrame(
            {
                "_id": [1, 2, 3],
                "a": [1, 4, 6],
                "b": ["test1", "test2", "dhsjak"],
                "json_field": [{"a": 1, "b": 2}, {"c": 3}, {"d": 4}],
                "camelCase": [435.34, None, 1432213.2],
            }
        )

    def tearDown(self):
        self._drop_tables_and_schema(models=[self.DummyTable], schema="dummy")

    def _find_precision(self, number: float = None) -> int:
        if isinstance(number, float):
            return len(str(number).split('.')[1])
        else:
            return 0

    def _round(self, x: float = None, decimals: int = 0) -> float:
        try:
            return round(x, decimals)
        except TypeError:
            return x

    def _assert_col(self, session: Session, col_name: str) -> None:
        entries = session.query(self.DummyTable).all()
        col_values = [getattr(e, col_name) for e in entries]
        expected = [None if pd.isna(v) else v for v in self.dummy_df[col_name].values]
        # Slight hack to Handle imprecision in floating point numbers
        decimals = (self._find_precision(n) for n in expected)
        col_values = [
            self._round(v, n_dec) if not pd.isna(v) else None for v, n_dec in zip(col_values, decimals)
        ]
        self.assertEqual(col_values, expected)

    def _assert_json_col(self, session: Session, col_name: str) -> None:
        entries = session.query(self.DummyTable).all()
        col_values = [getattr(e, col_name) for e in entries]
        expected = [
            None if pd.isna(v) else json.loads(json.dumps(v))
            for v in self.dummy_df[col_name].values
        ]
        self.assertEqual(col_values, expected)

    def _assert_col_default_primary_key(
        self, session: Session, col_name: str
    ) -> None:
        entries = session.query(self.DummyTable).all()
        col_values = [getattr(e, col_name) for e in entries]
        expected = [i + 1 for i, _ in enumerate(self.dummy_df[col_name].values)]
        self.assertEqual(col_values, expected)

    def _assert_col_null(self, session: Session, col_name: str) -> None:
        entries = session.query(self.DummyTable).all()
        col_values = [getattr(e, col_name) for e in entries]
        expected = [None for _ in self.dummy_df[col_name].values]
        self.assertEqual(col_values, expected)

    def test_database_writer_no_source(self):
        writer = DataBaseWriterBuilder().build()
        with session_context(self._session) as session:
            with self.assertRaises(RuntimeError):
                writer.write(session, columns=self.dummy_df.columns)

    def test_database_writer_all_columns(self):
        writer = DataBaseWriterBuilder().build()
        writer.set_source(self.DummyTable, self.dummy_df)

        with session_context(make_db_session(self.engine)) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=self.dummy_df.columns)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col(session, "a")
            self._assert_col(session, "b")
            self._assert_col(session, "camelCase")
            self._assert_json_col(session, "json_field")

    def test_database_writer_all_columns_twice(self):
        writer = (
            DataBaseWriterBuilder()
            .build()
            .set_source(self.DummyTable, self.dummy_df)
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=self.dummy_df.columns)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            writer.write(session, columns=self.dummy_df.columns)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")

    def test_database_writer_all_columns_append(self):
        writer = (
            DataBaseWriterBuilder()
            .set_write_mode(WriteMode.APPEND)
            .build()
            .set_source(self.DummyTable, self.dummy_df)
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=self.dummy_df.columns)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col(session, "a")
            self._assert_col(session, "b")
            self._assert_col(session, "camelCase")
            self._assert_json_col(session, "json_field")

    def test_database_writer_all_columns_append_twice(self):
        writer = (
            DataBaseWriterBuilder()
            .set_write_mode(WriteMode.APPEND)
            .build()
            .set_source(self.DummyTable, self.dummy_df)
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")

            # need to change the primary keys to avoid duplicates
            df2 = self.dummy_df.copy()
            df2["_id"] = df2["_id"] + 10
            writer.set_source(self.DummyTable, df2)
            writer.write(session)
            count = session.query(self.DummyTable).count()
            self.assertEqual(6, count, "assert dummy table")

    def test_database_writer_all_columns_default(self):
        writer = (
            DataBaseWriterBuilder()
            .build()
            .set_source(self.DummyTable, self.dummy_df)
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col(session, "a")
            self._assert_col(session, "b")
            self._assert_col(session, "camelCase")
            self._assert_json_col(session, "json_field")

    def test_database_writer_not_all_columns(self):
        writer = (
            DataBaseWriterBuilder()
            .build()
            .set_source(self.DummyTable, self.dummy_df)
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=["a", "b"])
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col(session, "a")
            self._assert_col(session, "b")
            self._assert_col_null(session, "camelCase")
            self._assert_col_null(session, "json_field")

    def test_database_writer_one_col_only(self):
        writer = (
            DataBaseWriterBuilder()
            .build()
            .set_source(self.DummyTable, self.dummy_df)
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=["b"],)
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col_default_primary_key(session, "_id")
            self._assert_col(session, "b")
            self._assert_col_null(session, "camelCase")
            self._assert_col_null(session, "json_field")

    def test_database_writer_last_col_only(self):
        writer = DataBaseWriterBuilder().set_null_field("nan").build()
        writer.set_source(
            self.DummyTable, self.dummy_df.astype({"camelCase": str})
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=["camelCase"])
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col_default_primary_key(session, "_id")
            self._assert_col_null(session, "b")
            self._assert_col(session, "camelCase")
            self._assert_col_null(session, "json_field")

    def test_database_writer_json_field(self):
        writer = DataBaseWriterBuilder().set_null_field("nan").build()
        writer.set_source(
            self.DummyTable, self.dummy_df.astype({"camelCase": str})
        )
        with session_context(self._session) as session:
            count = session.query(self.DummyTable).count()
            self.assertEqual(0, count, "assert dummy table")
            writer.write(session, columns=["json_field"])
            count = session.query(self.DummyTable).count()
            self.assertEqual(3, count, "assert dummy table")
            self._assert_col_default_primary_key(session, "_id")
            self._assert_col_null(session, "b")
            self._assert_col_null(session, "camelCase")
            self._assert_json_col(session, "json_field")


__all__ = ["DBDuckDBTests"]
