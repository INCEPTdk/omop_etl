""" unit tests for care_site transformation"""
# pylint: skip-file
import unittest
from dataclasses import dataclass
from typing import Any, Final

import pandas as pd

from etl.models.modelutils import (
    CharField,
    FloatField,
    IntField,
    make_model_base,
)
from etl.util.batcher import (
    DataFrameBatch,
    batch_process,
    source_to_target_batch,
)
from etl.util.db import Session, make_db_session, session_context
from tests.testutils import PostgresBaseTest


class DataFrameBatcherTests(unittest.TestCase):
    """testing source_to_target_batch and batch_process"""

    class StubCursor:
        def fetchmany(self, limit: int) -> Any:
            return 4

    @dataclass
    class StubConnection:
        query: str = ""

        def query_cursor(self, sql: str, **params) -> Any:
            self.query = sql
            return DataFrameBatcherTests.StubCursor()

    def test_batcher_process_once(self):
        @dataclass
        class MockBatch:
            called: bool = False

            def insert(self, fetched: Any) -> int:
                result = 0 if self.called else 14
                self.called = True
                return result

        batch = MockBatch()
        cnxn = self.StubConnection()
        cursor = cnxn.query_cursor("test")
        self.assertEqual(batch.called, False)
        count = sum(source_to_target_batch(cursor, batch, 8))
        self.assertEqual(batch.called, True)
        self.assertEqual(count, 14)

    def test_batcher_process_several_batches(self):
        limit: int = 4
        start_value: int = 1

        @dataclass
        class MockBatch:
            value: int

            def insert(self, fetched: Any) -> int:
                self.value += 1
                return 0 if self.value > limit else self.value

        batch = MockBatch(start_value)
        cnxn = self.StubConnection()
        cursor = cnxn.query_cursor("test")
        count = sum(source_to_target_batch(cursor, batch, limit))
        self.assertEqual(count, sum(list(range(start_value + 1, limit + 1))))

    def test_batcher_process_several_batches_func(self):
        limit: int = 4
        start_value: int = 1

        @dataclass
        class MockBatch:
            value: int

            def insert(self, fetched: Any) -> int:
                self.value += 1
                return 0 if self.value > limit else self.value

        cursor = self.StubCursor()
        batch = MockBatch(start_value)
        count = batch_process(cursor, batch, limit)
        self.assertEqual(count, sum(list(range(start_value + 1, limit + 1))))

    def test_batcher_process_several_batches_empty(self):
        class EmptyBatch:
            def insert(self, fetched: Any) -> int:
                return 0

        cursor = self.StubCursor()
        batch = EmptyBatch()
        count = batch_process(cursor, batch, 23)
        self.assertEqual(count, 0)


class DataFrameBatcherPostgresTests(PostgresBaseTest):
    TestModelBase: Final[Any] = make_model_base()

    class SourceA(TestModelBase):
        __tablename__: Final = "source_a"
        __table_args__ = {"schema": "test_source"}

        a: Final = IntField(primary_key=True)
        b: Final = CharField(10)
        c: Final = FloatField()

    class SourceB(TestModelBase):
        __tablename__: Final = "source_b"
        __table_args__ = {"schema": "test_source"}

        d: Final = IntField(primary_key=True)
        e: Final = CharField(20)

    class TargetA(TestModelBase):
        __tablename__: Final = "target_a"
        __table_args__ = {"schema": "test_target"}

        a: Final = IntField(primary_key=True)
        b: Final = CharField(10)
        c: Final = FloatField()

    class TargetB(TestModelBase):
        __tablename__: Final = "target_b"
        __table_args__ = {"schema": "test_target"}

        d: Final = IntField(primary_key=True)
        e: Final = CharField(20)

    def setUp(self):
        super().setUp()
        self._drop_tables_and_schema(
            models=[self.SourceA, self.SourceB], schema="test_source"
        )
        self._create_tables_and_schema(
            models=[self.SourceA, self.SourceB], schema="test_source"
        )
        self._drop_tables_and_schema(
            models=[self.TargetA, self.TargetB], schema="test_target"
        )
        self._create_tables_and_schema(
            models=[self.TargetA, self.TargetB], schema="test_target"
        )
        self.dummy_a = pd.DataFrame(
            {
                self.SourceA.a.key: [1, 4, 6],
                self.SourceA.b.key: ["test1", "test2", "dhsjak"],
                self.SourceA.c.key: [435.34, None, 1432213.2],
            }
        )
        self.dummy_b = pd.DataFrame(
            {
                self.SourceB.d.key: [3, 4, 5, 6, 7, 9, 13, 14, 342, 123, 222],
                self.SourceB.e.key: [
                    "3",
                    "4",
                    "5",
                    "6",
                    "7",
                    "9",
                    "13",
                    "14",
                    "342",
                    "123",
                    "222",
                ],
            }
        )
        with session_context(make_db_session(self.engine)) as session:
            self.assertEqual(session.query(self.SourceA).count(), 0)
            self.assertEqual(session.query(self.SourceB).count(), 0)
            self.assertEqual(session.query(self.TargetA).count(), 0)
            self.assertEqual(session.query(self.TargetB).count(), 0)

    def _set_up_source_dummy(self, session: Session) -> None:
        for _, row in self.dummy_a.iterrows():
            session.add(self.SourceA(**row.to_dict()))
        for _, row in self.dummy_b.iterrows():
            session.add(self.SourceB(**row.to_dict()))
        self.assertEqual(session.query(self.SourceA).count(), 3)
        self.assertEqual(session.query(self.SourceB).count(), 11)
        self.assertEqual(session.query(self.TargetA).count(), 0)
        self.assertEqual(session.query(self.TargetB).count(), 0)

    def test_batcher_single_df(self):
        with session_context(make_db_session(self.engine)) as session:
            self._set_up_source_dummy(session)

            target_batch = DataFrameBatch(
                session,
                str(self.TargetA.__table__),
                model=self.TargetA,
            )

            # use same database as both source and target
            with session.cursor() as source_cursor:
                source_cursor.execute(str(self.SourceA.__table__.select()))
                row_count = batch_process(source_cursor, target_batch, limit=3)
                self.assertEqual(row_count, 3, "3 rows should be inserted")
                self.assertEqual(session.query(self.SourceA).count(), 3)
                self.assertEqual(session.query(self.SourceB).count(), 11)
                self.assertEqual(session.query(self.TargetA).count(), 3)
                self.assertEqual(session.query(self.TargetB).count(), 0)

    def test_batcher_single_df_zero_limit(self):
        with session_context(make_db_session(self.engine)) as session:
            self._set_up_source_dummy(session)

            target_batch = DataFrameBatch(
                session,
                str(self.TargetB.__table__),
                model=self.TargetB,
            )

            # use same database as both source and target
            with session.cursor() as source_cursor:
                source_cursor.execute(str(self.SourceB.__table__.select()))
                row_count = batch_process(source_cursor, target_batch, limit=0)
                self.assertEqual(row_count, 0, "0 rows should be inserted")

                self.assertEqual(session.query(self.SourceB).count(), 11)
                self.assertEqual(session.query(self.TargetB).count(), 0)

    def test_batcher_single_df_low_limit(self):
        with session_context(make_db_session(self.engine)) as session:
            self._set_up_source_dummy(session)

            target_batch = DataFrameBatch(
                session,
                str(self.TargetB.__table__),
                model=self.TargetB,
            )

            # use same database as both source and target
            with session.cursor() as source_cursor:
                source_cursor.execute(str(self.SourceB.__table__.select()))
                row_count = batch_process(source_cursor, target_batch, limit=2)
                self.assertEqual(row_count, 11, "11 rows should be inserted")

                self.assertEqual(session.query(self.SourceB).count(), 11)
                self.assertEqual(session.query(self.TargetB).count(), 11)

                results_d = [v.d for v in session.query(self.TargetB).all()]
                results_e = [v.e for v in session.query(self.TargetB).all()]
                self.assertEqual(
                    results_d, list(self.dummy_b[self.TargetB.d.key].values)
                )
                self.assertEqual(
                    results_e, list(self.dummy_b[self.TargetB.e.key].values)
                )

    def test_batcher_single_df_high_limit(self):
        with session_context(make_db_session(self.engine)) as session:
            self._set_up_source_dummy(session)

            target_batch = DataFrameBatch(
                session,
                str(self.TargetB.__table__),
                model=self.TargetB,
            )

            # use same database as both source and target
            with session.cursor() as source_cursor:
                source_cursor.execute(str(self.SourceB.__table__.select()))
                row_count = batch_process(
                    source_cursor, target_batch, limit=100
                )
                self.assertEqual(row_count, 11, "11 rows should be inserted")
                self.assertEqual(session.query(self.SourceB).count(), 11)
                self.assertEqual(session.query(self.TargetB).count(), 11)

                results_d = [v.d for v in session.query(self.TargetB).all()]
                results_e = [v.e for v in session.query(self.TargetB).all()]
                self.assertEqual(
                    results_d, list(self.dummy_b[self.TargetB.d.key].values)
                )
                self.assertEqual(
                    results_e, list(self.dummy_b[self.TargetB.e.key].values)
                )


__all__ = [
    "DataFrameBatcherTests",
    "DataFrameBatcherPostgresTests",
]
