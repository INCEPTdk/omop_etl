import logging
import os
import unittest
from pathlib import Path
from typing import Any, Callable, Final, List

import pandas as pd

from etl.loader import CSVFileLoader, Loader
from etl.models.modelutils import (
    CharField,
    FloatField,
    IntField,
    PKIdMixin,
    make_model_base,
)
from etl.models.source import SOURCE_MODELS
from etl.models.tempmodels import TEMP_MODELS
from etl.process import TransformationRegistry, run_etl, run_transformations
from etl.transform.session_operation import SessionOperation
from etl.util.db import (
    DataBaseWriterBuilder,
    FakeSession,
    Session,
    make_db_session,
    make_fake_session,
    session_context,
)
from etl.util.exceptions import (
    ETLFatalErrorException,
    TransformationErrorException,
)
from etl.util.random import generate_dummy_data
from tests.testutils import DuckDBBaseTest


class ProcessUnitTests(unittest.TestCase):

    TestModelBase: Final[Any] = make_model_base(schema="dummy")

    class DummyTable(TestModelBase, PKIdMixin):
        __tablename__: Final = "dummy"
        __table_args__ = {"schema": "dummy"}

        a: Final = IntField()
        b: Final = CharField(10)
        c: Final = FloatField()

    class Dummy2Table(TestModelBase, PKIdMixin):
        __tablename__: Final = "dummy2"
        __table_args__ = {"schema": "dummy"}

        x: Final = IntField()
        y: Final = CharField(10)

    def setUp(self) -> None:
        super().setUp()
        self._session = make_fake_session()

        self.dummy_df = pd.DataFrame(
            {
                "_id": [1, 2, 3],
                "a": [1, 4, 6],
                "b": ["test1", "test2", "dhsjak"],
                "c": [435.34, None, 1432213.2],
            }
        )
        self.dummy_df2 = pd.DataFrame(
            {
                "_id": [1, 2],
                "x": [45, 23],
                "y": ["AB", "CD"],
            }
        )

    def test_run_transforms_fake(self):
        def transform1(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session, columns=["a", "c"])

        def transform2(session: FakeSession):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.Dummy2Table, self.dummy_df2)
            )
            writer.write(session, columns=["x"])

        with session_context(self._session) as session:
            self.assertEqual(0, len(session.get_sql_log()))
            run_transformations(
                session,
                transformations=[
                    (0, SessionOperation("test1", session, transform1)),
                    (1, SessionOperation("test2", session, transform2)),
                ],
            )
            # expect 1 delete and 1 copy per table = 4
            self.assertEqual(4, len(session.get_sql_log()))

    def test_run_transformations_with_log(self):
        def transform1(session: FakeSession):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session, columns=["a", "c"])

        def transform2(session: FakeSession):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.Dummy2Table, self.dummy_df2)
            )
            writer.write(session, columns=["x"])

        with session_context(self._session) as session:
            with self.assertLogs("ETL.Core", level=logging.DEBUG) as captured:
                self.assertEqual(len(captured.records), 0)
                run_transformations(
                    session,
                    transformations=[
                        (
                            0,
                            SessionOperation(
                                "test1",
                                session,
                                transform1,
                                description="Test 1",
                            ),
                        ),
                        (1, SessionOperation("test2", session, transform2)),
                    ],
                )

            self.assertEqual(len(captured.records), 4)
            self.assertEqual(
                captured.records[0].getMessage(),
                "Step 1: Performing transformation Test 1",
            )
            # 2nd record is memory tracking
            self.assertEqual(
                captured.records[2].getMessage(),
                "Step 2: Performing transformation test2",
            )

    def test_registry_empty_get(self):
        reg = TransformationRegistry()
        self.assertIsNone(reg.get("abc"))

    def test_registry_empty_add_get(self):
        reg = TransformationRegistry()
        reg.add_or_update("abc", 4)
        self.assertIsNotNone(reg.get("abc"))
        self.assertEqual(reg.get("abc"), 4)

    def test_registry_empty_add_get2(self):
        reg = TransformationRegistry()
        reg.add_or_update("abc", {"1234": 1234})
        self.assertIsNotNone(reg.get("abc"))
        self.assertEqual(reg.get("abc")["1234"], 1234)

    def test_registry_initial_state(self):
        reg = TransformationRegistry(initial_state={"abc": 78})
        self.assertEqual(reg.get("abc"), 78)
        reg.add_or_update("abc", {"1234": 1234})
        self.assertIsNotNone(reg.get("abc"))
        self.assertEqual(reg.get("abc")["1234"], 1234)

    def test_registry_lazy_get(self):
        reg = TransformationRegistry()
        reg.add_or_update("abc", 4)
        self.assertIsNotNone(reg.get("abc"))
        self.assertEqual(reg.get("abc"), 4)

        lg = reg.lazy_get("abc")
        self.assertEqual(lg(), 4)
        reg.add_or_update("abc", 5)
        self.assertEqual(lg(), 5)

    def test_registry_lazy_get_non_empty(self):
        reg = TransformationRegistry(initial_state={"abc": 78})
        lg = reg.lazy_get("abc")
        self.assertEqual(lg(), 78)
        reg.add_or_update("abc", 5)
        self.assertEqual(lg(), 5)

    def test_registry_with_sessions(self):
        input_value = 5

        class DummyOperation(SessionOperation):
            def __init__(dself, *args, initial_value: int, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                dself.initial_value = initial_value

            def _run(dself, *args, **kwargs) -> Any:
                return dself._func(dself.session, dself.initial_value)

        def dummy_trans(session: FakeSession, some_value: int) -> int:
            self.assertEqual(some_value, input_value)
            self.assertTrue(isinstance(session, FakeSession))
            return some_value * 2

        reg = TransformationRegistry()
        op = DummyOperation(
            key="dummy",
            session=self._session,
            func=dummy_trans,
            description="Create dummy transform",
            initial_value=input_value,
        )
        result = op(self._session)
        reg.add_or_update(op.key, result)
        self.assertEqual(reg.get("dummy"), input_value * 2)

    def test_registry_with_sessions_lazy_get(self):
        input_value = 5

        class DummyOperation(SessionOperation):
            def __init__(dself, *args, initial_value: int, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                dself.initial_value = initial_value

            def _run(dself, *args, **kwargs) -> Any:
                return dself._func(dself.session, dself.initial_value)

        def dummy_trans(session: FakeSession, some_value: int) -> int:
            self.assertEqual(some_value, input_value)
            self.assertTrue(isinstance(session, FakeSession))
            return some_value * 2

        reg = TransformationRegistry()
        op = DummyOperation(
            key="dummy",
            session=self._session,
            func=dummy_trans,
            description="Create dummy transform",
            initial_value=input_value,
        )
        result = op(self._session)
        reg.add_or_update(op.key, result)
        lg = reg.lazy_get("dummy")
        self.assertEqual(lg(), input_value * 2)
        reg.add_or_update("dummy", 15)
        self.assertEqual(lg(), 15)

        # run again without update
        result = op(self._session)
        self.assertEqual(lg(), 15)

    def test_registry_with_sessions_deps(self):
        input_value = 5

        class DummyOperation(SessionOperation):
            def __init__(dself, *args, initial_value: int, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                dself.initial_value = initial_value

            def _run(dself, *args, **kwargs) -> Any:
                return dself._func(dself.session, dself.initial_value)

        class PostDummyOperation(SessionOperation):
            def __init__(
                dself,
                *args,
                value_getter: Callable[[str], int],
                **kwargs,
            ) -> None:
                super().__init__(*args, **kwargs)
                dself._value_getter = value_getter

            def _run(dself, *args, **kwargs) -> Any:
                return dself._func(dself.session, dself._value_getter())

        def dummy_trans(session: FakeSession, some_value: int) -> int:
            self.assertEqual(some_value, input_value)
            self.assertTrue(isinstance(session, FakeSession))
            return some_value * 2

        def post_dummy_trans(session: FakeSession, prev_result: int) -> int:
            self.assertEqual(prev_result, input_value * 2)
            self.assertTrue(isinstance(session, FakeSession))
            return prev_result * 3

        reg = TransformationRegistry()
        op = DummyOperation(
            key="dummy",
            session=self._session,
            func=dummy_trans,
            description="Create dummy transform",
            initial_value=input_value,
        )
        op2 = PostDummyOperation(
            key="dummy2",
            session=self._session,
            func=post_dummy_trans,
            description="Create post dummy transform",
            value_getter=reg.lazy_get("dummy"),
        )
        result = op(self._session)
        reg.add_or_update(op.key, result)
        # must be added after update
        result2 = op2(self._session)
        self.assertEqual(input_value * 2 * 3, result2)

    def test_run_transformations_with_registry(self):
        class DummyOperation(SessionOperation):
            def __init__(dself, *args, initial_value: int, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                dself.initial_value = initial_value

            def _run(dself, *args, **kwargs) -> Any:
                return dself._func(dself.session, dself.initial_value)

        class PostDummyOperation(SessionOperation):
            def __init__(
                dself,
                *args,
                value_getter: Callable[[str], int],
                **kwargs,
            ) -> None:
                super().__init__(*args, **kwargs)
                dself._value_getter = value_getter

            def _run(dself, *args, **kwargs) -> Any:
                return dself._func(dself.session, dself._value_getter())

        reg = TransformationRegistry()
        transformations = [
            (
                0,
                DummyOperation(
                    key="dummy",
                    session=self._session,
                    func=lambda _, x: x * 2,
                    initial_value=5,
                ),
            ),
            # this takes the value from dummy to calculate post_dummy
            (
                1,
                PostDummyOperation(
                    key="post_dummy",
                    session=self._session,
                    func=lambda _, x: x * 3,
                    value_getter=reg.lazy_get("dummy"),
                ),
            ),
        ]

        dlg = reg.lazy_get("dummy")
        dlg2 = reg.lazy_get("post_dummy")

        self.assertIsNone(reg.get("dummy"))
        self.assertIsNone(reg.get("post_dummy"))
        self.assertIsNone(dlg())
        self.assertIsNone(dlg2())

        run_transformations(self._session, transformations, reg)

        self.assertEqual(reg.get("dummy"), 10)
        self.assertEqual(reg.get("post_dummy"), 10 * 3)
        self.assertEqual(dlg(), 10)
        self.assertEqual(dlg2(), 10 * 3)


class ProcessDuckDBTests(DuckDBBaseTest):
    TestModelBase: Final[Any] = make_model_base(schema="dummy")

    class DummyTable(TestModelBase, PKIdMixin):
        __tablename__: Final = "dummy_table"
        __table_args__ = {"schema": "dummy"}

        a: Final = IntField()
        b: Final = CharField(10)
        c: Final = FloatField()

    class DummyTableTwo(TestModelBase, PKIdMixin):
        __tablename__: Final = "dummy_table2"
        __table_args__ = {"schema": "dummy"}

        x: Final = IntField()
        y: Final = CharField(5)

    def setUp(self):
        super().setUp()
        self._session = make_db_session(self.engine)
        self._create_tables_and_schemas(
            models=[self.DummyTable, self.DummyTableTwo]
        )

        self.dummy_df = pd.DataFrame(
            {
                "_id": [1, 2, 3],
                "a": [1, 4, 6],
                "b": ["test1", "test2", "dhsjak"],
                "c": [435.34, None, 1432213.2],
            }
        )
        self.dummy_df2 = pd.DataFrame(
            {
                "_id": [1, 2],
                "x": [45, 23],
                "y": ["AB", "CD"],
            }
        )

    def tearDown(self) -> None:
        self._drop_tables_and_schemas(
            models=[self.DummyTable, self.DummyTableTwo]
        )
        super().tearDown()

    def test_run_transforms(self):
        def transform1(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session)

        def transform2(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTableTwo, self.dummy_df2)
            )
            writer.write(session)

        with session_context(self._session) as session:
            self.assertEqual(
                0, session.query(self.DummyTable).count(), "assert dummy table"
            )
            self.assertEqual(
                0,
                session.query(self.DummyTableTwo).count(),
                "assert dummy table 2",
            )
            run_transformations(
                session,
                transformations=[
                    (1, SessionOperation("test1", session, transform1)),
                    (2, SessionOperation("test2", session, transform2)),
                ],
            )
            self.assertEqual(
                3, session.query(self.DummyTable).count(), "assert dummy table"
            )
            self.assertEqual(
                2,
                session.query(self.DummyTableTwo).count(),
                "assert dummy table two",
            )

    def test_run_transformations_with_log(self):
        def transform1(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session, columns=self.dummy_df.columns)

        def transform2(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTableTwo, self.dummy_df2)
            )
            writer.write(session, columns=self.dummy_df2.columns)

        with session_context(self._session) as session:
            with self.assertLogs("ETL.Core", level=logging.DEBUG) as captured:
                self.assertEqual(len(captured.records), 0)
                run_transformations(
                    session,
                    transformations=[
                        (0, SessionOperation("test1", session, transform1)),
                        (1, SessionOperation("test2", session, transform2)),
                    ],
                )
            self.assertEqual(len(captured.records), 4)
            self.assertEqual(
                captured.records[0].getMessage(),
                "Step 1: Performing transformation test1",
            )
            # 2nd record is memory tracking
            self.assertEqual(
                captured.records[2].getMessage(),
                "Step 2: Performing transformation test2",
            )

    def test_run_transformations_with_session_error_check_log(self):
        trans_called = {1: False, 2: False, 3: False}

        def transform1(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session, columns=self.dummy_df.columns)
            trans_called[1] = True

        def transform2(session: Session):
            trans_called[2] = True
            raise TransformationErrorException(
                "Transform 2 throws a transformation error!"
            )

        def transform3(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTableTwo, self.dummy_df2)
            )
            writer.write(session, columns=self.dummy_df2.columns)
            trans_called[3] = True

        with session_context(self._session) as session:
            with self.assertLogs(
                "ETL.Core", level=logging.CRITICAL
            ) as captured:
                self.assertEqual(len(captured.records), 0)
                run_transformations(
                    session,
                    transformations=[
                        (0, SessionOperation("test1", session, transform1)),
                        (1, SessionOperation("test2", session, transform2)),
                        (2, SessionOperation("test3", session, transform3)),
                    ],
                )
            self.assertEqual(len(captured.records), 1)
            self.assertEqual(
                captured.records[0].getMessage(),
                "Transform 2 throws a transformation error!",
            )
        for _, v in trans_called.items():
            self.assertTrue(v)

    def test_run_transformations_with_session_error2_check_log(self):
        trans_called = {1: False, 2: False, 3: False}

        def transform1(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session, columns=self.dummy_df.columns)
            trans_called[1] = True

        def transform2(session: Session):
            trans_called[2] = True
            # not a transformation error
            raise RuntimeError("Transform 2 throws a runtime error!")

        def transform3(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTableTwo, self.dummy_df2)
            )
            writer.write(session, columns=self.dummy_df2.columns)
            trans_called[3] = True

        with session_context(self._session) as session:
            with self.assertLogs(
                "ETL.Core", level=logging.CRITICAL
            ) as captured:
                self.assertEqual(len(captured.records), 0)
                run_transformations(
                    session,
                    transformations=[
                        (0, SessionOperation("test1", session, transform1)),
                        (1, SessionOperation("test2", session, transform2)),
                        (2, SessionOperation("test3", session, transform3)),
                    ],
                )
            self.assertEqual(len(captured.records), 1)
            self.assertEqual(
                captured.records[0].getMessage(),
                "Transform 2 throws a runtime error!",
            )
        for _, v in trans_called.items():
            self.assertTrue(v)

    def test_run_transformations_with_session_error_check_db(self):
        trans_called = {1: False, 2: False, 3: False}

        def transform1(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTable, self.dummy_df)
            )
            writer.write(session, columns=self.dummy_df.columns)
            trans_called[1] = True

        def transform2(session: Session):
            trans_called[2] = True
            raise TransformationErrorException(
                "Transform 2 throws a transformation error!"
            )

        def transform3(session: Session):
            writer = (
                DataBaseWriterBuilder()
                .build()
                .set_source(self.DummyTableTwo, self.dummy_df2)
            )
            writer.write(session, columns=self.dummy_df2.columns)
            trans_called[3] = True

        for _, v in trans_called.items():
            self.assertFalse(v)

        called = False
        with self.assertRaises(ETLFatalErrorException):
            with session_context(self._session) as session:
                self.assertEqual(
                    0,
                    session.query(self.DummyTable).count(),
                    "assert dummy table",
                )
                self.assertEqual(
                    0,
                    session.query(self.DummyTableTwo).count(),
                    "assert dummy table 2",
                )
                run_transformations(
                    session,
                    transformations=[
                        (1, SessionOperation("test1", session, transform1)),
                        (2, SessionOperation("test2", session, transform2)),
                        (3, SessionOperation("test3", session, transform3)),
                    ],
                )

        for _, v in trans_called.items():
            self.assertTrue(v)

        called = False
        with session_context(self._session) as session:
            self.assertEqual(
                0,
                session.query(self.DummyTable).count(),
                "assert dummy table",
            )
            self.assertEqual(
                0,
                session.query(self.DummyTableTwo).count(),
                "assert dummy table two",
            )
            called = True
        self.assertTrue(called)


class RunETLDuckDBTests(DuckDBBaseTest):
    nentries: int = 1000
    csv_dir = os.path.join(
        Path(__file__).parent.parent.absolute(), "etl", "csv"
    )

    class RandomSourceLoader(Loader):
        """A fake source loader for testing"""

        def __init__(
            self,
            models: List[Any],
        ) -> None:
            super().__init__()
            self.models = models

        def load(self) -> Loader:
            self.reset()

            for model in self.models:
                dd = []
                for _ in range(RunETLPostgresTests.nentries):
                    dd.append(generate_dummy_data(model))
                self._update(model.__tablename__, pd.DataFrame(dd))
            return self


__all__ = ["ProcessUnitTests", "ProcessDuckDBTests", "RunETLDuckDBTests"]
