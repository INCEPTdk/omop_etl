"""Module for database utilities and helpers"""

import json
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from tempfile import NamedTemporaryFile
from typing import Any, Generator, Iterable, List, Literal, Optional

import pandas as pd
from sqlalchemy import JSON, create_engine, inspect
from sqlalchemy.engine import Engine, ScalarResult
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Query, sessionmaker

from etl.util.logger import setup_logger

from .connection import ConnectionDetails
from .exceptions import DependencyNotFoundException

logger = setup_logger("DEBUG")


class AbstractSession(ABC):
    """A simple interface for interacting with a DB (session)"""

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass

    @abstractmethod
    def add(self, obj: Any, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def query(self, *entities, **kwargs) -> Query:
        pass

    @abstractmethod
    def scalars(self, *entities, **kwargs) -> ScalarResult:
        pass

    @abstractmethod
    def execute(self, sql: Any, *args, **kwargs):
        pass


class Session(AbstractSession):
    """We wrap the sqlalchemy session in an interface we own, so we can easily fake/mock it"""

    def __init__(self, engine: Engine, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._session = sessionmaker(bind=engine)()

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def close(self):
        self._session.close()

    def add(self, obj: Any, **kwargs):
        self._session.add(obj, **kwargs)

    def connection(self) -> Any:
        return self._session.connection()

    def query(self, *entities, **kwargs) -> Query:
        return self._session.query(*entities, **kwargs)

    def scalars(self, *entities, **kwargs) -> ScalarResult:
        return self._session.scalars(*entities, **kwargs)

    def execute(self, sql: Any, *args, **kwargs):
        return self._session.execute(sql, *args, **kwargs)

    def connection_execute(self, sql: str, *args, **kwargs):
        return self._session.connection().connection.execute(
            sql, *args, **kwargs
        )


class FakeSession(AbstractSession):
    """A simple fake session for testing without having a real DB session"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._commits = 0
        self.objects = []
        self._sqllog = []

    def execute(self, sql: Any, *args, **kwargs) -> None:
        self._sqllog.append(sql)

    def get_sql_log(self) -> List[str]:
        return self._sqllog

    def __enter__(self) -> None:
        return self

    def __exit__(self, *args, **kwargs) -> None:
        pass

    def commit(self):
        self._commits += 1

    def rollback(self):
        self._commits -= 1

    def close(self):
        pass

    def add(self, obj: Any, **kwargs):
        # TO-DO: implements
        self.objects.append(obj)

    def query(self, *entities, **kwargs) -> Query:
        return Query(entities, session=None)

    def scalars(self, *entities, **kwargs) -> None:
        pass


def make_db_session(engine: Engine) -> Session:
    return Session(engine)


def make_fake_session() -> FakeSession:
    # pylint: disable=(abstract-class-instantiated)
    return FakeSession()


@contextmanager
def session_context(
    session: AbstractSession,
) -> Generator[AbstractSession, None, None]:
    """Context manager for using a database session,
    given a specific engine implementation"""
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def is_db_connected(engine: Engine) -> bool:
    """helper to check if we can connect to the DB"""
    if engine is None:
        return False
    try:
        engine.connect()
        return True
    except OperationalError:
        return False


def _create_engine_postgres(
    username: Optional[str] = "postgres",
    password: Optional[str] = "",
    host: Optional[str] = "localhost",
    dbname: Optional[str] = "postgres",
    port: Optional[int] = 5432,
    schema: Optional[str] = None,
    **kwargs,
) -> Engine:
    """Create a Postgres database engine based on connection details"""
    url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    connect_args = {}
    if schema is not None:
        connect_args = {"options": f"-csearch_path={schema}"}
    return create_engine(url, connect_args=connect_args, **kwargs)


def make_engine_postgres(connection: ConnectionDetails, **kwargs) -> Engine:
    """Checks if postgres is installed and creates an engine"""
    try:
        # pylint: disable=import-outside-toplevel
        import psycopg2 as _  # noqa: F401

        return _create_engine_postgres(
            username=connection.user,
            password=connection.password,
            host=connection.host,
            dbname=connection.dbname,
            port=connection.port,
            schema=connection.schema,
            **kwargs,
        )
    except ModuleNotFoundError as excep:
        raise DependencyNotFoundException(
            "psycopg2 is needed for a Postgres DBMS! Please install it!"
        ) from excep


def _create_engine_duckdb(
    dbname: Optional[str] = ":memory:",
    **kwargs,
) -> Engine:
    """Create a Duckdb database engine based on connection details"""
    url = f"duckdb:///{dbname}"
    return create_engine(url, connect_args={"config": kwargs})


def make_engine_duckdb(connection: ConnectionDetails, **kwargs) -> Engine:
    """Check if can create an engine with duckdb-engine"""
    try:
        return _create_engine_duckdb(
            dbname=connection.dbname,
            **kwargs,
        )
    except ModuleNotFoundError as excep:
        raise DependencyNotFoundException(
            "duckdb is needed for a DuckDB DBMS! Please install it!"
        ) from excep


def get_environment_variable(
    environment_variable_name: str = None, default: str = None
) -> str:
    schema_name: str = os.getenv(environment_variable_name, default=None)
    if not schema_name:
        logger.debug(
            "Environment variable %s not set, defaults to '%s'",
            environment_variable_name,
            default,
        )
        schema_name = default
    return schema_name


class WriteMode(Enum):
    """Enum for write modes"""

    APPEND = 1
    OVERWRITE = 2


class DataBaseWriter:
    """Use this to write pandas dataframes to a database session"""

    def __init__(self) -> None:
        self._source = None
        self._model = None

        # configuration options
        self.encoding: str = "utf-8"
        self.header: bool = True
        self.delimiter: str = ";"
        self.null_field: str = None
        self.write_mode: Literal[WriteMode.APPEND, WriteMode.OVERWRITE] = (
            WriteMode.OVERWRITE
        )
        self.read_buffer_size: int = 8192
        self.write_buffer_size: int = 268435500

    def _build_options_str(self) -> str:
        quote = '"'
        options = [
            "FORMAT CSV",
            f"DELIMITER E'{self.delimiter}'",
            "HEADER FALSE",
            f"QUOTE E'{quote}'",
        ]

        if self.null_field is not None:
            options.append(f"NULL '{self.null_field}'")
        options_str = ", ".join(options)

        return options_str

    def _initialise_target(self, session: Any, table: str) -> None:
        if self.write_mode == WriteMode.OVERWRITE:
            session.execute(f"DELETE FROM {table};")

    # pylint: disable=unused-variable
    def _do_insert(
        self,
        session: AbstractSession,
        file_path,
        table: str,
        columns: Iterable[str],
    ) -> None:
        columns = ", ".join(c for c in columns)
        self._initialise_target(session, table)
        session.execute(
            f"INSERT INTO {table} ({columns}) SELECT {columns} FROM read_csv('{file_path}');"
        )

    def _do_read(self, file_path: Any, columns: Iterable[str]) -> None:
        self._source[columns].to_csv(
            file_path,
            sep=self.delimiter,
            header=self.header,
            index=False,
            encoding=self.encoding,
        )

    def set_source(self, model: Any, source: pd.DataFrame) -> "DataBaseWriter":
        """
        Set the source dataframe.

        Will copy the dataframe to avoid mutating the source.
        Could be a performance issue if using repeatedly.

        TO-DO: Check the cost of this copy.
        """
        self._source = source.copy()
        self._model = model
        return self

    def _process_json_fields(self) -> None:
        """
        We need to convert json fields to strings
        when writing to the database.
        """
        if self._model is not None:
            for col in self._model.__table__.columns.values():
                if isinstance(col.type, JSON):
                    self._source[col.key] = self._source[col.key].apply(
                        lambda x: json.dumps(x) if x is not None else None
                    )

    def write(
        self,
        session: AbstractSession,
        columns: Optional[Iterable[str]] = None,
    ) -> None:
        df_columns = self._source.columns if columns is None else columns
        with NamedTemporaryFile(
            suffix=".csv",
            mode="w+b",
            delete=True,
        ) as csv_file:
            if self._source is None or self._model is None:
                raise RuntimeError("No source dataframe set!")

            self._process_json_fields()
            self._do_read(csv_file.name, df_columns)
            self._do_insert(
                session, csv_file.name, str(self._model.__table__), df_columns
            )


class DataBaseWriterBuilder:
    """Use this to build a DataBaseWriter"""

    def __init__(
        self, writer_cls: Optional[DataBaseWriter] = DataBaseWriter
    ) -> None:
        self._writer = writer_cls()

    def build(self) -> DataBaseWriter:
        """
        Construct the writer object and validate it.
        """
        if not self._validate():
            raise RuntimeError("Invalid writer configuration")
        return self._writer

    def set_encoding(self, encoding: str) -> "DataBaseWriterBuilder":
        """Enable/disble the header when reading the CSV file"""
        self._writer.encoding = encoding
        return self

    def set_header(self, enable: bool) -> "DataBaseWriterBuilder":
        """Enable/disble the header in the CSV file"""
        self._writer.header = enable
        return self

    def set_delimiter(self, delimiter: str) -> "DataBaseWriterBuilder":
        """Set the delimiter for the writer"""
        self._writer.delimiter = delimiter
        return self

    def set_null_field(self, null_field: str) -> "DataBaseWriterBuilder":
        """Set the null field when writing to the database"""
        self._writer.null_field = null_field
        return self

    def set_write_mode(
        self, mode: Literal[WriteMode.APPEND, WriteMode.OVERWRITE]
    ) -> "DataBaseWriterBuilder":
        """Set the null field when writing to the database"""
        self._writer.write_mode = mode
        return self

    @staticmethod
    def _validate() -> bool:
        """
        Validate the writer options.

        TO-DO: implemnent validation
        """
        return True


def check_table_exists(
    engine: Engine, tablename: str, schema: Optional[str] = None
) -> bool:
    """Helper to check if a table exists"""
    return inspect(engine).has_table(tablename, schema=schema)


# pylint: disable=too-many-arguments
def df_to_sql(
    session: AbstractSession,
    table: str,
    dataframe: pd.DataFrame,
    columns: Optional[List[str]] = None,
):
    """
    Helper function to quickly copy a Pandas DataFrame to an
    existing table in the database. All rows in the table are
    deleted before the copy.
    NOTE: This function should just to an insert but for some reason with duckdb
    it does not work. So we drop and recreate. The proper solution would be:
        session.execute(f"INSERT INTO {table} ({columns}) select {columns} from dataframe;")
    """

    columns = ", ".join(columns) if columns else "*"
    session.execute(f"drop table if exists {table}")
    if table == "lookups.concept_lookup":
        dataframe = dataframe.reset_index()
        session.execute(
            f"create table {table} as select index as lookup_id, {columns} from dataframe;"
        )
    else:
        session.execute(
            f"create table {table} as select {columns} from dataframe;"
        )


def get_source_cdm_schemas(
    session: AbstractSession,
) -> List[str]:
    """
    These functions reads all the schema in the database
    excludes the schemas that are not structured as a cdm schema and returns the list of cdm schemas
    """

    query = """SELECT table_schema
    FROM information_schema.tables
    WHERE table_name IN ('person', 'death', 'measurement', 'location', 'care_site')
    GROUP BY table_schema
    HAVING COUNT(table_name) = 5;
    """
    result = session.execute(query)
    TARGET_SCHEMA = get_environment_variable("TARGET_SCHEMA", "omopcdm")
    return [row[0] for row in result if row[0] != TARGET_SCHEMA]
