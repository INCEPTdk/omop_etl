"""Utilities specific for the data models"""

# pylint: disable=invalid-name
from typing import Any, Callable, Final, List, Optional

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import declarative_base, declarative_mixin
from sqlalchemy.schema import (
    AddConstraint,
    CreateIndex,
    CreateSequence,
    CreateTable,
    DropConstraint,
    DropIndex,
)

from ..util.exceptions import FrozenClassException
from ..util.sql import clean_sql

DIALECT_POSTGRES: Final = postgresql.dialect()


def create_int_pk_column(sequence_id: str) -> Column:
    return Column(
        Integer,
        Sequence(sequence_id),
        primary_key=True,
        server_default=Sequence(sequence_id).next_value(),
    )


def create_char_pk_column(x: int, sequence_id: str) -> Column:
    seq = Sequence(sequence_id)
    return Column(
        String(x), seq, primary_key=True, server_default=seq.next_value()
    )


ConstraintPK: Final = PrimaryKeyConstraint
# A simple alias for sqlalchemy's ForiegnKey
FK: Final[ForeignKey] = ForeignKey
CharField: Final[Callable[[Any], Column]] = lambda x, *args, **kwargs: Column(
    String(x), *args, **kwargs
)
DateField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    Date, *args, **kwargs
)
DateTimeField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    DateTime, *args, **kwargs
)
EnumField: Final[Callable[[Any], Column]] = lambda x, *args, **kwargs: Column(
    Enum(x), *args, **kwargs
)
IntField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    Integer, *args, **kwargs
)
PKIntField: Final[
    Callable[[Any], Column]
] = lambda sequence_id, *args, **kwargs: create_int_pk_column(sequence_id)
PKCharField: Final[
    Callable[[Any], Column]
] = lambda x, sequence_id, *args, **kwargs: create_char_pk_column(
    x, sequence_id
)

BigIntField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    BigInteger, *args, **kwargs
)
FloatField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    Float, *args, **kwargs
)
NumericField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    Numeric, *args, **kwargs
)
TextField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    Text, *args, **kwargs
)
JSONField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    JSON, *args, **kwargs
)
BoolField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    Boolean, *args, **kwargs
)

TimeStampField: Final[Callable[[Any], Column]] = lambda *args, **kwargs: Column(
    TIMESTAMP, *args, **kwargs
)


class _MetaModel(DeclarativeMeta):
    """
    Meta class to protect us from adding extra fields to our models
    """

    # pylint: disable=no-self-argument
    def __setattr__(cls, name: str, value: Any) -> None:
        if (
            name
            in [
                "_sa_instance_state",
                "_sa_registry",
                "_sa_class_manager",
                "_sa_declared_attr_reg",
                "__table__",
                "__mapper__",
                "__frozen",
                "_id",
            ]
            or hasattr(cls, name)
            or name.startswith('"')
        ):
            super().__setattr__(name, value)
        else:
            raise FrozenClassException(
                f"Model instances do not accept arbitrary attributes: {name}: {value}"
            )


def make_model_base(schema: Optional[str] = None) -> Any:
    """Dynamically create a new model base"""
    return declarative_base(
        metaclass=_MetaModel, metadata=MetaData(schema=schema)
    )


@clean_sql
def drop_tables_sql(models: List[Any], cascade=True) -> str:
    cascade_str = ""
    if cascade:
        cascade_str = "CASCADE"
    drop_sql = (
        "; ".join(
            [
                f"DROP TABLE IF EXISTS {str(m.__table__)} {cascade_str}"
                for m in models
            ]
        )
        + ";"
    )
    return drop_sql


@clean_sql
def create_tables_sql(models: List[Any], dialect=DIALECT_POSTGRES) -> str:
    sql = []
    for model in models:
        sql.append(
            str(
                CreateSequence(
                    Sequence(model.__table__.name + "_id_seq"),
                    if_not_exists=True,
                ).compile(dialect=dialect)
            )
        )
        sql.append(
            str(
                CreateTable(
                    model.__table__,
                    include_foreign_key_constraints=[],
                    if_not_exists=True,
                ).compile(dialect=dialect)
            )
        )
    return "; ".join(sql) + ";"


@clean_sql
def set_indexes_sql(
    models: List[Any], dialect: Optional[Any] = DIALECT_POSTGRES
) -> str:
    sql = []
    for model in models:
        for index in model.__table__.indexes:
            sql.append(
                str(
                    CreateIndex(index, if_not_exists=True).compile(
                        dialect=dialect
                    )
                )
            )
    return "; ".join(sql) + ";"


@clean_sql
def drop_indexes_sql(
    models: List[Any], dialect: Optional[Any] = DIALECT_POSTGRES
) -> str:
    sql = []
    for model in models:
        for index in model.__table__.indexes:
            sql.append(
                str(DropIndex(index, if_exists=True).compile(dialect=dialect))
            )
    return "; ".join(sql) + ";"


@clean_sql
def set_constraints_sql(
    models: List[Any], dialect: Optional[Any] = DIALECT_POSTGRES
) -> str:
    sql = []
    for model in models:
        for constraint in model.__table__.constraints:
            sql.append(
                str(
                    AddConstraint(constraint, if_not_exists=True).compile(
                        dialect=dialect
                    )
                )
            )
    return "; ".join(sql) + ";"


@clean_sql
def drop_constraints_sql(
    models: List[Any], dialect: Optional[Any] = DIALECT_POSTGRES
) -> str:
    sql = []
    for model in models:
        for constraint in model.__table__.constraints:
            sql.append(
                str(
                    DropConstraint(constraint, if_exists=True).compile(
                        dialect=dialect
                    )
                )
            )

    return "; ".join(sql) + ";"


@declarative_mixin
class PKIdMixin:
    """A mixin"""

    _id = Column(
        "_id",
        Integer,
        Sequence("_id", start=1),
        primary_key=True,
    )


ModelBase: Final[Any] = make_model_base()


@clean_sql
def set_default_schema_sql(schema: str) -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    SET search_path TO {schema};"""


@clean_sql
def load_from_csv(
    models: List[ModelBase], base_path: str = ".", delimiter: str = ";"  # type: ignore
) -> str:
    sql = []
    for model in models:
        cols_str = ",".join([v.key for v in model.__table__.columns.values()])
        sql.append(
            f"""
COPY {model.__tablename__} ({cols_str})
FROM '{base_path}/{model.__tablename__}.csv'
DELIMITER '{delimiter}'
NULL 'NULL';
"""
        )
    return "; ".join(sql) + ";"
