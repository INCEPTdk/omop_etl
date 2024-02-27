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
    String,
    Text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import declarative_mixin, declarative_base
from sqlalchemy.schema import (
    AddConstraint,
    CreateIndex,
    CreateTable,
    DropConstraint,
    DropIndex,
)

from ..util.exceptions import FrozenClassException
from ..util.sql import clean_sql

DIALECT_POSTGRES: Final = postgresql.dialect()

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


def rebase_model(klass: Any, new_base: Any) -> Any:
    """
    Use to change the metadata i.e. the schema.

    Usage example (remove the schema from Person model):
    ```
        ModelBaseWithoutSchema = make_model_base()
        _ = rebase_model(Person, ModelBaseWithoutSchema)
    ```
    """
    new_dict = {
        k: v
        for k, v in klass.__dict__.items()
        if not k.startswith("_") or k in {"__tablename__", "__table_args__"}
    }

    # klass.__table__.metadata.clear()
    # Associate the new table with the new metadata instead
    # of the old/other pool
    new_dict["__table__"] = klass.__table__.to_metadata(
        new_base.metadata, schema=new_base.metadata.schema
    )
    new_dict["schema"] = new_base.metadata.schema
    klass.__table__.schema = new_base.metadata.schema

    # Construct and return a new type
    return type(klass.__name__, (new_base,), new_dict)


def rebase_all(old_base: Any, new_base: Any) -> None:
    """
    Use to change the metadata i.e. the schema.

    Usage example (remove the schema from Person model):
    ```
        ModelBaseWithoutSchema = make_model_base()
        rebase_all(ModelBase, ModelBaseWithoutSchema)
    ```
    """
    for mapper in old_base.registry.mappers:
        cls = mapper.class_
        classname = cls.__name__
        if not classname.startswith("_"):
            _ = rebase_model(cls, new_base)


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

    _id = IntField(primary_key=True)


ModelBase: Final[Any] = make_model_base()


@clean_sql
def set_default_schema_sql(schema: str) -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    SET search_path TO {schema};"""


@clean_sql
def truncate_tables_sql(models: List[ModelBase], cascade=True) -> str:
    cascade_str = ""
    if cascade:
        cascade_str = "CASCADE"
    drop_sql = (
        "; ".join(
            [f"TRUNCATE TABLE {str(m.__table__)} {cascade_str}" for m in models]
        )
        + ";"
    )
    return drop_sql


@clean_sql
def load_from_csv(
    models: List[ModelBase], base_path: str = ".", delimiter: str = ";"
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
