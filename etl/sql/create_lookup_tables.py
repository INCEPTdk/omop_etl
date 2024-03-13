"""Create the lookup tables"""

from typing import Final, List

from ..models.modelutils import (
    DIALECT_POSTGRES,
    create_tables_sql,
    drop_tables_sql,
)
from ..models.tempmodels import LOOKUPS_SCHEMA, ConceptLookup, ConceptLookupStem
from ..util.sql import clean_sql

MODELS: Final[List] = [
    ConceptLookup,
    ConceptLookupStem,
]

SQL_CREATE_SCHEMA: Final[str] = f"CREATE SCHEMA IF NOT EXISTS {LOOKUPS_SCHEMA};"


@clean_sql
def _ddl_sql() -> str:
    statements = [
        SQL_CREATE_SCHEMA,
        drop_tables_sql(MODELS, cascade=True),
        create_tables_sql(MODELS, dialect=DIALECT_POSTGRES),
    ]
    return " ".join(statements)


SQL: Final[str] = _ddl_sql()