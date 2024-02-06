"""A module for sql helpers"""
from typing import Any


def clean_sql(func):
    """Decorator for cleaning sql statements generated in functions"""

    def decorated(*args, **kwargs) -> str:
        result = func(*args, **kwargs)
        cleaned_sql = " ".join(
            [s for s in result.replace("\n", " ").split(" ") if s != ""]
        )
        return cleaned_sql.strip()

    return decorated


def generate_id_sql() -> str:
    """Standard way to generate a unique key by row count"""
    return "ROW_NUMBER() OVER (ORDER BY (NULL))"


@clean_sql
def set_default_schema_sql(schema: str) -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    SET search_path TO {schema};"""


@clean_sql
def create_enum_sql(enum_type: Any) -> str:
    name_str = ",".join([f"'{e.name}'" for e in enum_type])
    return f"""
    DO $$ BEGIN
        CREATE TYPE {enum_type.__name__} AS ENUM ({name_str});
    EXCEPTION
        WHEN duplicate_object THEN null;
    END $$;
    """
