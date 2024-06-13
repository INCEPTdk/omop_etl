""" A collection of utilities for merging different ETl databases. """

from typing import List

from etl.models.omopcdm54.clinical import Person, VisitOccurrence
from etl.models.omopcdm54.health_systems import CareSite
from etl.models.omopcdm54.registry import OmopCdmModelBase
from etl.util.db import AbstractSession, get_source_cdm_schemas
from etl.util.sql import clean_sql


def move_to_end(lst, elem):
    """
    Moves an element to the end of the list if it exists in the list.
    """
    if elem in lst:
        lst.append(lst.pop(lst.index(elem)))
    return lst


@clean_sql
def _sql_merge_cdm_table(
    schemas: List[str],
    cdm_table: OmopCdmModelBase,
    cdm_columns: List[str] = None,
):
    """Generate SQL for merge (union) a CDM table based on a list of columns."""

    if not cdm_columns:
        cdm_columns = [
            c.key
            for c in cdm_table.__table__.columns
            if c.key not in cdm_table.__table__.primary_key.columns
        ]

    cdm_columns = move_to_end(cdm_columns, VisitOccurrence.care_site_id.key)
    cdm_columns = move_to_end(cdm_columns, Person.person_id.key)

    select_statements = []
    for schema in schemas:
        selects = ", ".join(
            [c for c in cdm_columns if c != Person.person_id.key]
        )
        inner_joins = ""
        if (
            Person.person_id.key in cdm_columns
            and cdm_table.__tablename__ != Person.__tablename__
        ):
            selects += ", person_mapping.merge_person_id as person_id"
            inner_joins += f""" INNER JOIN ({remap_person_id(schema, cdm_table, Person)}) AS person_mapping
            ON source.person_id = person_mapping.site_person_id"""

        if cdm_table.__tablename__ == VisitOccurrence.__tablename__:
            selects.replace(
                " care_site_id,",
                " care_site_mapping.merge_care_site_id as care_site_id, ",
            )
            inner_joins += f""" INNER JOIN ({remap_care_site_id(schema, CareSite)}) AS care_site_mapping
            ON source.care_site_id = care_site_mapping.site_care_site_id"""

        statement = f""" SELECT {selects}
            FROM {schema}.{cdm_table.__tablename__} as source
            {inner_joins}
        """

        select_statements.append(statement)

    return f"""INSERT INTO {cdm_table.__table__}
    ({', '.join(cdm_columns)})
    {' UNION ALL '.join(select_statements)};"""


def merge_cdm_table(
    session: AbstractSession,
    cdm_table: OmopCdmModelBase,
    cdm_columns: List[str] = None,
) -> None:
    """Merge (union) a CDM table based on a list of columns."""
    schemas = get_source_cdm_schemas(session)
    merge_sql = _sql_merge_cdm_table(schemas, cdm_table, cdm_columns)
    session.execute(merge_sql)


@clean_sql
def drop_duplicated_rows(
    cdm_table: OmopCdmModelBase, cdm_column: str, id_column: str
) -> str:
    """Remove duplicate rows from a CDM table based on a column."""

    return f"""WITH cte as (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY {cdm_column}) AS rn
        FROM {cdm_table.__table__}
    )
    DELETE FROM {cdm_table.__table__}
    WHERE {id_column} IN (
    SELECT {id_column}
    FROM cte
    WHERE rn > 1);"""

