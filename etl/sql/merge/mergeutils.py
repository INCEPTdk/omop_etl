""" A collection of utilities for merging different ETl databases. """

from typing import List, Optional, Tuple, Union

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

    cdm_columns = move_to_end(
        cdm_columns, VisitOccurrence.care_site_id.key
    )  # pylint: disable=no-member
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


@clean_sql
def remap_person_id(
    schema: str, cdm_table: OmopCdmModelBase, person_table: OmopCdmModelBase
):
    """Remap Person IDs in a CDM table."""
    return f"""SELECT DISTINCT site_cdm_table.person_id AS site_person_id,
                        merge_person.person_id AS merge_person_id
            FROM {schema}.{cdm_table.__tablename__} AS site_cdm_table
            INNER JOIN {schema}.{person_table.__tablename__} AS site_person
            ON site_cdm_table.person_id = site_person.person_id
            INNER JOIN {person_table.__table__} AS merge_person
            ON site_person.person_source_value = merge_person.person_source_value"""


@clean_sql
def remap_care_site_id(schema: str, care_site_table: OmopCdmModelBase):
    """Remap Care Site IDs in a CDM table."""
    return f"""SELECT DISTINCT merge_care_site.care_site_id as merge_care_site_id,
                    site_care_site.care_site_id as site_care_site_id
                FROM {schema}.{care_site_table.__tablename__} AS site_care_site
                INNER JOIN {care_site_table.__table__} AS merge_care_site
                ON site_care_site.care_site_source_value = merge_care_site.care_site_source_value"""


def build_aggregate_sql(
    agg_sum_columns: Union[str, List[str]]
) -> Tuple[str, str]:
    if not agg_sum_columns:
        select_stmt = ""
        insert_stmt = ""
    else:
        if isinstance(agg_sum_columns, str):
            agg_sum_columns = [agg_sum_columns]

        select_stmt = "," + ", ".join(
            [f"SUM(d.{col}) AS {col}" for col in agg_sum_columns]
        )
        insert_stmt = "," + ", ".join(agg_sum_columns)
    return insert_stmt, select_stmt


@clean_sql
def concatenate_overlapping_intervals(
    cdm_table: OmopCdmModelBase,
    key_columns: List[str],
    start_date_column: str,
    end_date_column: str,
    agg_sum_columns: Optional[Union[str, List[str]]] = "",
) -> str:
    """
    SQL code to concatenate overlapping intervals in observation periods.
    """
    key_cols = ", ".join(key_columns)

    join_condition_key_cols = " AND ".join(
        [f"i.{col} = d.{col}" for col in key_columns]
    )
    join_condition_start_date = (
        f"i.{start_date_column} <= d.{start_date_column}"
    )
    join_condition_end_date = f"i.{end_date_column} >= d.{end_date_column}"
    group_by_cols = ", ".join(
        [
            f"i.{c}"
            for c in key_columns + [start_date_column] + [end_date_column]
        ]
    )

    agg_sum_cols_insert, agg_sum_cols_select = build_aggregate_sql(
        agg_sum_columns
    )

    return f"""
    CREATE TEMP TABLE {cdm_table.__tablename__}_tmp AS SELECT * FROM {cdm_table.__table__};
    TRUNCATE TABLE {cdm_table.__table__};
    WITH
    weighted_endpoints AS (
        SELECT
            {key_cols},
            a,
            Sum(d) AS d
        FROM
            (
            SELECT
                {key_cols},
                {start_date_column} AS a,
                1 AS d
            FROM
            {cdm_table.__tablename__}_tmp
        UNION ALL
            SELECT
                {key_cols},
                {end_date_column} AS a,
                -1 AS d
            FROM
            {cdm_table.__tablename__}_tmp
            ) e
        GROUP BY
            {key_cols},
            a),
    endpoints_with_coverage AS (
        SELECT
            *,
            Sum(d) OVER (
            ORDER BY {key_cols},
            a) - d AS c
        FROM
            weighted_endpoints),
    equivalence_classes AS (
        SELECT
            *,
            COUNT(CASE WHEN c = 0 THEN 1 END) OVER (
            ORDER BY {key_cols},
            a) AS class
        FROM
            endpoints_with_coverage),
    final_intervals AS (
        SELECT {key_cols},
               min(a) AS {start_date_column},
               max(a) AS {end_date_column},
        FROM equivalence_classes
        GROUP BY {key_cols}, class
    )
    INSERT INTO {cdm_table.__table__} (
        {key_cols},
        {start_date_column},
        {end_date_column}
        {agg_sum_cols_insert}
    ) SELECT DISTINCT i.* {agg_sum_cols_select}
    FROM final_intervals i
    INNER JOIN {cdm_table.__tablename__}_tmp d
    ON {join_condition_key_cols} AND {join_condition_start_date} AND {join_condition_end_date}
    GROUP BY {group_by_cols};
"""
