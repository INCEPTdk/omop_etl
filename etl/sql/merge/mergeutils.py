""" A collection of utilities for merging different ETl databases. """

from typing import List, Optional, Tuple, Union

from etl.models.omopcdm54.clinical import Person, VisitOccurrence
from etl.models.omopcdm54.health_systems import CareSite
from etl.models.omopcdm54.registry import OmopCdmModelBase
from etl.util.db import AbstractSession, get_source_cdm_schemas
from etl.util.sql import clean_sql


def move_to_end(source_lst, elements):
    """
    Moves an element to the end of the list if it exists in the list.
    """
    lst = source_lst.copy()
    for elem in elements:
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
    # pylint: disable=no-member
    cdm_columns = move_to_end(
        cdm_columns, [VisitOccurrence.care_site_id.key, Person.person_id.key]
    )

    query_single_table = []
    for schema in schemas:
        selected_cols = ", ".join(
            [c for c in cdm_columns if c != Person.person_id.key]
        )
        inner_joins = ""
        if (
            Person.person_id.key in cdm_columns
            and cdm_table.__tablename__ != Person.__tablename__
        ):
            selected_cols += ", person_mapping.merge_person_id as person_id"
            inner_joins += f""" INNER JOIN ({remap_person_id(schema, cdm_table, Person)}) AS person_mapping
            ON source.person_id = person_mapping.site_person_id"""

        if cdm_table.__tablename__ == VisitOccurrence.__tablename__:
            selected_cols.replace(
                " care_site_id,",
                " care_site_mapping.merge_care_site_id as care_site_id, ",
            )
            inner_joins += f""" INNER JOIN ({remap_care_site_id(schema, CareSite)}) AS care_site_mapping
            ON source.care_site_id = care_site_mapping.site_care_site_id"""

        query_single_table.append(
            f"""INSERT INTO {cdm_table.__table__}
            ({', '.join(cdm_columns)})
            SELECT {selected_cols}
            FROM {schema}.{cdm_table.__tablename__} as source
            {inner_joins};
        """
        )
    return ";".join(query_single_table)


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
def drop_duplicate_rows(
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
    agg_columns: Union[str, List[str]], agg_function: str = "SUM"
) -> Tuple[str, str]:
    assert agg_function in [
        "SUM",
        "AVG",
        "COUNT",
        "MIN",
        "MAX",
    ], "Invalid aggregation function. {agg_function} is not supported."

    if not agg_columns:
        select_stmt = ""
        insert_stmt = ""
    else:
        if isinstance(agg_columns, str):
            agg_columns = [agg_columns]

        select_stmt = "," + ", ".join(
            [f"{agg_function}(d.{col}) AS {col}" for col in agg_columns]
        )
        insert_stmt = "," + ", ".join(agg_columns)
    return insert_stmt, select_stmt


@clean_sql
def _unite_intervals_sql(
    cdm_table: OmopCdmModelBase,
    key_columns: List[str],
    interval_start_column: str,
    interval_end_column: str,
    agg_columns: Optional[Union[str, List[str]]] = "",
    agg_function: str = "SUM",
) -> str:
    """
    SQL code to unite overlapping intervals in observation periods.
    """
    key_cols = ", ".join(key_columns)

    join_condition_key_cols = " AND ".join(
        [f"i.{col} = d.{col}" for col in key_columns]
    )
    join_condition_start_date = (
        f"i.{interval_start_column} <= d.{interval_start_column}"
    )
    join_condition_end_date = (
        f"i.{interval_end_column} >= d.{interval_end_column}"
    )
    group_by_cols = ", ".join(
        f"i.{c}"
        for c in key_columns + [interval_start_column] + [interval_end_column]
    )
    agg_sum_cols_insert, agg_sum_cols_select = build_aggregate_sql(
        agg_columns, agg_function
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
                {interval_start_column} AS a,
                1 AS d
            FROM
            {cdm_table.__tablename__}_tmp
        UNION ALL
            SELECT
                {key_cols},
                {interval_end_column} AS a,
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
               min(a) AS {interval_start_column},
               max(a) AS {interval_end_column},
        FROM equivalence_classes
        GROUP BY {key_cols}, class
    )
    INSERT INTO {cdm_table.__table__} (
        {key_cols},
        {interval_start_column},
        {interval_end_column}
        {agg_sum_cols_insert}
    ) SELECT DISTINCT i.* {agg_sum_cols_select}
    FROM final_intervals i
    INNER JOIN {cdm_table.__tablename__}_tmp d
    ON {join_condition_key_cols} AND {join_condition_start_date} AND {join_condition_end_date}
    GROUP BY {group_by_cols};
"""
