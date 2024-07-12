""" A collection of utilities for merging different ETl databases. """

from typing import List, Optional, Tuple, Union

from sqlalchemy.sql.schema import Column

from etl.models.omopcdm54.clinical import Death, Person, VisitOccurrence
from etl.models.omopcdm54.health_systems import CareSite
from etl.models.omopcdm54.registry import OmopCdmModelBase
from etl.util.db import (
    AbstractSession,
    get_environment_variable,
    get_source_cdm_schemas,
)
from etl.util.logger import Logger, getLogger
from etl.util.sql import clean_sql


@clean_sql
def _sql_merge_cdm_table(
    schema: str,
    cdm_table: OmopCdmModelBase,
    cdm_columns: List[Column],
):
    """Generate SQL for merge (union) a CDM table based on a list of columns."""

    selected_cols = ", ".join([f"{schema}.{c}" for c in cdm_columns])
    joins = ""

    if cdm_table.__table__.name == VisitOccurrence.__table__.name:
        remapped_col = (
            f"{schema}.{getattr(VisitOccurrence.care_site_id, 'expression')}"
        )
        s, j = remap_care_site_id(schema, remapped_col, CareSite)
        selected_cols = selected_cols.replace(remapped_col, s)
        joins += j

    insert_stmt: str = f""" INSERT INTO {cdm_table.__table__}
        ({', '.join([c.key for c in cdm_columns])})
        SELECT {selected_cols}
        FROM {schema}.{cdm_table.__tablename__}
        {joins}
    """

    return insert_stmt


def merge_cdm_table(
    session: AbstractSession,
    cdm_table: OmopCdmModelBase,
    logger: Logger = getLogger(),
) -> None:
    """
    Merge (union) a CDM table based on a list of columns.
    Skip person mapping should be used when all persons
    are the same across the different sites
    For example when they are pulled from a national registry
    """
    schemas = get_source_cdm_schemas(session)

    is_person_from_registry = (
        get_environment_variable("PERSON_FROM_REGISTRY", default="TRUE")
        == "TRUE"
    )

    merging_death = cdm_table.__table__.name == Death.__table__.name
    merging_person = cdm_table.__table__.name == Person.__table__.name
    merging_visit_occurrence = (
        cdm_table.__table__.name == VisitOccurrence.__table__.name
    )

    cdm_columns = [
        c
        for c in cdm_table.__table__.columns
        if c.key not in cdm_table.__table__.primary_key.columns
        or (merging_person or merging_visit_occurrence)
    ]

    if is_person_from_registry and (merging_person or merging_death):
        schemas = schemas[0:1]

    for schema in schemas:
        merge_sql = _sql_merge_cdm_table(schema, cdm_table, cdm_columns)
        session.execute(merge_sql)
        logger.debug(
            "\tIntermediate merge step. Merged %s records into %s from %s",
            session.query(cdm_table).count(),
            cdm_table.__table__,
            schema,
        )


@clean_sql
def _sql_get_care_site(
    schema: str,
):
    """Get the care site of the schema currently in scope."""

    return f"""select merge_cs.{CareSite.care_site_id.key}
    from {schema}.{CareSite.__tablename__} cs inner join
    {CareSite.__table__} merge_cs
    on cs.care_site_source_value = merge_cs.care_site_source_value;"""


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


def remap_person_id(schema: str, remapped_col, person_table: OmopCdmModelBase):
    """Remap Person IDs in a CDM table."""
    selects = "merge_person.person_id as person_id"
    joins = f"""
            INNER JOIN {schema}.{person_table.__tablename__} AS site_person
            ON {remapped_col} = site_person.person_id
            INNER JOIN {person_table.__table__} AS merge_person
            ON site_person.person_source_value = merge_person.person_source_value
            """
    return selects, joins


def remap_care_site_id(
    schema: str, remapped_col, care_site_table: OmopCdmModelBase
):
    """Remap Care Site IDs in a CDM table."""
    selects = "merge_care_site.care_site_id as merge_care_site_id"
    joins = f""" INNER JOIN {schema}.{care_site_table.__tablename__} AS site_care_site
                ON {remapped_col} = site_care_site.care_site_id
                INNER JOIN {care_site_table.__table__} AS merge_care_site
                ON site_care_site.care_site_source_value = merge_care_site.care_site_source_value"""

    return selects, joins


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
