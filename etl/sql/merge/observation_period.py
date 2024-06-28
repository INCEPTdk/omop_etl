"""SQL code to add the concatenate overlapping intervals in observation periods."""

from etl.models.omopcdm54.clinical import ObservationPeriod
from etl.util.sql import clean_sql


@clean_sql
def concatenate_overlapping_intervals():
    """
    SQL code to concatenate overlapping intervals in observation periods.
    """
    return f"""
    CREATE TEMP TABLE observation_period_tmp AS SELECT * FROM {ObservationPeriod.__table__};
    TRUNCATE TABLE {ObservationPeriod.__table__};
    WITH
    weighted_endpoints AS (
        SELECT
            {ObservationPeriod.person_id.key},
            {ObservationPeriod.period_type_concept_id.key},
            a,
            Sum(d) AS d
        FROM
            (
            SELECT
                {ObservationPeriod.person_id.key},
                {ObservationPeriod.period_type_concept_id.key},
                {ObservationPeriod.observation_period_start_date.key} AS a,
                1 AS d
            FROM
            observation_period_tmp
        UNION ALL
            SELECT
                {ObservationPeriod.person_id.key},
                {ObservationPeriod.period_type_concept_id.key},
                {ObservationPeriod.observation_period_end_date.key} AS a,
                -1 AS d
            FROM
            observation_period_tmp
            ) e
        GROUP BY
            {ObservationPeriod.person_id.key},
            {ObservationPeriod.period_type_concept_id.key},
            a),
    endpoints_with_coverage AS (
        SELECT
            *,
            Sum(d) OVER (
            ORDER BY {ObservationPeriod.person_id.key},
                {ObservationPeriod.period_type_concept_id.key},
                a) - d AS c
        FROM
            weighted_endpoints),
    equivalence_classes AS (
        SELECT
            *,
            COUNT(CASE WHEN c = 0 THEN 1 END) OVER (
            ORDER BY {ObservationPeriod.person_id.key},
                {ObservationPeriod.period_type_concept_id.key},
                a) AS class
        FROM
            endpoints_with_coverage)
    INSERT INTO {ObservationPeriod.__table__} (
        {ObservationPeriod.person_id.key},
        {ObservationPeriod.period_type_concept_id.key},
        {ObservationPeriod.observation_period_start_date.key},
        {ObservationPeriod.observation_period_end_date.key}
    )
    SELECT
    {ObservationPeriod.person_id.key},
    {ObservationPeriod.period_type_concept_id.key},
    min(a) AS {ObservationPeriod.observation_period_start_date.key},
    max(a) AS {ObservationPeriod.observation_period_end_date.key}
FROM
    equivalence_classes
GROUP BY
    {ObservationPeriod.person_id.key},
    {ObservationPeriod.period_type_concept_id.key},
    class;
"""
