"""SQL code to add the concatenate overlapping intervals in Condition era."""

from etl.models.omopcdm54.standardized_derived_elements import ConditionEra
from etl.util.sql import clean_sql


@clean_sql
def concatenate_overlapping_intervals():
    """
    SQL code to concatenate overlapping intervals when merging drug eras tables.
    """
    return f"""
    CREATE TEMP TABLE condition_era_tmp AS SELECT * FROM {ConditionEra.__table__};
    TRUNCATE TABLE {ConditionEra.__table__};
    WITH
    weighted_endpoints AS (
        SELECT
            {ConditionEra.person_id.key},
            {ConditionEra.condition_concept_id.key},
            a,
            Sum(d) AS d
        FROM
            (
            SELECT
                {ConditionEra.person_id.key},
                {ConditionEra.condition_concept_id.key},
                {ConditionEra.condition_era_start_date.key} AS a,
                1 AS d
            FROM
            condition_era_tmp
        UNION ALL
            SELECT
                {ConditionEra.person_id.key},
                {ConditionEra.condition_concept_id.key},
                {ConditionEra.condition_era_end_date.key} AS a,
                -1 AS d
            FROM
            condition_era_tmp
            ) e
        GROUP BY
            {ConditionEra.person_id.key},
            {ConditionEra.condition_concept_id.key},
            a),
    endpoints_with_coverage AS (
        SELECT
            *,
            Sum(d) OVER (
            ORDER BY {ConditionEra.person_id.key},
                {ConditionEra.condition_concept_id.key},
                a) - d AS c
        FROM
            weighted_endpoints),
    equivalence_classes AS (
        SELECT
            *,
            COUNT(CASE WHEN c = 0 THEN 1 END) OVER (
            ORDER BY {ConditionEra.person_id.key},
                {ConditionEra.condition_concept_id.key},
                a) AS class
        FROM
            endpoints_with_coverage),
    final_intervals AS (
        SELECT
            {ConditionEra.person_id.key},
            {ConditionEra.condition_concept_id.key},
            min(a) AS {ConditionEra.condition_era_start_date.key},
            max(a) AS {ConditionEra.condition_era_end_date.key}
        FROM
            equivalence_classes
        GROUP BY
            {ConditionEra.person_id.key},
            {ConditionEra.condition_concept_id.key},
            class)
    INSERT INTO {ConditionEra.__table__} (
        {ConditionEra.person_id.key},
        {ConditionEra.condition_concept_id.key},
        {ConditionEra.condition_era_start_date.key},
        {ConditionEra.condition_era_end_date.key},
        {ConditionEra.condition_occurrence_count.key}
    ) SELECT DISTINCT i.*, sum(d.{ConditionEra.condition_occurrence_count.key})
    FROM final_intervals i
    INNER JOIN condition_era_tmp d
    ON i.{ConditionEra.person_id.key} = d.{ConditionEra.person_id.key}
        AND i.{ConditionEra.condition_concept_id.key} = d.{ConditionEra.condition_concept_id.key}
        AND i.{ConditionEra.condition_era_start_date.key} <= d.{ConditionEra.condition_era_start_date.key}
        AND i.{ConditionEra.condition_era_end_date.key} >= d.{ConditionEra.condition_era_end_date.key}
    GROUP BY i.{ConditionEra.person_id.key}, i.{ConditionEra.condition_concept_id.key}, i.{ConditionEra.condition_era_start_date.key}, i.{ConditionEra.condition_era_end_date.key};
"""
