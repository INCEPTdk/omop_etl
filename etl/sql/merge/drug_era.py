"""SQL code to add the concatenate overlapping intervals in drug era."""

from etl.models.omopcdm54.standardized_derived_elements import DrugEra
from etl.util.sql import clean_sql


@clean_sql
def concatenate_overlapping_intervals():
    """
    SQL code to concatenate overlapping intervals when merging drug eras tables.
    """
    return f"""
    CREATE TEMP TABLE drug_era_tmp AS SELECT * FROM {DrugEra.__table__};
    TRUNCATE TABLE {DrugEra.__table__};
    WITH
    weighted_endpoints AS (
        SELECT
            {DrugEra.person_id.key},
            {DrugEra.drug_concept_id.key},
            a,
            Sum(d) AS d
        FROM
            (
            SELECT
                {DrugEra.person_id.key},
                {DrugEra.drug_concept_id.key},
                {DrugEra.drug_era_start_date.key} AS a,
                1 AS d
            FROM
            drug_era_tmp
        UNION ALL
            SELECT
                {DrugEra.person_id.key},
                {DrugEra.drug_concept_id.key},
                {DrugEra.drug_era_end_date.key} AS a,
                -1 AS d
            FROM
            drug_era_tmp
            ) e
        GROUP BY
            {DrugEra.person_id.key},
            {DrugEra.drug_concept_id.key},
            a),
    endpoints_with_coverage AS (
        SELECT
            *,
            Sum(d) OVER (
            ORDER BY a) - d AS c
        FROM
            weighted_endpoints),
    equivalence_classes AS (
        SELECT
            *,
            COUNT(CASE WHEN c = 0 THEN 1 END) OVER (
            ORDER BY a) AS class
        FROM
            endpoints_with_coverage),
    final_intervals AS (
        SELECT
            {DrugEra.person_id.key},
            {DrugEra.drug_concept_id.key},
            min(a) AS {DrugEra.drug_era_start_date.key},
            max(a) AS {DrugEra.drug_era_end_date.key}
        FROM
            equivalence_classes
        GROUP BY
            {DrugEra.person_id.key},
            {DrugEra.drug_concept_id.key},
            class)
    INSERT INTO {DrugEra.__table__} (
        {DrugEra.person_id.key},
        {DrugEra.drug_concept_id.key},
        {DrugEra.drug_era_start_date.key},
        {DrugEra.drug_era_end_date.key},
        {DrugEra.drug_exposure_count.key},
        {DrugEra.gap_days.key}
    ) SELECT DISTINCT i.*, sum(d.{DrugEra.drug_exposure_count.key}), sum(d.{DrugEra.gap_days.key})
    FROM final_intervals i
    INNER JOIN drug_era_tmp d
    ON i.{DrugEra.person_id.key} = d.{DrugEra.person_id.key}
        AND i.{DrugEra.drug_concept_id.key} = d.{DrugEra.drug_concept_id.key}
        AND i.{DrugEra.drug_era_start_date.key} <= d.{DrugEra.drug_era_start_date.key}
        AND i.{DrugEra.drug_era_end_date.key} >= d.{DrugEra.drug_era_end_date.key}
    GROUP BY i.{DrugEra.person_id.key}, i.{DrugEra.drug_concept_id.key}, i.{DrugEra.drug_era_start_date.key}, i.{DrugEra.drug_era_end_date.key};
"""
