""" Utility functions for transform queries """

from typing import List

# from sqlalchemy import cast, func, select, text, union_all
# from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.orm import Query

from ..models.omopcdm54.clinical import Stem as OmopStem

# from sqlalchemy.sql.expression import null


def derive_eras(
    key_columns: List[str],
    stem_start_column: str,
    stem_end_column: str,
    era_start_column: str,
    era_end_column: str,
    domain_id: str = "Condition",
    count_column: str = None,
) -> Query:

    if not key_columns:
        NotImplementedError("derive_eras expected at least one grouping column")

    key_cols = ", ".join(key_columns)

    stem_subset_criteria = f"""
    concept_id != 0
        AND concept_id IS NOT NULL
        AND domain_id = '{domain_id}'
        AND {stem_start_column} IS NOT NULL
        AND {stem_end_column} IS NOT NULL
        AND {stem_start_column} <= {stem_end_column}
    """

    return f"""
    WITH  weighted_endpoints AS (
        SELECT
            {key_cols},
            era_lookback_interval,
            a,
            sum(d) AS d,
            sum(n) AS n
        FROM (
            SELECT
                {key_cols},
                era_lookback_interval,
                {stem_start_column} AS a,
                1 AS d,
                1 AS n  -- for counting
            FROM {OmopStem.metadata.schema}.{OmopStem.__table__.name}
            WHERE {stem_subset_criteria}
            UNION ALL
            SELECT
                {key_cols},
                era_lookback_interval,
                {stem_end_column} + era_lookback_interval::INTERVAL AS a,
                -1 AS d,
                0 AS n
            FROM {OmopStem.metadata.schema}.{OmopStem.__table__.name}
            WHERE {stem_subset_criteria}
        ) AS endpoints
        GROUP BY {key_cols}, era_lookback_interval, a
    ),
    endpoints_with_coverage AS (
        SELECT
            *,
            sum(d) OVER(ORDER BY {key_cols}, a) - d AS c
        FROM weighted_endpoints
    ),
    equivalence_classes AS (
        SELECT
            *,
            COUNT(CASE WHEN c = 0 THEN 1 END) OVER(ORDER BY {key_cols}, a) AS class
        FROM endpoints_with_coverage
    )
    SELECT
        {key_cols},
        MIN(a) as {era_start_column},
        MAX(a - era_lookback_interval::INTERVAL) as {era_end_column},
        SUM(n) AS {count_column}
    FROM equivalence_classes
    GROUP BY {key_cols}, class
    """
