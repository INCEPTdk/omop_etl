""" SQL query string definition """

# pylint: disable=no-member
from datetime import date
from typing import Final, List

from ..models.omopcdm54 import (
    ConditionOccurrence,
    Death,
    DrugExposure,
    Measurement,
    Observation,
    ObservationPeriod,
    Person,
    ProcedureOccurrence,
    VisitOccurrence,
)
from ..util.sql import clean_sql

TARGET_TABLENAME: Final[str] = f"{str(ObservationPeriod.__table__)}"
TARGET_COLUMNS: Final[List[str]] = [
    v.key for v in ObservationPeriod.__table__.columns.values()
]
CONCEPT_ID_EHR: Final[int] = 32817
CONCEPT_ID_REGISTRY: Final[int] = 32879
DEFAULT_DATE: Final[date] = date(1800, 1, 1)
DEFAULT_OBSERVATION_DATE: Final[str] = DEFAULT_DATE.isoformat()

def _obs_period_sql(type_concept_id) -> str:
    return f"""
    SELECT
        {ObservationPeriod.person_id.key},
        COALESCE(
            LEAST(
                minimum_measurement_date,
                minimum_condition_date,
                minimum_procedure_date,
                minimum_observation_date,
                minimum_drug_date,
                minimum_visit_date
            ),
            '{DEFAULT_OBSERVATION_DATE}'
        ) AS {ObservationPeriod.observation_period_start_date.key},
        COALESCE(
            GREATEST(
                maximum_measurement_date,
                maximum_condition_date,
                maximum_procedure_date,
                maximum_observation_date,
                maximum_drug_date,
                death_date,
                maximum_visit_date
            ),
            '{DEFAULT_OBSERVATION_DATE}'
        ) AS {ObservationPeriod.observation_period_end_date.key},
        {type_concept_id} AS {ObservationPeriod.period_type_concept_id.key}
    FROM
    (
        SELECT
            *
        FROM

            (
                SELECT
                    {Measurement.person_id.key},
                    MIN({Measurement.measurement_date.key}) AS minimum_measurement_date,
                    MAX({Measurement.measurement_date.key}) AS maximum_measurement_date
                FROM
                    {str(Measurement.__table__)}
                WHERE
                    {Measurement.measurement_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                    AND
                    {Measurement.measurement_type_concept_id.key} = {type_concept_id}
                GROUP BY
                    1
            ) measurement_date_range

            FULL OUTER JOIN (
                SELECT
                    {ConditionOccurrence.person_id.key},
                    MIN(condition_date) AS minimum_condition_date,
                    MAX(condition_date) AS maximum_condition_date
                FROM
                    (
                        SELECT
                            {ConditionOccurrence.person_id.key},
                            {ConditionOccurrence.condition_start_date.key} AS condition_date,
                            {ConditionOccurrence.condition_type_concept_id.key}
                        FROM
                            {str(ConditionOccurrence.__table__)}
                        UNION
                        SELECT
                            {ConditionOccurrence.person_id.key},
                            {ConditionOccurrence.condition_end_date.key} AS condition_date,
                            {ConditionOccurrence.condition_type_concept_id.key}
                        FROM
                            {str(ConditionOccurrence.__table__)}
                    ) condition_dates
                WHERE
                    condition_date <> '{DEFAULT_OBSERVATION_DATE}'
                    AND
                    {ConditionOccurrence.condition_type_concept_id.key} = {type_concept_id}
                GROUP BY
                    1
            ) condition_date_range USING ({ConditionOccurrence.person_id.key})

            FULL OUTER JOIN (
                SELECT
                    {VisitOccurrence.person_id.key},
                    MIN(visit_date) AS minimum_visit_date,
                    MAX(visit_date) AS maximum_visit_date
                FROM
                    (
                        SELECT
                            {VisitOccurrence.person_id.key},
                            {VisitOccurrence.visit_start_date.key} AS visit_date
                        FROM
                            {str(VisitOccurrence.__table__)}
                        UNION
                        SELECT
                            {VisitOccurrence.person_id.key},
                            {VisitOccurrence.visit_end_date.key} AS visit_date
                        FROM
                            {str(VisitOccurrence.__table__)}
                    ) visit_dates
                WHERE
                    visit_date <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) visit_date_range USING ({VisitOccurrence.person_id.key})

            FULL OUTER JOIN (
                SELECT
                    {ProcedureOccurrence.person_id.key},
                    MIN({ProcedureOccurrence.procedure_date.key}) AS minimum_procedure_date,
                    MAX({ProcedureOccurrence.procedure_date.key}) AS maximum_procedure_date
                FROM
                    {str(ProcedureOccurrence.__table__)}
                WHERE
                    {ProcedureOccurrence.procedure_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                    AND
                    {ProcedureOccurrence.procedure_type_concept_id.key} = {type_concept_id}
                GROUP BY
                    1
            ) procedure_date_range USING ({ProcedureOccurrence.person_id.key})

            FULL OUTER JOIN (
                SELECT
                    {Observation.person_id.key},
                    MIN({Observation.observation_date.key}) AS minimum_observation_date,
                    MAX({Observation.observation_date.key}) AS maximum_observation_date
                FROM
                    {str(Observation.__table__)}
                WHERE {Observation.observation_type_concept_id.key} = {type_concept_id}
                GROUP BY
                    1
            ) observation_date_range USING ({Observation.person_id.key})

            FULL OUTER JOIN (
                SELECT
                    {DrugExposure.person_id.key},
                    MIN(drug_exposure_date) AS minimum_drug_date,
                    MAX(drug_exposure_date) AS maximum_drug_date
                FROM
                    (
                        SELECT
                            {DrugExposure.person_id.key},
                            {DrugExposure.drug_exposure_start_date.key} AS drug_exposure_date,
                            {DrugExposure.drug_type_concept_id.key}
                        from
                            {str(DrugExposure.__table__)}
                        UNION
                        SELECT
                            {DrugExposure.person_id.key},
                            {DrugExposure.drug_exposure_end_date.key} AS drug_exposure_date,
                            {DrugExposure.drug_type_concept_id.key}
                        FROM
                            {str(DrugExposure.__table__)}
                    ) drug_exposure_dates
                WHERE
                    drug_exposure_date <> '{DEFAULT_OBSERVATION_DATE}'
                    AND
                    {DrugExposure.drug_type_concept_id.key} = {type_concept_id}
                GROUP BY
                    1
            ) drug_date_range USING ({DrugExposure.person_id.key})

            FULL OUTER JOIN (
                SELECT
                    {Death.person_id.key},
                    MAX({Death.death_date.key}) AS {Death.death_date.key}
                FROM
                    {str(Death.__table__)}
                WHERE
                    {Death.death_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) death_date USING ({Death.person_id.key})
    ) all_ranges
    WHERE
        {Person.person_id.key} in (
            SELECT
                {Person.person_id.key}
            FROM
                {str(Person.__table__)}
        )
"""

@clean_sql
def _insert_observation_periods_sql() -> str:
    return f"""
INSERT INTO
        {TARGET_TABLENAME} (
            {ObservationPeriod.person_id.key},
            {ObservationPeriod.observation_period_start_date.key},
            {ObservationPeriod.observation_period_end_date.key},
            {ObservationPeriod.period_type_concept_id.key}
) WITH temp_observation_period as (
    {get_observation_period_sql(CONCEPT_ID_EHR)}
    union all 
    {get_observation_period_sql(CONCEPT_ID_REGISTRY)}
), OrderedTimes AS (
    SELECT person_id, observation_period_start_date AS TimePoint
    FROM temp_observation_period
    UNION
    SELECT person_id, observation_period_end_date AS TimePoint
    FROM temp_observation_period
),
DistinctTimes AS (
    SELECT person_id, TimePoint, LEAD(TimePoint, 1, TimePoint) OVER (PARTITION BY person_id ORDER BY TimePoint) AS NextTimePoint
    FROM OrderedTimes
),
TimeIntervals AS (
    SELECT person_id, TimePoint AS IntervalStart, NextTimePoint AS IntervalEnd
    FROM DistinctTimes
    WHERE TimePoint <> NextTimePoint
),
ExpandedPeriods AS (
    SELECT t.period_type_concept_id, 
        t.person_id, 
        t.observation_period_start_date, 
        t.observation_period_end_date,
        ti.person_id AS IntervalGroupID, 
        ti.IntervalStart, 
        ti.IntervalEnd,
        CASE WHEN t.period_type_concept_id = {CONCEPT_ID_EHR} THEN 0 ELSE 1 END AS period_type_concept_id_ranking
    FROM temp_observation_period t
    JOIN TimeIntervals ti 
    ON t.person_id = ti.person_id 
        AND ti.IntervalStart < t.observation_period_end_date 
        AND ti.IntervalEnd > t.observation_period_start_date
) SELECT DISTINCT ON (person_id, observation_period_start_date, observation_period_end_date) person_id, IntervalStart as observation_period_start_date, IntervalEnd as observation_period_end_date, period_type_concept_id
FROM ExpandedPeriods
WHERE IntervalStart < IntervalEnd
ORDER BY person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id_ranking
"""

def get_observation_period_sql(type_concept_id: int) -> str:
    return _obs_period_sql(type_concept_id)

def insert_observation_periods_sql() -> str:
    return _insert_observation_periods_sql()
