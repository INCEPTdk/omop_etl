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
from ..util.db import get_environment_variable
from ..util.sql import clean_sql

TARGET_TABLENAME: Final[str] = f"{str(ObservationPeriod.__table__)}"
TARGET_COLUMNS: Final[List[str]] = [
    v.key for v in ObservationPeriod.__table__.columns.values()
]
CONCEPT_ID_EHR: Final[int] = 32817
CONCEPT_ID_REGISTRY: Final[int] = 32879
DEFAULT_DATE: Final[date] = date(1800, 1, 1)
DEFAULT_OBSERVATION_DATE: Final[str] = DEFAULT_DATE.isoformat()


def _obs_period_registries_sql() -> str:
    REGISTRY_START_DATE: Final[str] = get_environment_variable(
        "REGISTRY_START_DATE", "1977-01-01"
    )
    REGISTRY_END_DATE: Final[str] = get_environment_variable(
        "REGISTRY_END_DATE", "2018-04-18"
    )

    return f"""
    SELECT
        {ObservationPeriod.person_id.key},
        GREATEST(
            COALESCE(
                birth_datetime::date,
                '{REGISTRY_START_DATE}'
            ),
            '{REGISTRY_START_DATE}'
            ) AS {ObservationPeriod.observation_period_start_date.key},
        LEAST(
            COALESCE(
                death_date,
                '{REGISTRY_END_DATE}'
            ),
            '{REGISTRY_END_DATE}'
            ) AS {ObservationPeriod.observation_period_end_date.key},
        {CONCEPT_ID_REGISTRY} AS {ObservationPeriod.period_type_concept_id.key}
    FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    {Death.person_id.key},
                    MAX({Death.death_date.key}) AS {Death.death_date.key}
                FROM
                    {str(Death.__table__)}
                WHERE
                    {Death.death_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) death_date

            FULL OUTER JOIN (
                SELECT
                    {Person.person_id.key},
                    MAX({Person.birth_datetime.key}) AS {Person.birth_datetime.key}
                FROM
                    {str(Person.__table__)}
                WHERE
                    {Person.birth_datetime.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) birth_datetime USING ({Person.person_id.key})
    ) all_ranges
    WHERE
        {Person.person_id.key} in (
            SELECT
                {Person.person_id.key}
            FROM
                {str(Person.__table__)}
        )
        AND observation_period_start_date is not NULL
        AND observation_period_end_date is not NULL
"""


def _obs_period_ehr_sql() -> str:
    return f"""
    SELECT
        {ObservationPeriod.person_id.key},
        LEAST(
            minimum_measurement_date,
            minimum_condition_date,
            minimum_procedure_date,
            minimum_observation_date,
            minimum_drug_date,
            minimum_visit_date
            ) AS {ObservationPeriod.observation_period_start_date.key},
        GREATEST(
            maximum_measurement_date,
            maximum_condition_date,
            maximum_procedure_date,
            maximum_observation_date,
            maximum_drug_date,
            maximum_visit_date
        ) AS {ObservationPeriod.observation_period_end_date.key},
        {CONCEPT_ID_EHR} AS {ObservationPeriod.period_type_concept_id.key}
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
                    {Measurement.measurement_type_concept_id.key} = {CONCEPT_ID_EHR}
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
                    {ConditionOccurrence.condition_type_concept_id.key} = {CONCEPT_ID_EHR}
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
                    {ProcedureOccurrence.procedure_type_concept_id.key} = {CONCEPT_ID_EHR}
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
                WHERE {Observation.observation_type_concept_id.key} = {CONCEPT_ID_EHR}
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
                    {DrugExposure.drug_type_concept_id.key} = {CONCEPT_ID_EHR}
                GROUP BY
                    1
            ) drug_date_range USING ({DrugExposure.person_id.key})
    ) all_ranges
    WHERE
        {Person.person_id.key} in (
            SELECT
                {Person.person_id.key}
            FROM
                {str(Person.__table__)}
        )
        AND observation_period_start_date is not NULL
        AND observation_period_end_date is not NULL
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
    {_obs_period_registries_sql()}
    union all
    {_obs_period_ehr_sql()}
), ordered_times AS (
    SELECT person_id, observation_period_start_date AS timepoint
    FROM temp_observation_period
    UNION
    SELECT person_id, observation_period_end_date AS timepoint
    FROM temp_observation_period
),
distinct_times AS (
    SELECT person_id, timepoint, LEAD(timepoint, 1, timepoint) OVER (PARTITION BY person_id ORDER BY timepoint) AS next_timepoint
    FROM ordered_times
),
time_intervals AS (
    SELECT person_id, timepoint AS interval_start, next_timepoint AS interval_end
    FROM distinct_times
    WHERE timepoint <> next_timepoint
),
expanded_periods AS (
    SELECT DISTINCT ON (t.person_id, ti.interval_start, ti.interval_end)
        t.period_type_concept_id,
        t.person_id,
        t.observation_period_start_date,
        t.observation_period_end_date,
        ti.person_id AS interval_groupid,
        ti.interval_start,
        ti.interval_end,
        CASE WHEN t.period_type_concept_id = {CONCEPT_ID_EHR} THEN 0 ELSE 1 END AS period_type_concept_id_ranking
    FROM temp_observation_period t
    JOIN time_intervals ti
    ON t.person_id = ti.person_id
        AND ti.interval_start < t.observation_period_end_date
        AND ti.interval_end > t.observation_period_start_date
    ORDER BY t.person_id, ti.interval_start, ti.interval_end, period_type_concept_id_ranking
), adjusted as (
     select person_id
     ,period_type_concept_id
     ,case when LAG(interval_end, 1)
                OVER (PARTITION BY person_id ORDER BY interval_start, period_type_concept_id) = interval_start and
                LAG (period_type_concept_id, 1)
                OVER (PARTITION BY person_id ORDER BY interval_start, period_type_concept_id) = {CONCEPT_ID_EHR}
                and period_type_concept_id = {CONCEPT_ID_REGISTRY}
                then date_add(interval_start, '1 day'::interval) else interval_start end AS observation_period_start_date
     ,case when LEAD(interval_start, 1)
                OVER (PARTITION BY person_id ORDER BY interval_start, period_type_concept_id) = interval_end and
                LEAD (period_type_concept_id, 1)
                OVER (PARTITION BY person_id ORDER BY interval_start, period_type_concept_id) = {CONCEPT_ID_EHR}
                and period_type_concept_id = {CONCEPT_ID_REGISTRY}
                then date_add(interval_end, '-1 day'::interval) else interval_end end AS observation_period_end_date
    from expanded_periods
    ORDER BY person_id, interval_start, period_type_concept_id
) select person_id, observation_period_start_date::date, observation_period_end_date::date, period_type_concept_id from adjusted;
"""


def insert_observation_periods_sql() -> str:
    return _insert_observation_periods_sql()
