""" SQL query string definition """
# pylint: disable=no-member
from typing import Final, List

from ..common import CONCEPT_ID_OBSERVATION_PERIOD, DEFAULT_DATE
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

# _min_prefix = "min_"
# _max_prefix = "max_"

# @clean_sql
# def _obs_period_insert_cols_sql() -> str:
#     return f"""
#     INSERT INTO {str(ObservationPeriod.__table__)}
#     (
#         {ObservationPeriod.person_id.key},
#         {ObservationPeriod.observation_period_start_date.key},
#         {ObservationPeriod.observation_period_end_date.key},
#         {ObservationPeriod.period_type_concept_id.key}
#     )
#     """


# @clean_sql
# def _obs_period_min_date_sql(default_date: Optional[date] = DEFAULT_DATE) -> str:
#     dates = ",".join(
#         [
#             f"{_min_prefix}{model.__tablename__}"
#             for model in [
#                 Measurement,
#                 ConditionOccurrence,
#                 ProcedureOccurrence,
#                 Observation,
#                 DrugExposure,
#                 VisitOccurrence,
#             ]
#         ]
#     )
#     return f"""
#     COALESCE(
#         LEAST(
#             {dates}
#         ),
#         '{default_date.isoformat()}'
#     ) AS observation_period_start_date
#     """


# @clean_sql
# def _obs_period_max_date_sql(default_date: Optional[date] = DEFAULT_DATE) -> str:
#     dates = ",".join(
#         [
#             f"{_max_prefix}{model.__tablename__}"
#             for model in [
#                 Measurement,
#                 ConditionOccurrence,
#                 ProcedureOccurrence,
#                 Observation,
#                 DrugExposure,
#                 VisitOccurrence,
#             ]
#         ]
#     )
#     return f"""
#     COALESCE(
#         GREATEST(
#             {dates},
#             {Death.__tablename__}_date
#         ),
#         '{default_date.isoformat()}'
#     ) AS observation_period_end_date
#     """


# @clean_sql
# def _obs_period_single_date_sql(
#     model: ModelBase,
#     date_col_name: str,
#     default_date: Optional[date] = DEFAULT_DATE,
# ) -> str:
#     fdate = f"{model.__tablename__}_date"
#     return f"""
#     SELECT
#         {Person.person_id.key},
#         MIN({date_col_name}) AS {_min_prefix}{fdate},
#         MAX({date_col_name}) AS {_max_prefix}{fdate}
#     FROM {str(model.__table__)}
#     WHERE {fdate} <> '{default_date.isoformat()}'
#     GROUP BY 1
#     """


# @clean_sql
# def _obs_period_double_date_sql(
#     model: ModelBase,
#     sdate_col_name: str,
#     edate_col_name: str,
#     default_date: Optional[date] = DEFAULT_DATE,
# ) -> str:
#     fdate = f"{model.__tablename__}_date"
#     return f"""
#     SELECT
#         {DrugExposure.person_id.key},
#         MIN({fdate}) AS {_min_prefix}{fdate},
#         MAX({fdate}) AS {_max_prefix}{fdate}
#         FROM
#         (
#             SELECT
#                 {Person.person_id.key} AS {Person.person_id.key},
#                 {sdate_col_name} AS {fdate}
#             FROM {model.__table__}
#             UNION
#             SELECT
#                 {Person.person_id.key} AS {Person.person_id.key},
#                 {edate_col_name} AS {fdate}
#             FROM {str(model.__table__)}
#         ) {model.__tablename__}_alias
#     WHERE {fdate} <> '{default_date.isoformat()}'
#     GROUP BY 1
#     """


# @clean_sql
# def _obs_period_death_date_sql(default_date: date = DEFAULT_DATE) -> str:
#     fdate = "death_date"
#     return f"""
#     SELECT
#         {Death.person_id.key} AS {Death.person_id.key},
#         MAX({Death.death_date.key}) AS {fdate}
#     FROM {str(Death.__table__)}
#     WHERE {fdate} <> '{default_date.isoformat()}'
#     GROUP BY 1
#     """


# @clean_sql
# def _obs_period_sql() -> str:
#     return f"""
#     {_obs_period_insert_cols_sql()}
#     SELECT
#         -- person id
#         {Person.person_id.key},
#         -- observation_period_start_date
#         {_obs_period_min_date_sql()},
#         -- observation_period_end_date
#         {_obs_period_max_date_sql()},
#         -- period_type_concept_id
#         {CONCEPT_ID_OBSERVATION_PERIOD} AS period_type_concept_id
#     FROM (
#         SELECT * FROM
#             ({_obs_period_single_date_sql(
#                 Measurement, Measurement.measurement_date.key)}) d1
#         FULL OUTER JOIN
#             ({_obs_period_single_date_sql(
#                 ProcedureOccurrence,ProcedureOccurrence.procedure_date.key)}) d2
#         USING ({Person.person_id.key})
#         FULL OUTER JOIN
#             ({_obs_period_single_date_sql(
#                 Observation, Observation.observation_date.key)}) d3
#         USING ({Person.person_id.key})
#         FULL OUTER JOIN
#             ({_obs_period_double_date_sql(ConditionOccurrence,
#                 VisitOccurrence.visit_start_date.key,
#                 VisitOccurrence.visit_end_date.key)}) d4
#         USING ({Person.person_id.key})
#         FULL OUTER JOIN
#             ({_obs_period_double_date_sql(ConditionOccurrence,
#                 ConditionOccurrence.condition_start_date.key,
#                 ConditionOccurrence.condition_end_date.key)}) d5
#         USING ({Person.person_id.key})
#         FULL OUTER JOIN
#             ({_obs_period_double_date_sql(DrugExposure,
#                 DrugExposure.drug_exposure_start_date.key,
#                 DrugExposure.drug_exposure_end_date.key)}) d6
#         USING ({Person.person_id.key})
#         FULL OUTER JOIN
#             ({_obs_period_death_date_sql()}) d7
#         USING ({Person.person_id.key})
#         )
#     ) AS d8
#     WHERE {Person.person_id.key} IN (
#         SELECT {Person.person_id.key} FROM {str(Person.__table__)}
#     );
#     """

DEFAULT_OBSERVATION_DATE: Final[str] = DEFAULT_DATE.isoformat()


@clean_sql
def _obs_period_sql() -> str:
    return f"""
    INSERT INTO
        {TARGET_TABLENAME} (
            {ObservationPeriod.person_id.key},
            {ObservationPeriod.observation_period_start_date.key},
            {ObservationPeriod.observation_period_end_date.key},
            {ObservationPeriod.period_type_concept_id.key}
        )
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
        {CONCEPT_ID_OBSERVATION_PERIOD} AS {ObservationPeriod.period_type_concept_id.key}
    FROM
    (
        SELECT
            *
        FROM

            -- measurement
            (
                SELECT
                    {Measurement.person_id.key},
                    MIN({Measurement.measurement_date.key}) AS minimum_measurement_date,
                    MAX({Measurement.measurement_date.key}) AS maximum_measurement_date
                FROM
                    {str(Measurement.__table__)}
                WHERE
                    {Measurement.measurement_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) measurement_date_range

            -- condition occurrence
            FULL OUTER JOIN (
                SELECT
                    {ConditionOccurrence.person_id.key},
                    MIN(condition_date) AS minimum_condition_date,
                    MAX(condition_date) AS maximum_condition_date
                FROM
                    (
                        SELECT
                            {ConditionOccurrence.person_id.key},
                            {ConditionOccurrence.condition_start_date.key} AS condition_date
                        FROM
                            {str(ConditionOccurrence.__table__)}
                        UNION
                        SELECT
                            {ConditionOccurrence.person_id.key},
                            {ConditionOccurrence.condition_end_date.key} AS condition_date
                        FROM
                            {str(ConditionOccurrence.__table__)}
                    ) condition_dates
                WHERE
                    condition_date <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) condition_date_range USING ({ConditionOccurrence.person_id.key})

            -- visit occurrence
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

            -- procedure occurrence
            FULL OUTER JOIN (
                SELECT
                    {ProcedureOccurrence.person_id.key},
                    MIN({ProcedureOccurrence.procedure_date.key}) AS minimum_procedure_date,
                    MAX({ProcedureOccurrence.procedure_date.key}) AS maximum_procedure_date
                FROM
                    {str(ProcedureOccurrence.__table__)}
                WHERE
                    {ProcedureOccurrence.procedure_date.key} <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) procedure_date_range USING ({ProcedureOccurrence.person_id.key})

            -- Observation
            FULL OUTER JOIN (
                SELECT
                    {Observation.person_id.key},
                    MIN({Observation.observation_date.key}) AS minimum_observation_date,
                    MAX({Observation.observation_date.key}) AS maximum_observation_date
                FROM
                    {str(Observation.__table__)}
                GROUP BY
                    1
            ) observation_date_range USING ({Observation.person_id.key})

            -- Drug exposure
            FULL OUTER JOIN (
                SELECT
                    {DrugExposure.person_id.key},
                    MIN(drug_exposure_date) AS minimum_drug_date,
                    MAX(drug_exposure_date) AS maximum_drug_date
                FROM
                    (
                        SELECT
                            {DrugExposure.person_id.key},
                            {DrugExposure.drug_exposure_start_date.key} AS drug_exposure_date
                        from
                            {str(DrugExposure.__table__)}
                        UNION
                        SELECT
                            {DrugExposure.person_id.key},
                            {DrugExposure.drug_exposure_end_date.key} AS drug_exposure_date
                        FROM
                            {str(DrugExposure.__table__)}
                    ) drug_exposure_dates
                WHERE
                    drug_exposure_date <> '{DEFAULT_OBSERVATION_DATE}'
                GROUP BY
                    1
            ) drug_date_range USING ({DrugExposure.person_id.key})

            -- death
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
        );
"""


SQL: Final[str] = _obs_period_sql()
