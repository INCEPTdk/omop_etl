""" Merge observation period tables """

import logging

from etl.sql.merge.mergeutils import _unite_intervals_sql, merge_cdm_table

from ...models.omopcdm54.clinical import ObservationPeriod
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.ObservationPeriod")


def unite_intervals(session: AbstractSession):

    SQL: str = _unite_intervals_sql(
        source_cdm_table=ObservationPeriod,
        target_cdm_table=ObservationPeriod,
        key_columns=[
            ObservationPeriod.person_id.key,  # pylint: disable=no-member
            ObservationPeriod.period_type_concept_id.key,
        ],
        interval_start_column=ObservationPeriod.observation_period_start_date.key,
        interval_end_column=ObservationPeriod.observation_period_end_date.key,
    )

    session.execute(SQL)


def transform(session: AbstractSession) -> None:
    """Run the Merge Observation period transformation"""
    logger.info("Starting the Observation Period merge transformation... ")

    merge_cdm_table(session, ObservationPeriod, logger)

    logger.info(
        "Merge Observation Period Transformation. Initial %s Periods(s) included ...",
        session.query(ObservationPeriod).count(),
    )

    unite_intervals(session)

    logger.info(
        "Merge Observation Period unite overlapping periods. Transformation complete! %s Period(s) included",
        session.query(ObservationPeriod).count(),
    )
