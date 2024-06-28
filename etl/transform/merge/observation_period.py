""" Merge observation period tables """

import logging

from etl.sql.merge.mergeutils import (
    concatenate_overlapping_intervals,
    merge_cdm_table,
)

from ...models.omopcdm54.clinical import ObservationPeriod
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.ObservationPeriod")


def concatenate_intervals(session: AbstractSession):

    SQL: str = concatenate_overlapping_intervals(
        ObservationPeriod,
        key_columns=[
            ObservationPeriod.person_id.key,
            ObservationPeriod.period_type_concept_id.key,
        ],
        start_date_column=ObservationPeriod.observation_period_start_date.key,
        end_date_column=ObservationPeriod.observation_period_end_date.key,
    )

    session.execute(SQL)


def transform(session: AbstractSession) -> None:
    """Run the Merge Observation period transformation"""
    logger.info("Starting the Observation Period merge transformation... ")

    merge_cdm_table(session, ObservationPeriod)

    logger.info(
        "Merge Observation Period Transformation. Initial %s Periods(s) included ...",
        session.query(ObservationPeriod).count(),
    )

    concatenate_intervals(session)

    logger.info(
        "Merge Observation Period Concatenate overlapping periods. Transformation complete! %s Period(s) included",
        session.query(ObservationPeriod).count(),
    )
