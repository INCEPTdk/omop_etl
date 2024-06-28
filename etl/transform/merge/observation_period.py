""" Merge observation period tables """

import logging

from etl.sql.merge.mergeutils import merge_cdm_table
from etl.sql.merge.observation_period import concatenate_overlapping_intervals

from ...models.omopcdm54.clinical import ObservationPeriod
from ...util.db import AbstractSession

logger = logging.getLogger("ETL.Merge.ObservationPeriod")


def transform(session: AbstractSession) -> None:
    """Run the Merge Observation period transformation"""
    logger.info("Starting the Observation Period merge transformation... ")

    merge_cdm_table(session, ObservationPeriod)

    logger.info(
        "Merge Observation Period Transformation. Initial %s Periods(s) included ...",
        session.query(ObservationPeriod).count(),
    )

    SQL: str = concatenate_overlapping_intervals()
    session.execute(SQL)

    logger.info(
        "Merge Observation Period Concatenate overlapping periods. Transformation complete! %s Period(s) included",
        session.query(ObservationPeriod).count(),
    )
