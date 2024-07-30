"""ObservationPeriod transformations"""

import logging

from ..models.omopcdm54.clinical import ObservationPeriod
from ..sql.observation_period import (
    CONCEPT_ID_EHR,
    CONCEPT_ID_REGISTRY,
    insert_observation_periods_sql,
)
from ..util.db import AbstractSession
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.ObservationPeriod")


def transform(session: AbstractSession) -> None:
    """Create the ObservationPeriod tables"""
    logger.info("Creating ObservationPeriod table in DB for EHR... ")
    execute_sql_transform(session, insert_observation_periods_sql())
    logger.info("ObservationPeriod Transform complete!")
    logger.info(
        "ObservationPeriod Transform: %s rows included from EHR.",
        session.query(ObservationPeriod)
        .where(ObservationPeriod.period_type_concept_id == CONCEPT_ID_EHR)
        .count(),
    )
    logger.info(
        "ObservationPeriod Transform: %s rows included from Registry.",
        session.query(ObservationPeriod)
        .where(ObservationPeriod.period_type_concept_id == CONCEPT_ID_REGISTRY)
        .count(),
    )
