"""ObservationPeriod transformations"""

import logging
from typing import Final

from ..models.omopcdm54.clinical import ObservationPeriod
from ..sql.observation_period import get_observation_period_sql
from ..util.db import AbstractSession
from .transformutils import execute_sql_transform

logger = logging.getLogger("ETL.ObservationPeriod")

EHR_CONCEPT_ID: Final[int] = 32817
REGISTRY_CONCEPT_ID: Final[int] = 32879


def transform(session: AbstractSession) -> None:
    """Create the ObservationPeriod tables"""
    logger.info("Creating ObservationPeriod table in DB for EHR... ")
    execute_sql_transform(session, get_observation_period_sql(EHR_CONCEPT_ID))
    logger.info("Creating ObservationPeriod table in DB for Registry... ")
    execute_sql_transform(
        session, get_observation_period_sql(REGISTRY_CONCEPT_ID)
    )
    logger.info(
        "ObservationPeriod Transform complete! %s rows included from EHR, %s rows included from Registry.",
        session.query(ObservationPeriod)
        .where(ObservationPeriod.period_type_concept_id == EHR_CONCEPT_ID)
        .count(),
        session.query(ObservationPeriod)
        .where(ObservationPeriod.period_type_concept_id == REGISTRY_CONCEPT_ID)
        .count(),
    )
