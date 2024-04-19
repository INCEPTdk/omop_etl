"""Observation transformations"""

import logging

from ..models.omopcdm54.clinical import Observation as OmopObservation
from ..sql.observation import ObservationInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Observation")


def transform(session: AbstractSession) -> None:
    """Run the Observation transformation"""
    logger.info("Starting the Observation transformation... ")
    session.execute(ObservationInsert)
    logger.info(
        "Observation Transformation complete! %s rows included",
        session.query(OmopObservation).count(),
    )
