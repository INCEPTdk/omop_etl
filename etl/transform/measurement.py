"""Measurement transformations"""

import logging

from ..models.omopcdm54.clinical import Measurement as OmopMeasurement
from ..sql.measurement import MeasurementInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Measurement")


def transform(session: AbstractSession) -> None:
    """Run the Measurement transformation"""
    logger.info("Starting the measurement transformation... ")
    session.execute(MeasurementInsert)
    logger.info(
        "Measurement Transformation complete! %s rows included",
        session.query(OmopMeasurement).count(),
    )
