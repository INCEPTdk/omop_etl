"""Specimen transformations"""

import logging

from ..models.omopcdm54.clinical import Specimen as OmopSpecimen
from ..sql.specimen import SpecimenInsert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Specimen")


def transform(session: AbstractSession) -> None:
    """Run the Specimen transformation"""
    logger.info("Starting the specimen transformation... ")
    session.execute(SpecimenInsert)
    logger.info(
        "Specimen Transformation complete! %s rows included",
        session.query(OmopSpecimen).count(),
    )
