"""Stem transformations"""
import logging

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..sql.stem import SQL_FUNCTIONS
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Stem")


def transform(session: AbstractSession) -> None:
    """Run the Stem transformation"""
    logger.info("Starting the Stem transformation... ")
    session.execute(SQL_FUNCTIONS)
    logger.info(
        "STEM Transformation complete! %s rows included",
        session.query(OmopStem).count(),
    )
