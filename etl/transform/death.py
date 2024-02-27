"""Death transformations"""
import logging

from ..sql.death import DEATH_INSERT
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Death")


def transform(session: AbstractSession) -> None:
    """Run the Death transformation"""
    logger.info("Starting the Death transformation... ")
    session.execute(DEATH_INSERT)
    logger.info("Death transformation finished successfully.")
