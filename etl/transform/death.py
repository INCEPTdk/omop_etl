"""Death transformations"""
import logging

from ..sql.death import DEATH_EXCLUDED, DEATH_INSERT, DEATH_UPLOADED
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Death")


def transform(session: AbstractSession) -> None:
    """Run the Death transformation"""
    logger.info("Starting the Death transformation... ")
    session.execute(DEATH_INSERT)
    count_excluded = session.query(DEATH_EXCLUDED).scalar()
    count_uploaded = session.query(DEATH_UPLOADED).scalar()
    logger.info("Death transformation finished successfully.")
    logger.info("Death: %d rows were uploaded.", count_uploaded)
    logger.info("Death: %d rows were excluded.", count_excluded)
