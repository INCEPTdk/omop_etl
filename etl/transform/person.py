"""Person transformations"""
import logging

from ..sql.person import PERSON_INSERT
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Person")


def transform(session: AbstractSession) -> None:
    """Run the Person transformation"""
    logger.info("Starting the Person transformation... ")
    session.execute(PERSON_INSERT)
    logger.info("Person transformation finished successfully!")
