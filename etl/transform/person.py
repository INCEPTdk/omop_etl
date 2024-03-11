"""Person transformations"""
import logging

from ..models.omopcdm54.clinical import Person as OmopPerson
from ..sql.person import PERSON_INSERT
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.Person")


def transform(session: AbstractSession) -> None:
    """Run the Person transformation"""
    logger.info("Starting the Person transformation... ")
    session.execute(PERSON_INSERT)
    logger.info(
        "PERSON Transformation complete! %s rows included",
        session.query(OmopPerson).count(),
    )
