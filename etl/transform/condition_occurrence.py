"""Condition occurrence transformations"""

import logging
from ..sql.condition_occurrence import ConditionOccurrenceInsert
from ..models.omopcdm54.clinical import ConditionOccurrence as OmopConditionOccurrence
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.ConditionOccurrence")


def transform(session: AbstractSession) -> None:
    """Run the Condition occurrence transformation"""
    logger.info("Starting the Condition occurrence transformation... ")
    session.execute(ConditionOccurrenceInsert)
    logger.info(
        "Condition occurrence Transformation complete! %s rows included",
        session.query(OmopConditionOccurrence).count(),
    )
