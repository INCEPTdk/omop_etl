"""Condition era transformation"""

import logging

from ..models.omopcdm54.standardized_derived_elements import (
    ConditionEra as OmopConditionEra,
)
from ..sql.condition_era import get_condition_era_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.ConditionEra")


def transform(session: AbstractSession) -> None:
    """Run the Condition era transformation"""
    logger.info("Starting the condition era transformation... ")
    session.execute(get_condition_era_insert(session))
    logger.info(
        "Condition era Transformation complete! %s rows included",
        session.query(OmopConditionEra).count(),
    )
