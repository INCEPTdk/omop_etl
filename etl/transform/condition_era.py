"""Condition era transformation"""

import logging

from ..models.omopcdm54.standardized_derived_elements import (
    ConditionEra as OmopConditionEra,
)
from ..sql.condition_era import (
    get_condition_era_insert,
    get_conditions_with_data,
)
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.ConditionEra")


def transform(session: AbstractSession) -> None:
    """Run the Condition era transformation"""
    logger.info("Starting the condition era transformation... ")

    chunked_concept_ids = get_conditions_with_data(session)
    for chunk in chunked_concept_ids:
        logger.debug("  Processing condition era for concept IDs %s...", chunk)
        session.execute(get_condition_era_insert(session, chunk))

    logger.info(
        "Condition era Transformation complete! %s rows included",
        session.query(OmopConditionEra).count(),
    )
