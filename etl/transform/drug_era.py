"""Drug era transformation"""

import logging

from ..models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from ..sql.drug_era import get_ingredient_era_insert, get_ingredients_with_data
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.DrugEra")


def transform(session: AbstractSession) -> None:
    """Run the Drug era transformation"""
    logger.info("Starting the drug era transformation... ")

    ingredients = get_ingredients_with_data(session)
    for concept_id, ingredient_name in ingredients:
        logger.debug(
            "  Processing drug era for ingredient  %s...",
            ingredient_name,
        )
        session.execute(get_ingredient_era_insert(session, concept_id))

    logger.info(
        "Drug era Transformation complete! %s rows included",
        session.query(OmopDrugEra).count(),
    )
