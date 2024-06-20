"""Drug era transformation"""

import logging

from sqlalchemy import and_

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from ..models.omopcdm54.vocabulary import (
    Concept as OmopConcept,
    ConceptAncestor as OmopConceptAncestor,
)
from ..sql.drug_era import get_ingredient_era_insert
from ..util.db import AbstractSession

logger = logging.getLogger("ETL.DrugEra")


def transform(session: AbstractSession) -> None:
    """Run the Drug era transformation"""
    logger.info("Starting the drug era transformation... ")

    ingredients_with_data = (
        session.query(
            OmopConceptAncestor.ancestor_concept_id,
            OmopConcept.concept_name,
        )
        .join(
            OmopStem,
            and_(
                OmopConceptAncestor.descendant_concept_id
                == OmopStem.concept_id,
                OmopStem.domain_id == "Drug",
            ),
        )
        .join(
            OmopConcept,
            and_(
                OmopConcept.vocabulary_id == "RxNorm",
                OmopConcept.concept_class_id == "Ingredient",
                OmopConceptAncestor.ancestor_concept_id
                == OmopConcept.concept_id,
            ),
        )
        .distinct()
    ).all()

    for concept_id, ingredient_name in ingredients_with_data:
        logger.debug(
            "  Processing drug era for ingredient  %s...",
            ingredient_name,
        )
        session.execute(get_ingredient_era_insert(session, concept_id))

    session.execute(get_ingredient_era_insert(session, 123))
    logger.info(
        "Drug era Transformation complete! %s rows included",
        session.query(OmopDrugEra).count(),
    )
