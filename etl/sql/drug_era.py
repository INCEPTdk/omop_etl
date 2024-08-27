"Drug era logic."

from sqlalchemy import and_, insert, literal, select
from sqlalchemy.sql import Insert

from ..models.omopcdm54.clinical import DrugExposure as OmopDrugExposure
from ..models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from ..models.omopcdm54.vocabulary import (
    Concept as OmopConcept,
    ConceptAncestor as OmopConceptAncestor,
)
from ..sql.utils import get_era_select
from ..util.db import AbstractSession


def get_ingredients_with_data(session: AbstractSession) -> list:
    return (
        session.query(
            OmopConceptAncestor.ancestor_concept_id,
            OmopConcept.concept_name,
        )
        .join(
            OmopDrugExposure,
            OmopConceptAncestor.descendant_concept_id
            == OmopDrugExposure.drug_concept_id,
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


def get_ingredient_era_insert(
    session: AbstractSession = None, ingredient_concept_id: int = None
) -> Insert:

    CteIngredientExposure = (
        select(
            OmopDrugExposure.person_id,
            literal(ingredient_concept_id).label("drug_concept_id"),
            OmopDrugExposure.drug_exposure_start_datetime,
            OmopDrugExposure.drug_exposure_end_datetime,
            OmopDrugExposure.era_lookback_interval,
        )
        .join(
            OmopConceptAncestor,
            and_(
                OmopConceptAncestor.ancestor_concept_id
                == ingredient_concept_id,
                OmopConceptAncestor.descendant_concept_id
                == OmopDrugExposure.drug_concept_id,
            ),
        )
        .where(
            and_(
                OmopDrugExposure.drug_exposure_start_datetime.isnot(None),
                OmopDrugExposure.drug_exposure_end_datetime.isnot(None),
            )
        )
    )

    DrugEraSelect = get_era_select(
        clinical_table=CteIngredientExposure,
        key_columns="person_id",
        start_column="drug_exposure_start_datetime",
        end_column="drug_exposure_end_datetime",
    )

    return insert(OmopDrugEra).from_select(
        names=[
            OmopDrugEra.person_id,
            OmopDrugEra.drug_era_start_date,
            OmopDrugEra.drug_era_end_date,
            OmopDrugEra.drug_exposure_count,
            OmopDrugEra.drug_concept_id,
        ],
        select=session.query(
            select(
                *DrugEraSelect.columns, literal(ingredient_concept_id)
            ).subquery()
        ),
        include_defaults=False,
    )
