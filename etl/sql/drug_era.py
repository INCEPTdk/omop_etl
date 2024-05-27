"Drug era logic."

from sqlalchemy import DATE, and_, case, cast, func, insert, or_
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql import Insert
from sqlalchemy.sql.expression import null

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from ..models.omopcdm54.vocabulary import Concept, ConceptAncestor
from ..util.db import AbstractSession, get_environment_variable as get_era_lookback_interval


DEFAULT_ERA_LOOKBACK_INTERVAL = get_era_lookback_interval(
    "DRUG_ERA_LOOKBACK", "0 hours"
)


def get_drug_era_insert(session: AbstractSession = None) -> Insert:
    lookback_interval = cast(
        func.coalesce(
            OmopStem.era_lookback_interval, DEFAULT_ERA_LOOKBACK_INTERVAL
        ),
        INTERVAL,
    )

    CteIngredientLevel = (
        session.query(
            OmopStem.person_id,
            OmopStem.era_lookback_interval,
            Concept.concept_id.label("ingredient_concept_id"),
            OmopStem.start_datetime.label("drug_exposure_start_datetime"),
            OmopStem.end_datetime.label("drug_exposure_end_datetime"),
            func.coalesce(
                OmopStem.start_datetime - lookback_interval,
                OmopStem.start_datetime,
            ).label("lookback_datetime"),
            func.coalesce(
                func.lag(OmopStem.end_datetime).over(
                    partition_by=[
                        OmopStem.person_id,
                        Concept.concept_id,
                    ],
                    order_by=[
                        OmopStem.start_datetime,
                        OmopStem.end_datetime,
                    ],
                ),
                OmopStem.start_datetime,
            ).label("previous_ingredient_exposure_datetime"),
        )
        .join(
            ConceptAncestor,
            ConceptAncestor.descendant_concept_id == OmopStem.concept_id,
        )
        .join(
            Concept,
            and_(
                Concept.concept_id == ConceptAncestor.ancestor_concept_id,
                Concept.vocabulary_id == "RxNorm",
                Concept.concept_class_id == "Ingredient",
            ),
        )
        .where(
            and_(
                OmopStem.concept_id != 0,
                or_(OmopStem.days_supply >= 0, OmopStem.days_supply.is_(None)),
                OmopStem.domain_id == "Drug",
            )
        )
        .cte(name="cte_ingredient_level")
    )

    CteNewEraIndicator = (
        session.query(
            CteIngredientLevel.c.person_id,
            CteIngredientLevel.c.era_lookback_interval,
            CteIngredientLevel.c.ingredient_concept_id,
            CteIngredientLevel.c.drug_exposure_start_datetime,
            CteIngredientLevel.c.drug_exposure_end_datetime,
            CteIngredientLevel.c.previous_ingredient_exposure_datetime,
            CteIngredientLevel.c.lookback_datetime,
            case(
                (
                    or_(
                        func.lag(CteIngredientLevel.c.person_id).over()
                        != CteIngredientLevel.c.person_id,
                        func.lag(
                            CteIngredientLevel.c.ingredient_concept_id
                        ).over()
                        != CteIngredientLevel.c.ingredient_concept_id,
                        CteIngredientLevel.c.lookback_datetime
                        >= CteIngredientLevel.c.previous_ingredient_exposure_datetime,
                    ),
                    1,
                ),
                else_=0,
            ).label("new_era_indicator"),
        )
        .order_by(
            CteIngredientLevel.c.person_id,
            CteIngredientLevel.c.ingredient_concept_id,
            CteIngredientLevel.c.drug_exposure_start_datetime,
            CteIngredientLevel.c.drug_exposure_end_datetime,
        )
        .cte("cte_new_era_indicator")
    )

    CteEraId = session.query(
        CteNewEraIndicator.c.person_id,
        CteNewEraIndicator.c.era_lookback_interval,
        CteNewEraIndicator.c.ingredient_concept_id,
        CteNewEraIndicator.c.drug_exposure_start_datetime,
        CteNewEraIndicator.c.drug_exposure_end_datetime,
        CteNewEraIndicator.c.previous_ingredient_exposure_datetime,
        CteNewEraIndicator.c.lookback_datetime,
        func.sum(CteNewEraIndicator.c.new_era_indicator)
        .over(
            order_by=[
                CteNewEraIndicator.c.person_id,
                CteNewEraIndicator.c.ingredient_concept_id,
                CteNewEraIndicator.c.drug_exposure_start_datetime,
                CteNewEraIndicator.c.drug_exposure_end_datetime,
            ]
        )
        .label("drug_era_id"),
    ).cte("cte_era_id")

    DrugEraSelect = (
        session.query(
            CteEraId.c.person_id,
            CteEraId.c.ingredient_concept_id.label("drug_concept_id"),
            cast(func.min(CteEraId.c.drug_exposure_start_datetime), DATE).label(
                "drug_era_start_date"
            ),
            cast(func.max(CteEraId.c.drug_exposure_end_datetime), DATE).label(
                "drug_era_end_date"
            ),
            func.count(CteEraId.c.ingredient_concept_id).label(
                "drug_exposure_count"
            ),
            null().label("gap_days"),
        ).group_by(
            CteEraId.c.drug_era_id,
            CteEraId.c.person_id,
            CteEraId.c.ingredient_concept_id,
        ).
        distinct()  # collapse identical eras into one
    )

    return insert(OmopDrugEra).from_select(
        names=[
            OmopDrugEra.person_id,
            OmopDrugEra.drug_concept_id,
            OmopDrugEra.drug_era_start_date,
            OmopDrugEra.drug_era_end_date,
            OmopDrugEra.drug_exposure_count,
            OmopDrugEra.gap_days,
        ],
        select=DrugEraSelect,
        include_defaults=False,
    )
