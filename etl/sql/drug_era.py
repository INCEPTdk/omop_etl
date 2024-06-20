"Drug era logic."

from sqlalchemy import DATE, INT, and_, case, cast, func, insert, literal, or_
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql import Insert
from sqlalchemy.sql.expression import null

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.omopcdm54.standardized_derived_elements import (
    DrugEra as OmopDrugEra,
)
from ..models.omopcdm54.vocabulary import (
    Concept as OmopConcept,
    ConceptAncestor as OmopConceptAncestor,
)
from ..util.db import (
    AbstractSession,
    get_environment_variable as get_era_lookback_interval,
)

DEFAULT_ERA_LOOKBACK_INTERVAL = get_era_lookback_interval(
    "DRUG_ERA_LOOKBACK", "0 hours"
)


def get_ingredients_with_data(session: AbstractSession) -> list:
    return (
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


def get_ingredient_era_insert(
    session: AbstractSession = None, ingredient_concept_id: int = None
) -> Insert:
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
            OmopStem.start_datetime.label("drug_exposure_start_datetime"),
            OmopStem.end_datetime.label("drug_exposure_end_datetime"),
            func.coalesce(
                OmopStem.start_datetime - lookback_interval,
                OmopStem.start_datetime,
            ).label("lookback_datetime"),
            func.coalesce(
                func.lag(OmopStem.end_datetime).over(
                    partition_by=[OmopStem.person_id],
                    order_by=[
                        OmopStem.start_datetime,
                        OmopStem.end_datetime,
                    ],
                ),
                OmopStem.start_datetime,
            ).label("previous_ingredient_exposure_datetime"),
        )
        .join(
            OmopConceptAncestor,
            and_(
                OmopConceptAncestor.ancestor_concept_id
                == ingredient_concept_id,
                OmopConceptAncestor.descendant_concept_id
                == OmopStem.concept_id,
                or_(OmopStem.days_supply >= 0, OmopStem.days_supply.is_(None)),
            ),
        )
        .cte(name="cte_ingredient_level")
    )

    CteNewSingleEraIndicator = (
        session.query(
            CteIngredientLevel.c.person_id,
            CteIngredientLevel.c.era_lookback_interval,
            CteIngredientLevel.c.drug_exposure_start_datetime,
            CteIngredientLevel.c.drug_exposure_end_datetime,
            CteIngredientLevel.c.previous_ingredient_exposure_datetime,
            CteIngredientLevel.c.lookback_datetime,
            case(
                (
                    or_(
                        func.lag(CteIngredientLevel.c.person_id).over()
                        != CteIngredientLevel.c.person_id,
                        func.lag(CteIngredientLevel.c.person_id)
                        .over()
                        .is_(None),
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
            CteIngredientLevel.c.drug_exposure_start_datetime,
            CteIngredientLevel.c.drug_exposure_end_datetime,
        )
        .cte("cte_new_single_era_indicator")
    )

    CteSingleEraId = session.query(
        CteNewSingleEraIndicator.c.person_id,
        CteNewSingleEraIndicator.c.era_lookback_interval,
        CteNewSingleEraIndicator.c.drug_exposure_start_datetime,
        CteNewSingleEraIndicator.c.drug_exposure_end_datetime,
        CteNewSingleEraIndicator.c.previous_ingredient_exposure_datetime,
        CteNewSingleEraIndicator.c.lookback_datetime,
        CteNewSingleEraIndicator.c.new_era_indicator,
        func.sum(CteNewSingleEraIndicator.c.new_era_indicator)
        .over(
            order_by=[
                CteNewSingleEraIndicator.c.person_id,
                CteNewSingleEraIndicator.c.drug_exposure_start_datetime,
                CteNewSingleEraIndicator.c.drug_exposure_end_datetime,
            ]
        )
        .label("drug_era_id"),
    ).cte("cte_single_era_id")

    CteSingleDrugErasWithDates = (
        session.query(
            CteSingleEraId.c.person_id,
            CteSingleEraId.c.drug_era_id,
            cast(CteSingleEraId.c.drug_exposure_start_datetime, DATE).label(
                "drug_era_start_date"
            ),
            cast(CteSingleEraId.c.drug_exposure_end_datetime, DATE).label(
                "drug_era_end_date"
            ),
        )
    ).cte("cte_single_drug_eras_with_dates")

    CteSingleDrugEras = (
        session.query(
            CteSingleDrugErasWithDates.c.person_id.label("person_id"),
            func.min(CteSingleDrugErasWithDates.c.drug_era_start_date).label(
                "drug_era_start_date"
            ),
            func.max(CteSingleDrugErasWithDates.c.drug_era_end_date).label(
                "drug_era_end_date"
            ),
            func.count(CteSingleDrugErasWithDates.c.person_id).label(
                "drug_exposure_count"
            ),
        ).group_by(
            CteSingleDrugErasWithDates.c.drug_era_id,
            CteSingleDrugErasWithDates.c.person_id,
        )
    ).cte("cte_single_drug_eras")

    CteCombinedEraId = (
        session.query(
            CteSingleDrugEras.c.person_id.label("person_id"),
            CteSingleDrugEras.c.drug_era_start_date.label(
                "drug_era_start_date"
            ),
            CteSingleDrugEras.c.drug_era_end_date.label("drug_era_end_date"),
            CteSingleDrugEras.c.drug_exposure_count.label(
                "drug_exposure_count"
            ),
            case(
                (
                    or_(
                        func.lag(CteSingleDrugEras.c.person_id).over()
                        != CteSingleDrugEras.c.person_id,
                        func.lag(CteSingleDrugEras.c.person_id)
                        .over()
                        .is_(None),
                        CteSingleDrugEras.c.drug_era_start_date
                        > func.lag(
                            CteSingleDrugEras.c.drug_era_end_date
                        ).over(),
                    ),
                    1,
                ),
                else_=0,
            ).label("new_era_indicator"),
        )
    ).cte("cte_combined_era_id")

    CteCombinedEras = (
        session.query(
            CteCombinedEraId.c.person_id.label("person_id"),
            CteCombinedEraId.c.drug_era_start_date.label("drug_era_start_date"),
            CteCombinedEraId.c.drug_era_end_date.label("drug_era_end_date"),
            CteCombinedEraId.c.drug_exposure_count.label("drug_exposure_count"),
            func.sum(CteCombinedEraId.c.new_era_indicator)
            .over(
                order_by=[
                    CteCombinedEraId.c.person_id,
                    CteCombinedEraId.c.drug_era_start_date,
                    CteCombinedEraId.c.drug_era_end_date,
                ]
            )
            .label("combined_era_id"),
        )
    ).cte("cte_combined_eras")

    DrugEraSelect = session.query(
        CteCombinedEras.c.person_id,
        literal(ingredient_concept_id).label("drug_concept_id"),
        func.min(CteCombinedEras.c.drug_era_start_date).label(
            "drug_era_start_date"
        ),
        func.max(CteCombinedEras.c.drug_era_end_date).label(
            "drug_era_end_date"
        ),
        func.sum(cast(CteCombinedEras.c.drug_exposure_count, INT)).label(
            "drug_exposure_count"
        ),
        null().label("gap_days"),
    ).group_by(
        CteCombinedEras.c.person_id,
        CteCombinedEras.c.combined_era_id,
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
