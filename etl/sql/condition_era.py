"Condition era logic."

from itertools import zip_longest

from sqlalchemy import and_, case, cast, func, insert, or_, select
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql import Insert

from ..models.omopcdm54.clinical import Stem as OmopStem
from ..models.omopcdm54.standardized_derived_elements import (
    ConditionEra as OmopConditionEra,
)
from ..util.db import (
    AbstractSession,
    get_environment_variable as get_era_lookback_interval,
)

DEFAULT_ERA_LOOKBACK_INTERVAL = get_era_lookback_interval(
    "CONDITION_ERA_LOOKBACK", "30 days"
)


def get_conditions_with_data(
    session: AbstractSession, chunk_size: int = 50
) -> list:
    conditions = session.scalars(
        select(OmopStem.concept_id)
        .where(
            and_(
                OmopStem.concept_id != 0,
                OmopStem.concept_id.is_not(None),
                OmopStem.domain_id == "Condition",
            )
        )
        .distinct()
    ).all()

    chunked = list(zip_longest(*([iter(conditions)] * chunk_size)))
    chunked[-1] = tuple(c for c in chunked[-1] if c)  # remove None padding

    return chunked


def get_condition_era_insert(
    session: AbstractSession = None, concept_ids: tuple = None
) -> Insert:
    lookback_interval = cast(
        func.coalesce(
            OmopStem.era_lookback_interval, DEFAULT_ERA_LOOKBACK_INTERVAL
        ),
        INTERVAL,
    )

    CtePreviousConditionDate = (
        session.query(
            OmopStem.person_id,
            OmopStem.concept_id.label("condition_concept_id"),
            func.coalesce(OmopStem.start_date, OmopStem.end_date).label(
                "condition_start_date"
            ),
            func.coalesce(OmopStem.end_date, OmopStem.start_date).label(
                "condition_end_date"
            ),
            (OmopStem.start_date - lookback_interval).label("lookback_date"),
            func.coalesce(
                func.lag(OmopStem.end_date).over(), OmopStem.start_date
            ).label("previous_condition_date"),
        )
        .where(
            and_(
                OmopStem.domain_id == "Condition",
                OmopStem.concept_id.in_(concept_ids),
                OmopStem.start_date.isnot(None),
                OmopStem.end_date.isnot(None),
                OmopStem.start_date <= OmopStem.end_date,
            )
        )
        .order_by(
            OmopStem.person_id,
            OmopStem.concept_id,
            OmopStem.start_date,
            OmopStem.end_date,
        )
        .cte(name="cte_previous_condition_date")
    )

    is_new_era = or_(
        func.lag(CtePreviousConditionDate.c.person_id).over()
        != CtePreviousConditionDate.c.person_id,
        func.lag(CtePreviousConditionDate.c.condition_concept_id).over()
        != CtePreviousConditionDate.c.condition_concept_id,
        CtePreviousConditionDate.c.lookback_date
        >= CtePreviousConditionDate.c.previous_condition_date,
    )

    CteNewEraIndicator = session.query(
        CtePreviousConditionDate.c.person_id,
        CtePreviousConditionDate.c.condition_concept_id,
        CtePreviousConditionDate.c.condition_start_date,
        CtePreviousConditionDate.c.condition_end_date,
        case((is_new_era, 1), else_=0).label("new_era_indicator"),
    ).cte("cte_new_era_indicator")

    CteEraId = session.query(
        CteNewEraIndicator.c.person_id,
        CteNewEraIndicator.c.condition_concept_id,
        CteNewEraIndicator.c.condition_start_date,
        CteNewEraIndicator.c.condition_end_date,
        func.sum(CteNewEraIndicator.c.new_era_indicator).over().label("era_id"),
    ).cte("cte_era_id")

    ConditionEraSelect = session.query(
        CteEraId.c.person_id,
        CteEraId.c.condition_concept_id,
        func.min(CteEraId.c.condition_start_date),
        func.max(CteEraId.c.condition_end_date),
        func.count(CteEraId.c.condition_concept_id).label(
            "condition_occurrence_count"
        ),
    ).group_by(
        CteEraId.c.era_id,
        CteEraId.c.person_id,
        CteEraId.c.condition_concept_id,
    )

    return insert(OmopConditionEra).from_select(
        names=[
            OmopConditionEra.person_id,
            OmopConditionEra.condition_concept_id,
            OmopConditionEra.condition_era_start_date,
            OmopConditionEra.condition_era_end_date,
            OmopConditionEra.condition_occurrence_count,
        ],
        select=ConditionEraSelect,
        include_defaults=False,
    )
