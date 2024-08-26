"Condition occurrence logic."

from datetime import datetime
from typing import Final

from sqlalchemy import DateTime, and_, case, cast, func, insert, or_, select
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    ConditionOccurrence as OmopConditionOccurrence,
    Stem as OmopStem,
)

StemConditionOccurrence: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    func.coalesce(OmopStem.start_date, OmopStem.end_date).label("start_date"),
    func.coalesce(
        OmopStem.start_datetime,
        cast(OmopStem.start_date, DateTime),
        OmopStem.end_datetime,
        cast(OmopStem.end_date, DateTime),
    ).label("start_datetime"),
    case(
        (OmopStem.end_date > datetime.now(), OmopStem.start_date),
        else_=func.coalesce(OmopStem.end_date, OmopStem.start_date).label(
            "end_date"
        ),
    ).label("end_date"),
    case(
        (OmopStem.end_datetime > datetime.now(), OmopStem.start_datetime),
        else_=func.coalesce(
            OmopStem.end_datetime,
            cast(OmopStem.end_date, DateTime),
            OmopStem.start_datetime,
            cast(OmopStem.start_date, DateTime),
        ),
    ).label("end_datetime"),
    OmopStem.type_concept_id,
    OmopStem.condition_status_concept_id,
    OmopStem.stop_reason,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.condition_status_source_value,
).where(
    and_(
        OmopStem.domain_id == "Condition",
        OmopStem.concept_id.is_not(None),
        OmopStem.type_concept_id.is_not(None),
        or_(
            OmopStem.start_date.is_not(None),
            OmopStem.end_date.is_not(None),
        ),
        OmopStem.start_date <= OmopStem.end_date,
    )
)

ConditionOccurrenceInsert: Final[Insert] = insert(
    OmopConditionOccurrence
).from_select(
    names=[
        OmopConditionOccurrence.person_id,
        OmopConditionOccurrence.condition_concept_id,
        OmopConditionOccurrence.condition_start_date,
        OmopConditionOccurrence.condition_start_datetime,
        OmopConditionOccurrence.condition_end_date,
        OmopConditionOccurrence.condition_end_datetime,
        OmopConditionOccurrence.condition_type_concept_id,
        OmopConditionOccurrence.condition_status_concept_id,
        OmopConditionOccurrence.stop_reason,
        OmopConditionOccurrence.provider_id,
        OmopConditionOccurrence.visit_occurrence_id,
        OmopConditionOccurrence.condition_source_value,
        OmopConditionOccurrence.condition_source_concept_id,
        OmopConditionOccurrence.condition_status_source_value,
    ],
    select=StemConditionOccurrence,
)
