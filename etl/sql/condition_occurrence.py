"Condition occurrence logic."

from typing import Final

from sqlalchemy import Date, DateTime, and_, cast, func, insert, or_, select
from sqlalchemy.sql import Insert, Select
from sqlalchemy.sql.expression import Case

from ..models.omopcdm54.clinical import (
    ConditionOccurrence as OmopConditionOccurrence,
    Stem as OmopStem,
)

StemConditionOccurrence: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    func.coalesce(OmopStem.end_date, OmopStem.start_date).label("end_date"),
    Case(
        [OmopStem.end_datetime.is_not(None), OmopStem.end_datetime],
        [OmopStem.end_date.is_not(None), cast(OmopStem.end_date, DateTime)],
        [OmopStem.start_datetime.is_not(None), OmopStem.start_datetime],
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
        or_(OmopStem.concept_id.is_not(None), OmopStem.concept_id != 0),
        OmopStem.start_date == cast(OmopStem.start_datetime, Date),
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
