"Procedure occurrence logic."

from typing import Final

from sqlalchemy import DateTime, and_, cast, func, insert, select, or_
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    ProcedureOccurrence as OmopProcedureOccurrence,
    Stem as OmopStem,
)

StemProcedureOccurrence: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    func.coalesce(OmopStem.start_date, OmopStem.end_date).label("start_date"),
    func.coalesce(
        OmopStem.start_datetime,
        cast(OmopStem.start_datetime, DateTime),
        OmopStem.end_datetime,
    ).label("start_datetime"),
    func.coalesce(OmopStem.end_date, OmopStem.start_date).label("end_date"),
    func.coalesce(
        OmopStem.end_datetime,
        cast(OmopStem.end_date, DateTime),
        OmopStem.start_datetime,
    ).label("end_datetime"),
    OmopStem.type_concept_id,
    OmopStem.modifier_concept_id,
    OmopStem.quantity,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.modifier_source_value,
).where(
    and_(
        OmopStem.domain_id == "Procedure",
        OmopStem.concept_id.is_not(None),
        OmopStem.type_concept_id.is_not(None),
        or_(
            OmopStem.start_date.is_not(None),
            OmopStem.end_date.is_not(None),
            OmopStem.start_datetime.is_not(None),
            OmopStem.end_datetime.is_not(None),
        ),
    )
)

ProcedureOccurrenceInsert: Final[Insert] = insert(
    OmopProcedureOccurrence
).from_select(
    names=[
        OmopProcedureOccurrence.person_id,
        OmopProcedureOccurrence.procedure_concept_id,
        OmopProcedureOccurrence.procedure_date,
        OmopProcedureOccurrence.procedure_datetime,
        OmopProcedureOccurrence.procedure_end_date,
        OmopProcedureOccurrence.procedure_end_datetime,
        OmopProcedureOccurrence.procedure_type_concept_id,
        OmopProcedureOccurrence.modifier_concept_id,
        OmopProcedureOccurrence.quantity,
        OmopProcedureOccurrence.provider_id,
        OmopProcedureOccurrence.visit_occurrence_id,
        OmopProcedureOccurrence.procedure_source_value,
        OmopProcedureOccurrence.procedure_source_concept_id,
        OmopProcedureOccurrence.modifier_source_value,
    ],
    select=StemProcedureOccurrence,
)
