"Observation logic."

from typing import Final

from sqlalchemy import Date, DateTime, and_, cast, func, insert, or_, select
from sqlalchemy.sql import Insert, Select
from sqlalchemy.sql.expression import Case

from ..models.omopcdm54.clinical import (
    Observation as OmopObservation,
    Stem as OmopStem,
)

StemObservation: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    OmopStem.type_concept_id,
    OmopStem.value_as_number,
    OmopStem.value_as_string,
    OmopStem.value_as_concept_id,
    OmopStem.unit_concept_id,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.visit_detail_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.unit_source_value,
    OmopStem.qualifier_source_value,
    OmopStem.value_source_value,
    OmopStem.event_id,
    OmopStem.event_field_concept_id,
).where(
    and_(
        OmopStem.domain_id == "Observation",
        or_(OmopStem.concept_id.is_not(None), OmopStem.concept_id != 0),
        OmopStem.start_date == cast(OmopStem.start_datetime, Date),
    )
)

ObservationInsert: Final[Insert] = insert(OmopObservation).from_select(
    names=[
        OmopObservation.person_id,
        OmopObservation.observation_concept_id,
        OmopObservation.observation_date,
        OmopObservation.observation_datetime,
        OmopObservation.observation_type_concept_id,
        OmopObservation.value_as_number,
        OmopObservation.value_as_string,
        OmopObservation.value_as_concept_id,
        OmopObservation.unit_concept_id,
        OmopObservation.provider_id,
        OmopObservation.visit_occurrence_id,
        OmopObservation.visit_detail_id,
        OmopObservation.observation_source_value,
        OmopObservation.observation_source_concept_id,
        OmopObservation.unit_source_value,
        OmopObservation.qualifier_source_value,
        OmopObservation.value_source_value,
        OmopObservation.observation_event_id,
        OmopObservation.obs_event_field_concept_id,
    ],
    select=StemObservation,
)
