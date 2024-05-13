"Measurement logic."

from typing import Final

from sqlalchemy import Date, and_, cast, insert, select
from sqlalchemy.sql import Insert, Select

from ..models.omopcdm54.clinical import (
    Measurement as OmopMeasurement,
    Stem as OmopStem,
)

StemMeasurement: Final[Select] = select(
    OmopStem.person_id,
    OmopStem.concept_id,
    OmopStem.start_date,
    OmopStem.start_datetime,
    OmopStem.type_concept_id,
    OmopStem.operator_concept_id,
    OmopStem.value_as_number,
    OmopStem.value_as_concept_id,
    OmopStem.unit_concept_id,
    OmopStem.range_low,
    OmopStem.range_high,
    OmopStem.provider_id,
    OmopStem.visit_occurrence_id,
    OmopStem.visit_detail_id,
    OmopStem.source_value,
    OmopStem.source_concept_id,
    OmopStem.unit_source_value,
    OmopStem.unit_source_concept_id,
    OmopStem.value_source_value,
    OmopStem.event_id,
    OmopStem.event_field_concept_id,
).where(
    and_(
        OmopStem.domain_id == "Measurement",
        OmopStem.concept_id.is_not(None),
        OmopStem.concept_id != 0,
        OmopStem.start_date == cast(OmopStem.start_datetime, Date),
    )
)

MeasurementInsert: Final[Insert] = insert(OmopMeasurement,).from_select(
    names=[
        OmopMeasurement.person_id,
        OmopMeasurement.measurement_concept_id,
        OmopMeasurement.measurement_date,
        OmopMeasurement.measurement_datetime,
        OmopMeasurement.measurement_type_concept_id,
        OmopMeasurement.operator_concept_id,
        OmopMeasurement.value_as_number,
        OmopMeasurement.value_as_concept_id,
        OmopMeasurement.unit_concept_id,
        OmopMeasurement.range_low,
        OmopMeasurement.range_high,
        OmopMeasurement.provider_id,
        OmopMeasurement.visit_occurrence_id,
        OmopMeasurement.visit_detail_id,
        OmopMeasurement.measurement_source_value,
        OmopMeasurement.measurement_source_concept_id,
        OmopMeasurement.unit_source_value,
        OmopMeasurement.unit_source_concept_id,
        OmopMeasurement.value_source_value,
        OmopMeasurement.measurement_event_id,
        OmopMeasurement.meas_event_field_concept_id,
    ],
    select=StemMeasurement,
)
